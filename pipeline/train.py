"""Historical ALS helpers retained ONLY for benchmark/reproducibility.

The VectorAssembler creates auditable auxiliary per-pair count features. ALS
correctly consumes visitorid/itemid/rating, never the assembled feature vector.
Metrics describe a model fitted before held-out test time; only after evaluation
does the final serving model refit all observed interactions. No RMSE is reported
for implicit binary preference ranking.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
import itertools
import hashlib
import gc
import json
import math
import os
from pathlib import Path
import sqlite3
from uuid import uuid4

from .ranking import EVENT_WEIGHTS, rolling_folds


MAX_INT_ID = 2147483647


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as sink:
            json.dump(value, sink, indent=2, ensure_ascii=False, allow_nan=False)
            sink.flush()
            os.fsync(sink.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def prepare_interactions(events):
    """Aggregate ONLY the supplied time partition into implicit confidence weights."""
    from pyspark.sql import functions as F
    counts = [F.sum(F.when(F.col("event") == event, 1.0).otherwise(0.0)).alias(name)
              for event, name in (("view", "view_count"), ("addtocart", "cart_count"),
                                  ("transaction", "purchase_count"))]
    result = (events.filter(F.col("event").isin(*EVENT_WEIGHTS))
            .groupBy("visitorid", "itemid").agg(*counts)
            .withColumn("rating", F.col("view_count") + 3.0 * F.col("cart_count") + 5.0 * F.col("purchase_count"))
            .withColumn("visitorid", F.col("visitorid").cast("int"))
            .withColumn("itemid", F.col("itemid").cast("int")))
    if os.getenv("BIGDATA_LOG_CONFIDENCE", "0") == "1":
        result = result.withColumn("rating", F.log1p("rating"))
    return result


def fit_als_pipeline(interactions, parameters: dict, *, max_iter: int = 5):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.recommendation import ALS
    assembler = VectorAssembler(inputCols=["view_count", "cart_count", "purchase_count"],
                                outputCol="interaction_features", handleInvalid="error")
    als = ALS(userCol="visitorid", itemCol="itemid", ratingCol="rating", implicitPrefs=True,
              rank=int(parameters["rank"]), regParam=float(parameters["reg_param"]),
              alpha=float(parameters["alpha"]), maxIter=max_iter, seed=42,
              coldStartStrategy="drop", numUserBlocks=int(os.getenv("BIGDATA_ALS_BLOCKS", "2")),
              numItemBlocks=int(os.getenv("BIGDATA_ALS_BLOCKS", "2")),
              intermediateStorageLevel="MEMORY_AND_DISK", finalStorageLevel="MEMORY_AND_DISK")
    return Pipeline(stages=[assembler, als]).fit(interactions)


def recommend_unseen(als_model, users, history, top_k: int, catalog_size: int):
    """Exact unseen ranking; cache small cohorts once across candidate buckets.

    Bucket budgets and ranking semantics are unchanged. Eager checkpointing the
    small final result permits releasing cached cohort/history intermediates.
    """
    from functools import reduce
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
    spark=users.sparkSession
    schema=StructType([StructField("visitorid",IntegerType()),StructField("itemid",IntegerType()),
                       StructField("score",DoubleType()),StructField("rank",IntegerType())])
    if catalog_size < 1:return spark.createDataFrame([],schema)
    requested=users.select(F.col("visitorid").cast("int").alias("visitorid")).distinct()
    seen=(history.select(F.col("visitorid").cast("int").alias("visitorid"),
                         F.col("itemid").cast("int").alias("itemid")).distinct()
          .join(requested,"visitorid","inner").cache())
    known_users=als_model.userFactors.select(F.col("id").alias("visitorid"))
    candidates=(requested.join(known_users,"visitorid","inner")
        .join(seen.groupBy("visitorid").count().withColumnRenamed("count","seen_count"),"visitorid","left")
        .fillna({"seen_count":0}).withColumn("request_count",F.least(F.lit(catalog_size),
          F.pow(F.lit(2.0),F.ceil(F.log2(F.lit(top_k)+F.col("seen_count")))).cast("int"))).cache())
    try:
        budgets=[row.request_count for row in candidates.select("request_count").distinct().collect()]
        if not budgets:return spark.createDataFrame([],schema)
        parts=[]
        for budget in sorted(budgets):
            subset=candidates.filter(F.col("request_count")==budget).select("visitorid")
            recommendations=als_model.recommendForUserSubset(subset,int(budget))
            rows=recommendations.select("visitorid",F.explode("recommendations").alias("rec"))
            rows=rows.select("visitorid",F.col("rec.itemid").alias("itemid"),F.col("rec.rating").cast("double").alias("score"))
            rows=rows.filter(~F.isnan("score") & F.col("score").isNotNull()
                             & (F.abs("score")!=F.lit(float("inf")))).join(seen,["visitorid","itemid"],"left_anti")
            order=Window.partitionBy("visitorid").orderBy(F.desc("score"),F.asc("itemid"))
            parts.append(rows.withColumn("rank",F.row_number().over(order)).filter(F.col("rank")<=top_k))
        return reduce(lambda a,b:a.unionByName(b),parts).localCheckpoint(eager=True)
    finally:
        candidates.unpersist()
        seen.unpersist()


def evaluate_ranking(als_model, history, holdout, *, top_k: int = 10) -> dict:
    """Macro user metrics on novel held-out pairs; cold users/items remain misses."""
    from pyspark.sql import functions as F
    training_pairs = history.select("visitorid", "itemid").distinct()
    all_targets = holdout.filter(F.col("event").isin(*EVENT_WEIGHTS)).select(
        F.col("visitorid").cast("int").alias("visitorid"), F.col("itemid").cast("int").alias("itemid")).distinct()
    targets = all_targets.join(training_pairs, ["visitorid", "itemid"], "left_anti").cache()
    population_pairs = targets.count()
    population_users = targets.select("visitorid").distinct().count()
    evaluation_cap = int(os.getenv("BIGDATA_EVAL_MAX_USERS", "0"))
    if evaluation_cap < 0:
        raise ValueError("BIGDATA_EVAL_MAX_USERS cannot be negative")
    if evaluation_cap and population_users > evaluation_cap:
        sample_users = (targets.select("visitorid").distinct()
                        .orderBy(F.xxhash64("visitorid", F.lit(42)), "visitorid").limit(evaluation_cap))
        selected = targets.join(sample_users, "visitorid", "inner").cache()
        targets.unpersist()
        targets = selected
        all_targets = all_targets.join(sample_users, "visitorid", "inner")
    targets_count = targets.count()
    all_count = all_targets.count()
    catalog_size = als_model.itemFactors.count()
    relevant_users = targets.select("visitorid").distinct()
    user_count = relevant_users.count()
    base = {"population_eligible_users": population_users, "population_novel_pairs": population_pairs,
            "evaluation_sampling": "all eligible users" if not evaluation_cap or population_users <= evaluation_cap
                                   else f"lowest xxhash64(visitorid,42), capped at {evaluation_cap}; no target-dependent selection",
            "k": top_k, "eligible_users": user_count, "heldout_unique_pairs": all_count,
            "novel_heldout_pairs": targets_count, "excluded_repeat_pairs": all_count - targets_count,
            "training_catalog_size": catalog_size, "precision_at_k": None, "recall_at_k": None,
            "ndcg_at_k": None, "catalog_coverage": None, "user_coverage": None,
            "definition": "macro user ranking over novel held-out items; cold users/items count as misses; precision denominator is K"}
    if not user_count:
        targets.unpersist()
        return {**base, "status": "not_evaluable_no_novel_targets"}
    warm_users = als_model.userFactors.select(F.col("id").alias("visitorid"))
    warm_items = als_model.itemFactors.select(F.col("id").alias("itemid"))
    base["cold_users"] = relevant_users.join(warm_users, "visitorid", "left_anti").count()
    base["cold_item_pairs"] = targets.join(warm_items, "itemid", "left_anti").count()
    predictions = recommend_unseen(als_model, relevant_users, history, top_k, catalog_size).cache()
    actual = targets.groupBy("visitorid").agg(F.collect_set("itemid").alias("actual"))
    predicted = predictions.groupBy("visitorid").agg(
        F.sort_array(F.collect_list(F.struct("rank", "itemid"))).alias("ordered"))
    scored = (actual.join(predicted, "visitorid", "left")
              .withColumn("predicted", F.coalesce(F.expr("transform(ordered, x -> x.itemid)"), F.array().cast("array<int>")))
              .withColumn("hits", F.size(F.array_intersect("actual", "predicted")))
              .withColumn("precision", F.col("hits") / F.lit(top_k))
              .withColumn("recall", F.col("hits") / F.size("actual"))
              .withColumn("dcg", F.expr("""CASE WHEN size(predicted) = 0 THEN 0D ELSE
                  aggregate(transform(sequence(1,size(predicted)), i ->
                    IF(array_contains(actual,element_at(predicted,i)),1D/log2(i+1D),0D)),
                    0D,(acc,x)->acc+x) END"""))
              .withColumn("ideal", F.expr(f"""aggregate(transform(sequence(1,least({top_k},size(actual))),
                  i -> 1D/log2(i+1D)),0D,(acc,x)->acc+x)"""))
              .withColumn("ndcg", F.col("dcg") / F.col("ideal")))
    averages = scored.agg(F.avg("precision").alias("precision_at_k"), F.avg("recall").alias("recall_at_k"),
                          F.avg("ndcg").alias("ndcg_at_k")).first().asDict()
    base.update({key: float(value) for key, value in averages.items()})
    warm_scores = scored.join(warm_users, "visitorid", "inner").agg(
        F.count("visitorid").alias("users"), F.avg("precision").alias("precision_at_k"),
        F.avg("recall").alias("recall_at_k"), F.avg("ndcg").alias("ndcg_at_k")).first().asDict()
    base["warm_users_only_diagnostic"] = warm_scores
    base["catalog_coverage"] = predictions.select("itemid").distinct().count() / catalog_size if catalog_size else 0.0
    base["user_coverage"] = predictions.select("visitorid").distinct().count() / user_count
    predictions.unpersist()
    targets.unpersist()
    return {**base, "status": "evaluated"}


def _export_seen_sqlite(observed_pairs, destination: Path) -> int:
    """All observed users, including held-out history, streamed to a disk index."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    count = 0
    try:
        with sqlite3.connect(temporary) as connection:
            connection.execute("PRAGMA cache_size=-65536")
            connection.execute("PRAGMA temp_store=MEMORY")
            connection.execute("CREATE TABLE seen_items(visitorid INTEGER NOT NULL,itemid INTEGER NOT NULL,PRIMARY KEY(visitorid,itemid)) WITHOUT ROWID")
            pending = []
            for row in observed_pairs.toLocalIterator():
                pending.append((int(row.visitorid), int(row.itemid)))
                if len(pending) >= 1000:
                    connection.executemany("INSERT OR IGNORE INTO seen_items VALUES(?,?)", pending)
                    count += len(pending)
                    pending.clear()
            if pending:
                connection.executemany("INSERT OR IGNORE INTO seen_items VALUES(?,?)", pending)
                count += len(pending)
            connection.commit()
        # The sqlite3 context manager commits but does not close the handle.
        connection.close()
        os.replace(temporary, destination)
    finally:
        if "connection" in locals():
            connection.close()
        if temporary.exists():
            temporary.unlink()
    return count


def _selection_grid(quick: bool) -> list[dict]:
    rank = int(os.getenv("BIGDATA_ALS_RANK", "8"))
    ranks = [rank] if quick else [rank, rank * 2]
    regs = [0.05, 0.1]
    alphas = [20.0] if quick else [10.0, 20.0]
    return [{"rank": rank, "reg_param": reg, "alpha": alpha}
            for rank, reg, alpha in itertools.product(ranks, regs, alphas)]


def train_als_legacy(spark, run_root: str, run_id: str, quick: bool = True, top_k: int = 10) -> dict:
    """Train/evaluate real Spark MLlib ALS, then publish a bounded serving bundle."""
    from pyspark.sql import functions as F
    from .config import lake_path
    if type(top_k) is not int or not 1 <= top_k <= 100:
        raise ValueError("top_k must be an integer in 1..100")
    max_users = int(os.getenv("BIGDATA_SERVING_MAX_USERS", "5000"))
    if max_users < 1:
        raise ValueError("BIGDATA_SERVING_MAX_USERS must be positive")
    root = Path(run_root)
    metrics_path = root / "metrics" / "model_metrics.json"
    serving_path = root / "serving" / "model_bundle.json"
    for protected in (root / "models" / "als_pipeline", root / "models" / "evaluation_pipeline", serving_path):
        if protected.exists():
            raise FileExistsError(f"Model output already exists; retrain into a new run: {protected}")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    silver_path = lake_path(run_root, "silver")
    silver = spark.read.format("delta").load(silver_path)
    from delta.tables import DeltaTable
    silver_version = int(DeltaTable.forPath(spark, silver_path).history(1).select("version").first()[0])
    required = {"timestamp", "visitorid", "itemid", "event"}
    if not required.issubset(silver.columns):
        raise ValueError(f"Silver is missing fields: {sorted(required - set(silver.columns))}")
    id_column = "event_id" if "event_id" in silver.columns else "eventid" if "eventid" in silver.columns else None
    if not id_column:
        raise ValueError("Silver requires event_id or eventid for deterministic deduplication")
    bad_ids = silver.filter(
        F.col("visitorid").isNull() | F.col("itemid").isNull()
        | (F.col("visitorid") < 0) | (F.col("visitorid") > MAX_INT_ID)
        | (F.col("itemid") < 0) | (F.col("itemid") > MAX_INT_ID)
        | (F.col("visitorid") != F.col("visitorid").cast("int"))
        | (F.col("itemid") != F.col("itemid").cast("int")))
    if bad_ids.limit(1).count():
        raise ValueError("ALS requires nonnegative 32-bit IDs; original-ID cast would be unsafe")
    # Payload JSON/Kafka/ingestion columns are unnecessary for ML and can make
    # a multi-million-row cache spill gigabytes. Preserve event-ID dedup first,
    # then cache only the four model inputs. No events/users are subsampled.
    observed = (silver.select(id_column,"timestamp","visitorid","itemid","event")
                .dropDuplicates([id_column]).drop(id_column).cache())
    observed_count = observed.count()
    events = observed.filter(F.col("event").isin(*EVENT_WEIGHTS)).cache()
    count = events.count()
    if count < 20:
        raise ValueError("Need at least 20 weighted clickstream events for chronological model evaluation")
    if events.filter(F.col("timestamp").isNull() | (F.col("timestamp") <= 0)).limit(1).count():
        raise ValueError("Weighted events require positive epoch-millisecond timestamps")
    # Row quantiles over raw event times, BEFORE any user-item aggregation.
    split_path=root/"metrics"/"temporal_split.json"
    split_identity={"source":silver_path,"delta_version":silver_version,"input_events":count}
    if split_path.exists():
        saved_split=json.loads(split_path.read_text())
        if any(saved_split.get(k)!=v for k,v in split_identity.items()):
            raise ValueError("Frozen temporal split belongs to a different dataset/version")
        boundaries=[int(v) for v in saved_split["boundaries"]]
    else:
        boundaries = [int(value) for value in events.stat.approxQuantile("timestamp", [0.50, 0.70, 0.85], 0.001)]
        _write_json_atomic(split_path,{**split_identity,"boundaries":boundaries})
    if len(boundaries) != 3 or any(first >= second for first, second in zip(boundaries, boundaries[1:])):
        raise ValueError("Not enough distinct event timestamps for two chronological validation folds")
    folds = rolling_folds(boundaries)
    test_end = boundaries[-1]
    development = events.filter(F.col("timestamp") <= test_end)
    test = events.filter(F.col("timestamp") > test_end).cache()
    if test.limit(1).count() == 0:
        raise ValueError("Chronological held-out test partition is empty")
    fold_data = []
    for fold in folds:
        history = prepare_interactions(events.filter(F.col("timestamp") <= fold["train_end_inclusive"])).cache()
        validation = events.filter((F.col("timestamp") > fold["validation_start_exclusive"])
                                   & (F.col("timestamp") <= fold["validation_end_inclusive"])).cache()
        if history.count() < 2 or validation.limit(1).count() == 0:
            raise ValueError("A chronological training/validation fold has insufficient interactions")
        fold_data.append((history, validation))
    candidates = []
    for parameters in _selection_grid(quick):
        fold_metrics = []
        for fold_index, ((history, validation), fold) in enumerate(zip(fold_data, folds), 1):
            signature = {"parameters":parameters,"fold":fold,"source":silver_path,
                "delta_version":silver_version,"input_events":count,"k":top_k,
                "confidence":os.getenv("BIGDATA_LOG_CONFIDENCE","0"),
                "iterations":os.getenv("BIGDATA_CV_ITERATIONS","3" if quick else "10"),
                "cap":os.getenv("BIGDATA_EVAL_MAX_USERS","0"),"seed":42}
            key=hashlib.sha256(json.dumps(signature,sort_keys=True).encode()).hexdigest()
            score_file=root/"metrics"/"cv_scores"/(key+".json")
            if score_file.exists():
                saved=json.loads(score_file.read_text())
                if saved["signature"] != signature: raise ValueError("CV identity mismatch")
                metrics=saved["metrics"]
                print(f"Reusing committed CV fold {fold_index}: {parameters}",flush=True)
            else:
                print(f"ALS validation parameters={parameters}, chronological fold={fold_index}",flush=True)
                pipeline_model=fit_als_pipeline(history,parameters,max_iter=int(os.getenv("BIGDATA_CV_ITERATIONS","3" if quick else "10")))
                metrics=evaluate_ranking(pipeline_model.stages[-1],history,validation,top_k=top_k)
                _write_json_atomic(score_file,{"signature":signature,"metrics":metrics})
                print(f"CV committed: fold={fold_index}, P={metrics.get('precision_at_k')}, R={metrics.get('recall_at_k')}, NDCG={metrics.get('ndcg_at_k')}",flush=True)
                pipeline_model.stages[-1].userFactors.unpersist()
                pipeline_model.stages[-1].itemFactors.unpersist()
                del pipeline_model
                gc.collect()
                if os.getenv("BIGDATA_BOUND_SERVING","0") == "1":spark._jvm.java.lang.System.gc()
            fold_metrics.append({"fold":fold_index,**fold,**metrics})
        usable = [metric for metric in fold_metrics if metric["status"] == "evaluated"]
        candidates.append({"parameters": parameters, "folds": fold_metrics,
                           "mean_validation_ndcg_at_k": sum(m["ndcg_at_k"] for m in usable) / len(usable) if usable else None,
                           "mean_validation_recall_at_k": sum(m["recall_at_k"] for m in usable) / len(usable) if usable else None,
                           "evaluable_folds": len(usable)})
    eligible = [candidate for candidate in candidates if candidate["evaluable_folds"] == len(folds)]
    if not eligible:
        _write_json_atomic(metrics_path, {"status": "not_evaluable", "run_id": run_id, "cross_validation": candidates})
        raise ValueError("No candidate has novel validation targets in both chronological folds")
    # Grid order is deterministic when models tie; no test data influences selection.
    best = max(eligible, key=lambda candidate: (candidate["mean_validation_ndcg_at_k"], candidate["mean_validation_recall_at_k"]))
    parameters = best["parameters"]
    evaluation_interactions = prepare_interactions(development).cache()
    print(f"ALS held-out evaluation with selected parameters={parameters}", flush=True)
    evaluation_model = fit_als_pipeline(evaluation_interactions, parameters, max_iter=int(os.getenv("BIGDATA_FINAL_ITERATIONS", "5" if quick else "10")))
    heldout = evaluate_ranking(evaluation_model.stages[-1], evaluation_interactions, test, top_k=top_k)
    evaluation_model.write().save(str(root / "models" / "evaluation_pipeline"))
    evaluation_model.stages[-1].userFactors.unpersist()
    evaluation_model.stages[-1].itemFactors.unpersist()
    del evaluation_model
    if os.getenv("BIGDATA_BOUND_SERVING", "0") == "1":
        gc.collect()
        spark._jvm.java.lang.System.gc()
    evaluated_at = _utc_now()
    model_version = f"{run_id}:als-serving-v1"
    report = {"training_configuration": {
                  "cv_iterations": int(os.getenv("BIGDATA_CV_ITERATIONS", "3" if quick else "10")),
                  "final_iterations": int(os.getenv("BIGDATA_FINAL_ITERATIONS", "5" if quick else "10")),
                  "als_blocks": int(os.getenv("BIGDATA_ALS_BLOCKS", "2")),
                  "evaluation_user_cap": int(os.getenv("BIGDATA_EVAL_MAX_USERS", "0")),
                  "quick_mode_definition": "reduced hyperparameter grid; DOES NOT subsample training events"},
              "confidence_transform": "log1p(weighted_counts)" if os.getenv("BIGDATA_LOG_CONFIDENCE", "0") == "1" else "weighted_counts",
              "status": "evaluated", "run_id": run_id, "algorithm": "als_implicit", "quick_mode": quick,
              "metric_model_version": f"{run_id}:als-evaluation-v1", "serving_model_version": model_version,
              "evaluated_at_utc": evaluated_at, "seed": 42, "top_k": top_k,
              "event_weights": EVENT_WEIGHTS, "input_events": observed_count, "weighted_events": count,
              "silver_path": silver_path,"silver_delta_version":silver_version,
              "excluded_nonweighted_events": observed_count - count,
              "chronological_boundaries_epoch_ms": boundaries,
              "split_semantics": "fold1 train<=50% validate(50%,70%]; fold2 train<=70% validate(70%,85%]; evaluate train<=85% test>85%; split before aggregation",
              "selected_parameters": parameters, "selection_metric": "mean_validation_ndcg_at_k",
              "cross_validation": candidates, "heldout_test": heldout,
              "evaluation_train_events": development.count(), "heldout_test_events": test.count(),
              "feature_pipeline": {"assembler_inputs": ["view_count", "cart_count", "purchase_count"],
                                   "assembler_output": "interaction_features",
                                   "als_inputs": ["visitorid", "itemid", "rating"],
                                   "note": "Auxiliary vector is persisted/auditable; ALS consumes IDs and weighted implicit ratings."},
              "quality_gate": "No minimum accuracy claim: inspect heldout metrics and coverage, including cold users/items.",
              "serving_refit_scope": "all observed weighted silver events, only AFTER heldout evaluation; metrics describe evaluation model"}
    _write_json_atomic(metrics_path, report)
    # Final operational fit can use all events because quality measurements are
    # already frozen above and explicitly tied to the evaluation model version.
    full_interactions = prepare_interactions(events).cache()
    print("ALS serving refit on all observed interactions", flush=True)
    full_model = fit_als_pipeline(full_interactions, parameters, max_iter=int(os.getenv("BIGDATA_FINAL_ITERATIONS", "5" if quick else "10")))
    full_model.write().save(str(root / "models" / "als_pipeline"))
    als_model = full_model.stages[-1]
    all_users = als_model.userFactors.select(F.col("id").alias("visitorid"))
    all_seen = observed.select(F.col("visitorid").cast("int").alias("visitorid"),
                                F.col("itemid").cast("int").alias("itemid")).distinct().repartition(32).cache()
    catalog_size, trained_users = als_model.itemFactors.count(), als_model.userFactors.count()
    bounded_export = os.getenv("BIGDATA_BOUND_SERVING", "0") == "1"
    selected_users = (all_seen.groupBy("visitorid").count().orderBy(F.desc("count"), F.asc("visitorid"))
                      .limit(max_users).select("visitorid").join(all_users, "visitorid", "inner").cache()
                      if bounded_export else all_users.orderBy("visitorid").limit(max_users).cache())
    recommendation_users = selected_users if bounded_export else all_users
    recommendations = recommend_unseen(als_model, recommendation_users, all_seen, top_k, catalog_size).cache()
    recommendations.write.mode("errorifexists").parquet(str(root / "models" / "recommendations"))
    selected_ids = [int(row.visitorid) for row in selected_users.collect()]
    exported = {str(user): [] for user in selected_ids}
    for row in recommendations.join(selected_users, "visitorid", "inner").orderBy("visitorid", "rank").toLocalIterator():
        exported[str(row.visitorid)].append({"itemid": int(row.itemid), "score": float(row.score)})
    popularity = [{"itemid": int(row.itemid), "score": float(row.score)} for row in
                  full_interactions.groupBy("itemid").agg(F.sum("rating").alias("score"))
                  .orderBy(F.desc("score"), F.asc("itemid")).limit(max(1000, top_k * 20)).collect()]
    seen_store = root / "serving" / "seen_items.sqlite"
    seen_pairs_count = _export_seen_sqlite(all_seen.orderBy("visitorid","itemid"), seen_store)
    seen_export = {}
    with closing(sqlite3.connect(seen_store)) as connection:
        for user in selected_ids:
            seen_export[str(user)] = [int(row[0]) for row in connection.execute(
                "SELECT itemid FROM seen_items WHERE visitorid=? ORDER BY itemid", (user,))]
    serving_metadata = {"recommendation_scope": "most active observed users, capped for local serving" if bounded_export else "all trained users in Parquet",
                        "max_json_users": max_users, "exported_json_users": len(exported),
                        "trained_users": trained_users, "training_catalog_size": catalog_size,
                        "all_observed_seen_pairs": seen_pairs_count, "all_recommendations_parquet": "models/recommendations",
                        "fallback": "users outside JSON recommendation scope use popularity with all-observed SQLite seen filtering"}
    report["serving_scope"] = serving_metadata
    report["trained_at_utc"] = _utc_now()
    _write_json_atomic(metrics_path, report)
    bundle = {"run_id": run_id, "model_version": model_version, "trained_at_utc": report["trained_at_utc"],
              "algorithm": "als_implicit", "recommendations": exported, "seen_items": seen_export,
              "seen_items_store": "seen_items.sqlite", "popular_items": popularity,
              "metrics": report, "serving_scope": serving_metadata}
    # Sidecar and model artifacts exist before the bundle becomes visible.
    _write_json_atomic(serving_path, bundle)
    als_model.userFactors.unpersist()
    als_model.itemFactors.unpersist()
    for history, validation in fold_data:
        history.unpersist()
        validation.unpersist()
    for frame in (observed, events, test, evaluation_interactions, full_interactions, all_seen, recommendations, selected_users):
        frame.unpersist()
    return report


def train_model(spark, run_root, run_id, quick=True, top_k=10):
    """Compatibility import: the main Phase 5 trainer is now Spark Graph."""
    from pipeline.graph_training import train_model as train_graph_model
    return train_graph_model(spark, run_root, run_id, quick, top_k)
