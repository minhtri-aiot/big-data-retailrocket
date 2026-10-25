"""Phase 5: Spark item-item + session-transition training, evaluation and export.

Graph relations are computed with Spark SQL, never by collecting raw events to
the driver. Only bounded evaluation queries and streamed SQLite rows leave Spark.
Offline evaluation and FastAPI call the SAME dependency-free score_context.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time

from pipeline.config import atomic_json, lake_path, pipeline_lock, run_path, utc_now
from pipeline.ranking import user_ranking_metrics
from pipeline.session_model import score_context

ALGORITHM = "graph_session"
CONFIG = {"graph": "binary_user", "similarity": "conditional", "fusion": "strength",
          "transition_weight": 0.25, "strength_power": 0.5, "seed_limit": 5,
          "position_decay": 0.8, "query_half_life_minutes": None,
          "event_weights": {"view": 1.0, "addtocart": 1.0, "transaction": 1.0}}
METRIC_KEYS = ("precision_at_k", "recall_at_k", "ndcg_at_k", "hit_rate_at_k")


def checksum(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalize_events(frame):
    """Fail closed on malformed model inputs; retain transaction-level dedup."""
    from pyspark.sql import functions as F
    required = {"timestamp", "visitorid", "itemid", "event"}
    if not required.issubset(frame.columns):
        raise ValueError(f"Missing model inputs: {sorted(required-set(frame.columns))}")
    # Reviews (outside this model's signal contract) may exist in Silver.
    # Keep null events for validation: SQL ``NULL != 'review'`` is NULL and
    # would otherwise silently discard malformed input before the check.
    frame = frame.filter(F.col("event").isNull() | (F.col("event") != "review"))
    invalid = ~F.col("event").isin("view", "addtocart", "transaction") | F.col("event").isNull()
    for name in ("timestamp", "visitorid", "itemid"):
        cast = F.col(name).cast("long")
        invalid = invalid | cast.isNull() | (cast < (1 if name == "timestamp" else 0))
        invalid = invalid | (F.col(name).cast("double") != cast.cast("double"))
        if name != "timestamp":
            invalid = invalid | (cast > 2147483647)
    if "transactionid" in frame.columns:
        tx = F.col("transactionid")
        invalid = invalid | ((F.col("event") == "transaction") &
                             (tx.cast("long").isNull() | (tx.cast("long") < 0)))
    if frame.filter(invalid).limit(1).count():
        raise ValueError("Invalid timestamp/ID/event/transaction; fix or quarantine input before training")
    keys = [name for name in ("event_id", "timestamp", "visitorid", "itemid", "event", "transactionid")
            if name in frame.columns]
    result = frame.select(*keys).dropDuplicates(keys)
    return result.select(F.col("timestamp").cast("long").alias("timestamp"),
                         F.col("visitorid").cast("int").alias("visitorid"),
                         F.col("itemid").cast("int").alias("itemid"), "event")


def sessionize(events):
    """30-minute inactivity gap; a timestamp group never predicts itself."""
    from pyspark.sql import functions as F, Window
    order = Window.partitionBy("visitorid").orderBy("timestamp")
    times = events.select("visitorid", "timestamp").distinct()
    times = times.withColumn("previous", F.lag("timestamp").over(order))
    times = times.withColumn("new_session", F.when(F.col("previous").isNull() |
                             (F.col("timestamp")-F.col("previous") > 1800000), 1).otherwise(0))
    times = times.withColumn("session", F.sum("new_session").over(order.rowsBetween(Window.unboundedPreceding, 0)))
    times = times.withColumn("session_start", F.min("timestamp").over(Window.partitionBy("visitorid", "session")))
    times = times.withColumn("next_timestamp", F.lead("timestamp").over(Window.partitionBy("visitorid", "session").orderBy("timestamp")))
    result = events.join(times.drop("previous", "new_session"), ["visitorid", "timestamp"])
    return result.withColumn("first_timestamp", F.min("timestamp").over(Window.partitionBy("visitorid", "session", "itemid")))


def build_relations(events, neighbor_limit=100, max_user_items=20):
    """Directed conditional co-occurrence, plus next-group raw transition counts."""
    from pyspark.sql import functions as F, Window
    recent = events.groupBy("visitorid", "itemid").agg(F.max("timestamp").alias("last"))
    recent = recent.withColumn("r", F.row_number().over(Window.partitionBy("visitorid").orderBy(F.desc("last"), "itemid")))
    recent = recent.filter(F.col("r") <= max_user_items).select("visitorid", "itemid").cache()
    marginal = recent.groupBy("itemid").count().withColumnRenamed("itemid", "seed").withColumnRenamed("count", "support")
    left = recent.select("visitorid", F.col("itemid").alias("seed"))
    right = recent.select("visitorid", F.col("itemid").alias("candidate"))
    pairs = left.join(right, "visitorid").filter(F.col("seed") != F.col("candidate"))
    pairs = pairs.groupBy("seed", "candidate").count().join(marginal, "seed")
    co = pairs.select("seed", "candidate", (F.col("count")/F.col("support") *
                      F.col("count")/(F.col("count")+3.0)).alias("score"))
    # Materialize before releasing the bounded interaction cache.
    co = co.withColumn("r", F.row_number().over(Window.partitionBy("seed").orderBy(F.desc("score"), "candidate")))
    co = co.filter(F.col("r") <= neighbor_limit).drop("r").localCheckpoint(eager=True)
    recent.unpersist()
    sessions = sessionize(events).select("visitorid", "session", "timestamp", "next_timestamp",
                                         "itemid", "first_timestamp").distinct().cache()
    a = sessions.select("visitorid", "session", F.col("next_timestamp").alias("at"), F.col("itemid").alias("seed")).distinct()
    b = sessions.filter(F.col("timestamp") == F.col("first_timestamp")).select(
        "visitorid", "session", F.col("timestamp").alias("at"), F.col("itemid").alias("candidate")).distinct()
    transitions = a.join(b, ["visitorid", "session", "at"]).filter(F.col("seed") != F.col("candidate"))
    transitions = transitions.groupBy("seed", "candidate").count().withColumnRenamed("count", "score")
    transitions = transitions.withColumn("r", F.row_number().over(Window.partitionBy("seed").orderBy(F.desc("score"), "candidate")))
    transitions = transitions.filter(F.col("r") <= neighbor_limit).drop("r").localCheckpoint(eager=True)
    sessions.unpersist()
    return co, transitions


def export_index(co, transitions, destination, metadata):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    temporary.unlink(missing_ok=True)
    counts = {}
    with closing(sqlite3.connect(temporary)) as db:
        for table, frame in (("neighbors", co), ("transitions", transitions)):
            db.execute(f"CREATE TABLE {table}(seed INTEGER,candidate INTEGER,score REAL NOT NULL,PRIMARY KEY(seed,candidate)) WITHOUT ROWID")
            db.execute(f"CREATE INDEX {table}_rank ON {table}(seed,score DESC,candidate)")
            count = 0
            batch = []
            for row in frame.toLocalIterator():
                batch.append((int(row.seed), int(row.candidate), float(row.score)))
                count += 1
                if len(batch) == 10000:
                    db.executemany(f"INSERT INTO {table} VALUES(?,?,?)", batch)
                    batch.clear()
            db.executemany(f"INSERT INTO {table} VALUES(?,?,?)", batch)
            counts[table] = count
        db.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.executemany("INSERT INTO metadata VALUES(?,?)", [(key, json.dumps(value)) for key, value in metadata.items()])
        db.commit()
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("Graph index integrity check failed")
    os.replace(temporary, destination)
    return {**counts, "sha256": checksum(destination), "bytes": destination.stat().st_size}


def export_context(events, destination):
    """Bounded historical context for GET /recommendations/{visitorid}."""
    from pyspark.sql import functions as F, Window
    recent = events.groupBy("visitorid", "itemid").agg(F.max(F.struct("timestamp", "event")).alias("last"))
    recent = recent.select("visitorid", "itemid", "last.timestamp", "last.event")
    recent = recent.withColumn("r", F.row_number().over(Window.partitionBy("visitorid").orderBy(F.desc("timestamp"), "itemid")))
    with closing(sqlite3.connect(destination)) as db:
        db.execute("CREATE TABLE user_context(visitorid INTEGER,itemid INTEGER,timestamp INTEGER,event TEXT,PRIMARY KEY(visitorid,itemid)) WITHOUT ROWID")
        batch = []
        for r in recent.filter(F.col("r") <= 5).toLocalIterator():
            batch.append((int(r.visitorid), int(r.itemid), int(r.timestamp), r.event))
            if len(batch) == 10000:
                db.executemany("INSERT INTO user_context VALUES(?,?,?,?)", batch); batch.clear()
        db.executemany("INSERT INTO user_context VALUES(?,?,?,?)", batch)
        db.commit()


def popularity(events, limit=10000):
    from pyspark.sql import functions as F
    return [{"itemid": int(r.itemid), "score": float(r['count'])}
            for r in events.groupBy("itemid").count().orderBy(F.desc("count"), "itemid").limit(limit).collect()]


def make_queries(events, cutoff, end, cap=5000):
    """One query/session: earliest future novel timestamp with prior context.

    Labels: ALL distinct new items from that timestamp to the period/session end.
    Anonymous-session seen scope; no future labels in context, graph or fallback.
    """
    from pyspark.sql import functions as F
    s = sessionize(events.filter(F.col("timestamp") <= end)).cache()
    keys = ["visitorid", "session", "session_start"]
    try:
        active = s.groupBy(*keys).agg(F.max("timestamp").alias("last")).filter(F.col("last") > cutoff).count()
        q = (s.filter((F.col("timestamp") > cutoff) & (F.col("timestamp") > F.col("session_start")) &
                     (F.col("timestamp") == F.col("first_timestamp")))
             .groupBy(*keys).agg(F.min("timestamp").alias("target_timestamp")))
        eligible = q.count()
        q = q.withColumn("query_id", F.concat_ws(":", "visitorid", "session_start"))
        q = q.orderBy(F.sha2(F.concat(F.col("query_id"), F.lit(":42")), 256), "query_id").limit(cap)
        joined = s.join(q, keys)
        event = F.struct("timestamp", "itemid", "event")
        grouped = joined.groupBy(*keys, "query_id", "target_timestamp").agg(
            F.sort_array(F.collect_list(F.when(F.col("timestamp") < F.col("target_timestamp"), event))).alias("context"),
            F.sort_array(F.collect_set(F.when(F.col("timestamp") < F.col("target_timestamp"), F.col("itemid")))).alias("seen"),
            F.sort_array(F.collect_set(F.when(F.col("first_timestamp") >= F.col("target_timestamp"), F.col("itemid")))).alias("actual"))
        grouped = grouped.withColumn("context", F.when(F.size("context") > 100, F.slice("context", -100, 100)).otherwise(F.col("context")))
        rows = []
        for r in grouped.orderBy("query_id").collect():
            d = r.asDict(recursive=True)
            d["as_of"] = int(d["target_timestamp"])-1
            rows.append(d)
        return rows, {"active_sessions": active, "eligible_sessions": eligible, "eligible_queries": len(rows),
                      "query_cap": cap, "selection": "lowest SHA256(visitorid:session_start:42); before scoring",
                      "queries_with_multiple_labels": sum(len(r["actual"]) > 1 for r in rows)}
    finally:
        s.unpersist()


def measure(predicted, actual, k):
    return {**user_ranking_metrics(predicted, actual, k), "hit_rate_at_k": float(bool(set(predicted[:k]) & set(actual)))}


def evaluate(index, rows, popular, config, k, output=None, comparator=None):
    totals = {label: {key: 0.0 for key in METRIC_KEYS} for label in ("graph", "popularity", "als")}
    records = []
    for row in rows:
        scored = score_context(index, row["context"], popular, row["seen"], k, config, row["as_of"])
        predictions = {"graph": [r["itemid"] for r in scored["items"]],
                       "popularity": [r["itemid"] for r in popular if r["itemid"] not in set(row["seen"])][:k]}
        if comparator:
            predictions["als"] = comparator(row)
        for label, listing in predictions.items():
            for key, value in measure(listing, row["actual"], k).items():
                totals[label][key] += value
        records.append({**row, "predictions": predictions, "graph_source": scored["source"]})
    n = len(rows)
    summary = {label: {"k": k, "eligible_queries": n, "status": "evaluated" if n else "not_evaluable",
                       **{key: value/n if n else None for key, value in values.items()}}
               for label, values in totals.items() if label != "als" or comparator}
    if output:
        Path(output).write_text("".join(json.dumps(r, sort_keys=True)+"\n" for r in records), encoding="utf-8")
    return summary


def als_comparator(training, rows, popular, k, parameters=None):
    """Historical-user ALS benchmark; no fold-in of query context into factors.

    Same queries/labels/cutoff/seen and popularity fallback as Graph. Explicitly
    report the information difference: ALS uses trained user ID, Graph uses context.
    Never exported as a serving model or used for selecting the model family.
    """
    from pyspark.sql import functions as F
    from pipeline.train import prepare_interactions, fit_als_pipeline
    parameters = parameters or {"rank": 16, "reg_param": 0.1, "alpha": 20.0}
    model = fit_als_pipeline(prepare_interactions(training), parameters, max_iter=5).stages[-1]
    users = training.sparkSession.createDataFrame([(r["visitorid"],) for r in rows], "visitorid int").distinct()
    # Exact top K after per-query filtering: enough overfetch for the longest seen list.
    budget = min(model.itemFactors.count(), k + max((len(r["seen"]) for r in rows), default=0))
    raw = model.recommendForUserSubset(users, budget).collect() if budget else []
    values = {int(r.visitorid): sorted([(int(v.itemid), float(v.rating)) for v in r.recommendations], key=lambda x: (-x[1], x[0])) for r in raw}
    model.userFactors.unpersist()
    model.itemFactors.unpersist()
    def predict(row):
        seen = set(row["seen"])
        listing = [i for i, score in values.get(row["visitorid"], []) if i not in seen and math.isfinite(score)][:k]
        listing += [r["itemid"] for r in popular if r["itemid"] not in seen and r["itemid"] not in listing][:max(0, k-len(listing))]
        return listing
    return predict


def train_graph(spark, run_root, run_id, events, quick=True, top_k=10, compare_als=False,
                query_cap=5000, source=None, neighbor_limit=100, max_user_items=20):
    from pyspark.sql import functions as F
    from pipeline.train import _export_seen_sqlite
    if not 1 <= top_k <= 100 or not 1 <= query_cap <= 50000 or neighbor_limit < 1 or max_user_items < 2:
        raise ValueError("Invalid K/query/graph limits")
    root = Path(run_root)
    serving = root / "serving"
    if (serving / "model_bundle.json").exists():
        raise FileExistsError("Train into a new run; active model artifacts are immutable")
    (root / "metrics").mkdir(parents=True, exist_ok=True)
    version = f"graph-{run_id}-v1"
    events = normalize_events(events).cache()
    started = time.monotonic()
    try:
        count = events.count()
        if count < 20:
            raise ValueError("Need at least 20 events and four distinct temporal partitions")
        bounds = [int(t) for t in events.approxQuantile("timestamp", [.5, .7, .85], 0.001)]
        end = int(events.agg(F.max("timestamp")).first()[0])
        if len(set(bounds)) != 3 or bounds[-1] >= end:
            raise ValueError("Not enough distinct timestamps for temporal validation/test")
        plan = {"algorithm": ALGORITHM, "model_family_locked": True, "config": CONFIG,
                "boundaries_epoch_ms": bounds, "maximum_epoch_ms": end,
                "split": "event-row approximate quantiles 50/70/85% (error<=0.001); timestamp ties kept together",
                "protocol": "remaining_session", "seen_scope": "anonymous session only",
                "k": top_k, "source": source or {}, "training_events": count,
                "query_cap": query_cap, "max_user_items": max_user_items, "neighbor_limit": neighbor_limit,
                "als_role": "benchmark_only" if compare_als else "not_run",
                "test_reuse_disclosure": "RetailRocket historical period has been examined during earlier project development; not an untouched future benchmark"}
        atomic_json(root / "metrics/graph_plan.json", plan)
        configs = [{**CONFIG, "transition_weight": weight} for weight in ([.25] if quick else [.1, .25, .4, .5])]
        validation = []
        def fit(cutoff, path, fit_version):
            training = events.filter(F.col("timestamp") <= cutoff)
            co, tr = build_relations(training, neighbor_limit, max_user_items)
            try:
                profile = export_index(co, tr, path, {"model_version": fit_version, "cutoff": cutoff, "algorithm": ALGORITHM})
            finally:
                co.unpersist(); tr.unpersist()
            return training, popularity(training), profile
        for fold, (cutoff, until) in enumerate(zip(bounds[:-1], bounds[1:])):
            print(f"GRAPH validation fold {fold+1}", flush=True)
            path = root / "models" / f"validation-{fold}.sqlite"
            training, pop, _ = fit(cutoff, path, f"{version}:validation-{fold}")
            queries, profile = make_queries(events, cutoff, until, query_cap)
            validation.append({"cutoff": cutoff, "end": until, "cohort": profile,
                "policies": [{"configuration": cfg, "metrics": evaluate(path, queries, pop, cfg, top_k,
                    root / "metrics" / f"validation-{fold}-weight-{cfg['transition_weight']}.jsonl")["graph"]} for cfg in configs]})
        def selection_score(index):
            values = [fold["policies"][index]["metrics"] for fold in validation if fold["cohort"]["eligible_queries"]]
            return tuple(sum(v[key] for v in values)/len(values) if values else -1 for key in ("ndcg_at_k", "recall_at_k", "precision_at_k"))
        selected = max(range(len(configs)), key=selection_score)
        config = configs[selected]
        atomic_json(root / "metrics/graph_validation.json", {"folds": validation, "selected_configuration": config,
                    "selection": "validation mean NDCG then Recall then Precision; fixed policy when quick=True; never ALS"})
        print("GRAPH held-out evaluation (before full-history refit)", flush=True)
        eval_path = root / "models/evaluation_graph.sqlite"
        metric_version = f"{version}:evaluation-before-{bounds[-1]}"
        training, pop, _ = fit(bounds[-1], eval_path, metric_version)
        queries, cohort = make_queries(events, bounds[-1], end, query_cap)
        baseline = als_comparator(training, queries, pop, top_k) if compare_als and queries else None
        result = evaluate(eval_path, queries, pop, config, top_k, root / "metrics/test_rankings.jsonl", baseline)
        metrics = {"protocol_kind": "remaining_session", "k": top_k, "selected": result["graph"],
                   "served": result["graph"], "selected_model_promoted": True,
                   "popularity": result["popularity"], "heldout_test": result["graph"],
                   "als_comparison": {"status": "evaluated" if baseline else "not_run", "metrics": result.get("als"),
                       "policy": "historical-user implicit ALS + identical popularity fallback; no context fold-in",
                       "hyperparameters": {"rank": 16, "regParam": .1, "alpha": 20, "maxIter": 5},
                       "claim": "No general superiority claim; same temporal cutoff, cohort, labels, K and seen scope; ALS user-ID input differs from Graph session input; baseline not tuned"},
                   "label_profile": cohort, "metric_model_version": metric_version,
                   "served_policy": f"Spark conditional item-item {1-config['transition_weight']:.0%} + transition {config['transition_weight']:.0%}",
                   "test_reuse_disclosure": plan["test_reuse_disclosure"],
                   "limitations": ["Conditional on a session with prior context and a later novel item; excludes single-event sessions.",
                       "Cold users/items remain in cohort; no negative sampling. Unknown items need observed edges or fallback.",
                       "Historical test graph excludes future events; serving graph refit on all input after scores frozen.",
                       "Graph top-neighbor and user-history caps bound retrieval. Scores are not purchase probabilities."]}
        atomic_json(root / "metrics/model_metrics.json", metrics)
        print("GRAPH serving refit on all input", flush=True)
        _, pop_all, index_profile = fit(end, serving / "session_neighbors.sqlite", version)
        export_context(events, serving / "session_neighbors.sqlite")
        index_profile.update(sha256=checksum(serving / "session_neighbors.sqlite"), bytes=(serving / "session_neighbors.sqlite").stat().st_size)
        seen_count = _export_seen_sqlite(events.select("visitorid", "itemid").distinct(), serving / "seen_items.sqlite")
        # Small inspectable list for /api/users; recommendations are scored live.
        user_sample = [int(r.visitorid) for r in events.select("visitorid").distinct().orderBy("visitorid").limit(1000).collect()]
        bundle = {"schema_version": 2, "run_id": run_id, "model_version": version, "algorithm": ALGORITHM,
                  "metric_model_version": metric_version, "trained_at_utc": utc_now(),
                  "recommendations": {}, "seen_items": {}, "sample_users": user_sample,
                  "seen_items_store": "seen_items.sqlite", "popular_items": pop_all, "metrics": metrics,
                  "source": source or {}, "training_plan": plan,
                  "serving_scope": {"kind": "on-demand graph; no exported ALS user lists", "seen_pairs": seen_count},
                  "session_model": {"model_version": version, "metric_model_version": metric_version,
                      "index_file": "session_neighbors.sqlite", "configuration": config, "metrics": metrics,
                      "index_profile": index_profile}}
        hashes = {name: checksum(serving / name) for name in ("session_neighbors.sqlite", "seen_items.sqlite")}
        bundle["artifact_hashes"] = hashes
        atomic_json(serving / "model_bundle.json", bundle)
        atomic_json(serving / "manifest.json", {"run_id": run_id, "model_version": version,
                    "sha256": {**hashes, "model_bundle.json": checksum(serving / "model_bundle.json")}})
        counts = {r.event: int(r['count']) for r in events.groupBy("event").count().collect()}
        atomic_json(root / "metrics/kpis.json", {"run_id": run_id, "silver_events": count,
                    "visitors": events.select("visitorid").distinct().count(),
                    "items": events.select("itemid").distinct().count(), "event_counts": counts})
        return {"algorithm": ALGORITHM, "model_version": version, "metric_model_version": metric_version,
                "events": count, "graph": index_profile, "metrics": metrics,
                "elapsed_seconds": round(time.monotonic()-started, 3)}
    finally:
        events.unpersist()


def train_model(spark, run_root, run_id, quick=True, top_k=10):
    """Main integrated pipeline entry point: pin Delta Silver then train Graph."""
    from delta.tables import DeltaTable
    uri = lake_path(run_root, "silver")
    version = int(DeltaTable.forPath(spark, uri).history(1).select("version").first()[0])
    events = spark.read.format("delta").option("versionAsOf", version).load(uri)
    return train_graph(spark, run_root, run_id, events, quick, top_k,
                       query_cap=int(os.getenv("GRAPH_EVAL_MAX_QUERIES", "5000")),
                       source={"format": "delta", "uri": uri, "version": version})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default="data/sample/events_10000.csv")
    p.add_argument("--format", choices=["csv", "parquet", "delta"], default="csv")
    p.add_argument("--run-id", required=True)
    p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "outputs"))
    p.add_argument("--master", default="local[2]")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--query-cap", type=int, default=5000)
    p.add_argument("--full-grid", action="store_true")
    p.add_argument("--compare-als", action="store_true")
    p.add_argument("--activate", action="store_true", help="Activate Graph only after training and artifact validation")
    args = p.parse_args()
    root = run_path(args.run_id, args.output_root)
    if root.exists():
        p.error("Run already exists; choose a new --run-id")
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.master(args.master).appName("RetailRocket-Graph-Phase5").config("spark.sql.shuffle.partitions", "8")
    if args.format == "delta":
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    report = {"run_id": args.run_id, "source_run_id": None, "status": "running", "started_at_utc": utc_now(),
              "scope": "standalone offline Graph training; Kafka/HDFS/NoSQL orchestration not exercised"}
    try:
        with pipeline_lock(args.output_root):
            source = {"format": args.format, "uri": args.input}
            if args.format == "csv":
                source["sha256"] = checksum(args.input)
                frame = spark.read.option("header", True).csv(args.input)
            else:
                reader = spark.read.format(args.format)
                if args.format == "delta":
                    from delta.tables import DeltaTable
                    v = int(DeltaTable.forPath(spark, args.input).history(1).select("version").first()[0])
                    reader = reader.option("versionAsOf", v); source["version"] = v
                frame = reader.load(args.input)
            result = train_graph(spark, root, args.run_id, frame, not args.full_grid, args.k, args.compare_als, args.query_cap, source)
            report.update(status="completed", algorithm=ALGORITHM, finished_at_utc=utc_now(), training=result)
            atomic_json(root / "metrics/run_report.json", report)
            if args.activate:
                from service.store import BundleStore, Settings
                store = BundleStore(Settings(output_root=Path(args.output_root), run_id=args.run_id))
                if store.load()[0] is None:
                    raise ValueError("Refusing activation: Graph artifacts failed service validation")
                atomic_json(Path(args.output_root) / "latest_run.json", {"run_id": args.run_id, "model_version": result["model_version"], "activated_at_utc": utc_now()})
            print(json.dumps(result, indent=2))
    except Exception as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        atomic_json(root / "metrics/run_report.json", report)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
