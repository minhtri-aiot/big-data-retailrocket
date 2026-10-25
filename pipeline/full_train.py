"""Full CSV -> validated clean -> HDFS Delta -> temporal Graph -> API bundle.

This is OFFLINE MODEL TRAINING, not a claim of full Kafka replay or full Mongo
facts/session projection. SQLite preserves seen history for every observed user.
"""
import argparse
import json
import os
from pathlib import Path
import time

from pipeline.config import atomic_json, pipeline_lock, run_path, utc_now


def execute(run_id, input_csv, output_root="outputs", resume=False):
    # Safe full-source defaults also apply when called directly without PS1.
    for key,value in {
        "PYSPARK_SUBMIT_ARGS":"--driver-memory 3g --conf spark.driver.maxResultSize=512m --conf spark.local.dir=/tmp/full_spark_scratch pyspark-shell",
        "GRAPH_EVAL_MAX_QUERIES":"5000",
    }.items():
        os.environ.setdefault(key,value)
    root=run_path(run_id,output_root)
    if root.exists() and not resume:
        raise FileExistsError("Use a new run ID to preserve earlier models")
    report={"run_id":run_id,"status":"running","source_run_id":None,
            "started_at_utc":utc_now(),"stages":{},
            "scope":"full-history offline training; no Kafka replay; Mongo/Redis model registry only; complete SQLite seen history"}
    if resume:
        report=json.loads((root/"metrics/run_report.json").read_text())
        if report["status"] == "completed": raise ValueError("Already completed")
        for name in ("evaluation_graph.sqlite",):
            if (root/"models"/name).exists():
                raise ValueError("Preserve partial models before recovery")
        report.update(status="running")
        report.pop("error",None)
        report.setdefault("recovery_attempts",[]).append({"resumed_at_utc":utc_now(),
            "reason":"Docker disk IO interruption; reuse committed full-history data"})
    spark=None
    start=time.monotonic()
    with pipeline_lock(output_root):
        def stage(name):
            report["current_stage"]=name
            atomic_json(root/"metrics/run_report.json",report)
            print(f"[{utc_now()}] FULL TRAIN: {name}",flush=True)
        try:
            from phase3.streaming import create_spark
            stage("spark_startup")
            spark=create_spark("local[4]",enable_delta=True,download_jars=True)
            spark.conf.set("spark.sql.shuffle.partitions","24")
            spark.sparkContext.setCheckpointDir((root/"spark_checkpoints").as_posix())
            spark.conf.set("spark.sql.files.maxPartitionBytes",16*1024*1024)
            if resume:
                stage("reuse_committed_full_history_HDFS")
                assert report["stages"]["history_import"]["raw_rows"] == 2756101
                assert report["stages"]["lake"]["silver_events"] == 2755641
            else:
                from pipeline.history import import_history
                stage("import_all_csv_validate_and_deduplicate")
                report["stages"]["history_import"]=import_history(spark,input_csv,str(root),run_id)
                report["clean_event_count"]=report["stages"]["history_import"]["clean_rows"]
                if report["stages"]["history_import"]["raw_rows"] != 2756101:
                    raise AssertionError("Expected the complete 2,756,101-row RetailRocket source")
                from pipeline.storage import build_lake
                stage("hdfs_delta_bronze_silver_gold")
                report["stages"]["lake"]=build_lake(spark,str(root))
            from pipeline.graph_training import train_model
            stage("temporal_validation_full_history_Graph")
            report["stages"]["training"]=train_model(spark,str(root),run_id,quick=True)
            from pipeline.acid_check import verify_acid
            stage("delta_acid_check")
            report["stages"]["acid"]=verify_acid(spark,str(root))
            from pipeline.runner import publish_model_registry
            stage("publish_model_registry")
            report["stages"]["model_registry"]=publish_model_registry(root,run_id,
                os.getenv("MONGO_URI","mongodb://mongodb:27017"),os.getenv("REDIS_URL","redis://redis:6379/0"))
            report.update(status="completed",finished_at_utc=utc_now(),elapsed_seconds=round(time.monotonic()-start,3))
            atomic_json(root/"metrics/run_report.json",report)
            atomic_json(Path(output_root)/"latest_run.json",{"run_id":run_id,"activated_at_utc":utc_now(),
                "model_version":report["stages"]["model_registry"]["model_version"]})
            return report
        except Exception as error:
            report.update(status="failed",error=f"{type(error).__name__}: {error}",finished_at_utc=utc_now())
            atomic_json(root/"metrics/run_report.json",report)
            raise
        finally:
            if spark: spark.stop()


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id",required=True)
    p.add_argument("--resume",action="store_true")
    p.add_argument("--input",default="data/raw/events.csv")
    p.add_argument("--output-root",default=os.getenv("OUTPUT_ROOT","outputs"))
    args=p.parse_args()
    print(json.dumps(execute(args.run_id,args.input,args.output_root,resume=args.resume),indent=2))
