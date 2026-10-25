"""Check Phase 5 -> Phase 6 parity, checksums, versions and cold-start via HTTP.

This verifies the Graph serving path, not Kafka/HDFS/Airflow connectivity.
Use verify_e2e separately for the integrated infrastructure.
"""
import argparse
import json
import os
from pathlib import Path
import urllib.request

from pipeline.config import atomic_json, run_path
from pipeline.graph_training import METRIC_KEYS, checksum, measure
from pipeline.session_model import score_context


def verify(base_url, run_id, output_root="outputs"):
    root = run_path(run_id, output_root)
    serving = root / "serving"
    bundle = json.loads((serving / "model_bundle.json").read_text())
    def request(route, body=None):
        payload = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base_url.rstrip("/")+route, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    assert bundle["algorithm"] == "graph_session"
    for name, sha in bundle["artifact_hashes"].items():
        assert checksum(serving / name) == sha
    ready = request("/health/ready")
    assert ready["status"] in {"ready", "degraded"} and ready["model"]["algorithm"] == "graph_session"
    metrics = request("/api/metrics")
    assert metrics["model_version"] == bundle["model_version"] == metrics["session_model"]["model_version"]
    assert metrics["metric_model_version"] == bundle["metric_model_version"]
    assert metrics["model_metrics"] == bundle["metrics"]
    totals = {key: 0.0 for key in METRIC_KEYS}
    records = [json.loads(line) for line in (root / "metrics/test_rankings.jsonl").read_text().splitlines()]
    for r in records:
        assert all(e["timestamp"] < r["target_timestamp"] for e in r["context"])
        assert not set(r["actual"]) & set(r["seen"])
        for label, listing in r["predictions"].items():
            assert len(listing) == len(set(listing)) and not set(listing) & set(r["seen"])
        for key, val in measure(r["predictions"]["graph"], r["actual"], bundle["metrics"]["k"]).items():
            totals[key] += val
    if records:
        for key, value in totals.items():
            assert abs(value/len(records)-bundle["metrics"]["heldout_test"][key]) < 1e-12
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect(serving / "session_neighbors.sqlite")) as db:
        edge = db.execute("SELECT seed FROM neighbors ORDER BY seed LIMIT 1").fetchone()
    bodies = [{"events": [], "k": 10}, {"events": [{"itemid": 2147483647}], "k": 10}]
    if edge:
        bodies.append({"events": [{"itemid": edge[0], "event": "view", "timestamp": 1}], "k": 10})
    samples = []
    for body in bodies:
        data = request("/api/session/recommendations", body)
        assert data["algorithm"] == "graph_session" and data["model_version"] == bundle["model_version"]
        assert data["session_model_version"] == bundle["model_version"]
        assert not set(x["itemid"] for x in body["events"]) & set(x["itemid"] for x in data["items"])
        if body["events"] and "timestamp" in body["events"][0]:
            expected = score_context(serving/"session_neighbors.sqlite", body["events"], bundle["popular_items"],
                                     set(), 10, bundle["session_model"]["configuration"], 1)
            assert data["items"] == expected["items"]
        samples.append({"source": data["source"], "returned": data["returned"]})
    user = request("/api/recommendations/2147483647")
    assert user["algorithm"] == "graph_session" and user["source"] == "popular_fallback"
    result = {"status": "passed", "run_id": run_id, "algorithm": "graph_session",
              "model_version": bundle["model_version"], "metrics_recomputed_queries": len(records),
              "checks": ["artifact hashes", "API model/metric identity", "Graph scorer parity", "seen filtering", "cold-start"],
              "samples": samples, "scope": "Graph artifact + HTTP; does not assert infrastructure or scheduler execution"}
    atomic_json(root / "metrics/graph_http_verification.json", result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--run-id", required=True)
    p.add_argument("--output-root", default=os.getenv("OUTPUT_ROOT", "outputs"))
    args = p.parse_args()
    print(json.dumps(verify(args.base_url, args.run_id, args.output_root), indent=2))
