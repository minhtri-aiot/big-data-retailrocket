"""Run with: uvicorn service.app:app --host 0.0.0.0 --port 8000."""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from pipeline.config import resolve_data_run_id
from service.backends import Backends
from service.store import BundleError, BundleStore, MAX_ID, Settings, validate_items


def number(value: str, label: str, minimum: int, maximum: int) -> int:
    if not re.fullmatch(r"[0-9]+", value) or len(value) > 10:
        raise HTTPException(422, f"{label} must be an integer in [{minimum}, {maximum}]")
    result = int(value)
    if not minimum <= result <= maximum:
        raise HTTPException(422, f"{label} must be in [{minimum}, {maximum}]")
    return result



class SessionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    itemid: StrictInt = Field(ge=0, le=MAX_ID)
    event: Literal["view", "addtocart", "transaction"] = "view"
    timestamp: StrictInt | None = Field(default=None, ge=0)


class SessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    visitorid: StrictInt | None = Field(default=None, ge=0, le=MAX_ID)
    events: list[SessionEvent] = Field(default_factory=list, max_length=100)
    k: StrictInt = Field(default=10, ge=1, le=100)

def create_app(settings: Settings | None = None, backends: Backends | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = BundleStore(settings)
    online = backends or Backends(settings)

    @asynccontextmanager
    async def lifespan(application):
        yield
        online.close()

    application = FastAPI(title="Big Data · Gợi ý sản phẩm", version="1.0.0", lifespan=lifespan)
    application.state.bundle_store = store
    application.state.backends = online

    def require_bundle():
        data, status = store.load()
        if data is None:
            raise HTTPException(503, detail={"message": "No usable model bundle for the selected run", "model": status})
        try:
            data_run = resolve_data_run_id(data["run_id"], settings.output_root)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise HTTPException(503, detail={
                "message": "Invalid model data lineage; history filtering refused",
                "run_id": data["run_id"], "error": type(error).__name__,
            }) from error
        status = {**status, "data_run_id": data_run}
        return data, status

    @application.get("/health/live", tags=["health"])
    def live():
        return {"status": "alive"}

    @application.get("/health/ready", tags=["health"])
    def ready():
        data, status = store.load()
        if data is not None:
            try:
                status = {**status, "data_run_id": resolve_data_run_id(data["run_id"], settings.output_root)}
            except (OSError, ValueError, KeyError, TypeError) as error:
                status = {**status, "ready": False, "error": "Invalid model data lineage",
                          "lineage_error": type(error).__name__}
                data = None
        dependencies = online.statuses()
        degraded = status.get("stale", False) or any(
            row["enabled"] and row["status"] != "ok" for row in dependencies.values())
        return JSONResponse({"status": "not_ready" if data is None else "degraded" if degraded else "ready",
                             "model": status, "dependencies": dependencies},
                            status_code=200 if data is not None else 503)

    @application.get("/api/recommendations/{visitorid}", tags=["recommendations"])
    def recommendations(visitorid: str, k: str = Query("10", description="Integer 1–100")):
        started = time.perf_counter()
        user = number(visitorid, "visitorid", 0, MAX_ID)
        count = number(k, "k", 1, 100)
        bundle, status = require_bundle()
        run, version = bundle["run_id"], bundle["model_version"]
        data_run = status["data_run_id"]
        try:
            seen, has_sidecar = store.seen_history(bundle, user)
        except BundleError as error:
            raise HTTPException(503, str(error)) from error
        history_runs = [run] + ([data_run] if data_run != run else [])
        history_sources = ["bundle"] + (["sqlite"] if has_sidecar else [])
        warnings = []
        for history_run in history_runs:
            live_seen, live_sources, live_warnings = online.history(history_run, user)
            seen.update(live_seen)
            history_sources.extend(live_sources)
            warnings.extend(live_warnings)
        history_sources = list(dict.fromkeys(history_sources))
        warnings = list(dict.fromkeys(warnings))
        if bundle["algorithm"] == "graph_session":
            try:
                from pipeline.session_model import score_context
                context = store.recent_context(bundle, user)
                scored = score_context(store.session_index(bundle), context, bundle["popular_items"], seen,
                                       count, bundle["session_model"]["configuration"], int(time.time()*1000))
            except (OSError, ValueError, sqlite3.Error) as error:
                raise HTTPException(503, "Graph artifact is unavailable") from error
            return {"run_id": run, "data_run_id": data_run, "model_version": version,
                    "trained_at_utc": bundle["trained_at_utc"],
                    "metric_model_version": bundle["metric_model_version"], "algorithm": "graph_session",
                    "visitorid": user, "requested_k": count, "returned": len(scored["items"]),
                    "items": scored["items"], "source": scored["source"], "cache_hit": False,
                    "context_source": "phase5_snapshot_recent_items", "history_sources": history_sources,
                    "history_runs": history_runs, "seen_items_excluded": len(seen), "warnings": warnings,
                    "model_stale": status["stale"], "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "request_ms": round((time.perf_counter()-started)*1000, 3)}
        cached = online.cache_get(run, version, user, count)
        source = ({"als_popularity_hybrid":"hybrid_bundle", "popularity_recent":"popularity_bundle", "item_knn_hybrid":"neighborhood_bundle"}.get(bundle["algorithm"],"als_bundle") if str(user) in bundle["recommendations"] else "popular_fallback")
        candidates = bundle["recommendations"].get(str(user), [])
        # Live history is applied even on cache hits. Fill holes from the current
        # model/popularity lists so a newly seen cached item cannot reappear.
        groups = ([cached["items"]] if cached else []) + [candidates, bundle["popular_items"]]
        selected, emitted = [], set()
        for group in groups:
            for row in group:
                item = row["itemid"]
                if item not in seen and item not in emitted:
                    selected.append({"itemid": item, "score": row["score"]})
                    emitted.add(item)
                    if len(selected) == count:
                        break
            if len(selected) == count:
                break
        if cached:
            source = cached["source"]
        generated = datetime.now(timezone.utc).isoformat()
        online.cache_set({"run_id": run, "model_version": version, "visitorid": user,
                          "k": count, "items": selected, "source": source, "generated_at_utc": generated})
        return {"run_id": run, "data_run_id": data_run, "model_version": version, "trained_at_utc": bundle["trained_at_utc"],
                "algorithm": bundle["algorithm"], "metric_model_version": bundle.get("metric_model_version") or bundle["metrics"].get("metric_model_version"),
                "visitorid": user, "requested_k": count, "returned": len(selected),
                "items": selected, "source": source, "cache_hit": cached is not None,
                "history_sources": history_sources, "history_runs": history_runs, "seen_items_excluded": len(seen),
                "model_stale": status["stale"], "reload_error": status.get("reload_error"),
                "warnings": warnings, "generated_at_utc": generated,
                "request_ms": round((time.perf_counter() - started) * 1000, 3)}

    @application.post("/api/session/recommendations", tags=["recommendations"])
    def session_recommendations(payload: SessionRequest):
        """Use only the submitted past context; requests never share a session cache."""
        started = time.perf_counter()
        now_ms = int(time.time() * 1000)
        events = []
        for row in payload.events:
            timestamp = now_ms if row.timestamp is None else row.timestamp
            if timestamp > now_ms:
                raise HTTPException(422, "Session event timestamp must not be in the future")
            events.append({"itemid": row.itemid, "event": row.event, "timestamp": timestamp})
        bundle, status = require_bundle()
        try:
            index = store.session_index(bundle)
        except BundleError as error:
            raise HTTPException(503, str(error)) from error
        run, version, data_run = bundle["run_id"], bundle["model_version"], status["data_run_id"]
        seen = {row["itemid"] for row in events}
        sources, warnings, history_runs = [], [], []
        if payload.visitorid is not None:
            try:
                historical, has_sidecar = store.seen_history(bundle, payload.visitorid)
            except BundleError as error:
                raise HTTPException(503, str(error)) from error
            seen.update(historical)
            sources = ["bundle"] + (["sqlite"] if has_sidecar else [])
            history_runs = [run] + ([data_run] if data_run != run else [])
            for history_run in history_runs:
                live_seen, live_sources, live_warnings = online.history(history_run, payload.visitorid)
                seen.update(live_seen)
                sources.extend(live_sources)
                warnings.extend(live_warnings)
        info = bundle["session_model"]
        try:
            from pipeline.session_model import score_context
            scored = score_context(index, events, bundle["popular_items"], seen,
                                   k=payload.k, config=info.get("configuration", {}), as_of=now_ms)
            items = validate_items(scored["items"], "session recommendations")
            if len(items) > payload.k or any(row["itemid"] in seen for row in items):
                raise BundleError("Session scorer returned an invalid or already-seen item")
        except (ImportError, OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            raise HTTPException(503, "Session recommendation model is unavailable") from error
        return {"run_id": run, "data_run_id": data_run, "model_version": version,
                "session_model_version": info.get("model_version", version + ":session"),
                "metric_model_version": info.get("metric_model_version"),
                "algorithm": "graph_session" if bundle["algorithm"] == "graph_session" else "session_item_item", "visitorid": payload.visitorid,
                "requested_k": payload.k, "returned": len(items), "items": items,
                "source": scored["source"], "context_events": len(events),
                "context_items": scored["context_items"], "matched_seeds": scored["matched_seeds"],
                "seen_items_excluded": len(seen), "history_sources": list(dict.fromkeys(sources)),
                "history_runs": history_runs, "warnings": list(dict.fromkeys(warnings)),
                "cache_hit": False, "persistence": "client_context_only",
                "model_stale": status["stale"], "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "request_ms": round((time.perf_counter() - started) * 1000, 3)}

    @application.get("/api/metrics", tags=["metrics"])
    def metrics():
        bundle, status = require_bundle()
        kpis, error = store.kpis(bundle["run_id"])
        session = bundle.get("session_model")
        session_status = None
        if session is not None:
            try:
                store.session_index(bundle)
                session_status = {**session, "available": True,
                                  "model_version": session.get("model_version", bundle["model_version"] + ":session")}
            except BundleError as session_error:
                session_status = {**session, "available": False, "error": str(session_error)}
        return {"run_id": bundle["run_id"], "data_run_id": status["data_run_id"], "model_version": bundle["model_version"],
                "trained_at_utc": bundle["trained_at_utc"], "algorithm": bundle["algorithm"],
                "metric_model_version": bundle.get("metric_model_version") or bundle["metrics"].get("metric_model_version"), "model": status,
                "model_metrics": bundle["metrics"], "session_model": session_status,
                "kpis": kpis, "kpi_error": error,
                "serving_scope": bundle.get("serving_scope", {}),
                "exported_recommendation_users": len(bundle["recommendations"]),
                "popular_item_count": len(bundle["popular_items"]),
                "unavailable_metrics": {"ctr": "Source lacks ad/product impression denominators",
                                        "revenue": "Source lacks product prices and order amounts"}}

    @application.get("/api/users", tags=["recommendations"])
    def users(limit: str = Query("20", description="Integer 1–200")):
        maximum = number(limit, "limit", 1, 200)
        bundle, _ = require_bundle()
        values = bundle.get("sample_users", sorted(int(value) for value in bundle["recommendations"]))
        return {"run_id": bundle["run_id"], "model_version": bundle["model_version"],
                "user_ids": values[:maximum], "total_exported_users": len(values),
                "scope": "Snapshot sample; Graph is scored on demand" if bundle["algorithm"] == "graph_session" else "Users with exported personalized recommendations; others use popularity fallback"}

    static = Path(__file__).parent / "static"
    application.mount("/static", StaticFiles(directory=static), name="static")

    @application.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(static / "index.html")

    return application


app = create_app()
