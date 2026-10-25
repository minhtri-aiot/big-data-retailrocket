"""Validate and atomically activate local model bundles without crossing runs."""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
from datetime import datetime
import json
import hashlib
import math
import os
from pathlib import Path
import re
import sqlite3
from threading import RLock
from typing import Any

MAX_ID = 2**31 - 1
RUN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


class BundleError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    output_root: Path = Path("outputs")
    run_id: str | None = None
    redis_url: str | None = None
    mongo_uri: str | None = None
    mongo_db: str = "bigdata"
    cache_ttl: int = 60
    max_bundle_bytes: int = 256 * 1024 * 1024
    require_graph: bool = False  # Explicit Settings may inspect historical test fixtures.

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            output_root=Path(os.getenv("OUTPUT_ROOT", "outputs")),
            run_id=os.getenv("RUN_ID") or None,
            redis_url=os.getenv("REDIS_URL") or None,
            mongo_uri=os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or None,
            mongo_db=os.getenv("MONGO_DB", "bigdata"),
            require_graph=True,  # Application runtime never serves a legacy ALS bundle.
        )


@dataclass(frozen=True)
class LoadedBundle:
    data: dict
    signature: tuple


def _signature(path: Path) -> tuple:
    info = path.stat()
    return info.st_mtime_ns, info.st_size, info.st_ino


def read_json(path: Path, max_bytes: int = 256 * 1024 * 1024) -> tuple[Any, tuple]:
    before = _signature(path)
    if before[1] > max_bytes:
        raise BundleError("JSON exceeds the configured size limit")
    raw = path.read_text(encoding="utf-8")
    after = _signature(path)
    if before != after:
        raise BundleError("File changed while loading; retry after atomic publication")
    def nonfinite(value):
        raise BundleError(f"Non-finite JSON number: {value}")
    return json.loads(raw, parse_constant=nonfinite), after


def valid_id(value: Any) -> bool:
    return type(value) is int and 0 <= value <= MAX_ID


def user_key(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"0|[1-9][0-9]*", value)) and int(value) <= MAX_ID


def validate_items(items: Any, label: str) -> list[dict]:
    if not isinstance(items, list):
        raise BundleError(f"{label} must be a list")
    seen = set()
    for row in items:
        if not isinstance(row, dict) or not valid_id(row.get("itemid")):
            raise BundleError(f"{label} contains an invalid item ID")
        score = row.get("score")
        if type(score) not in (int, float) or not math.isfinite(score):
            raise BundleError(f"{label} contains an invalid score")
        if row["itemid"] in seen:
            raise BundleError(f"{label} contains duplicate items")
        seen.add(row["itemid"])
    return items


def validate_bundle(data: Any, expected_run: str) -> dict:
    if not isinstance(data, dict) or data.get("run_id") != expected_run:
        raise BundleError("Bundle run_id does not match the selected run")
    version = data.get("model_version")
    if not isinstance(version, str) or not version.strip() or len(version) > 200:
        raise BundleError("model_version must be a nonempty string of at most 200 characters")
    if data.get("algorithm") not in {"graph_session", "als_implicit", "als_popularity_hybrid", "popularity_recent", "item_knn_hybrid"}:
        raise BundleError("Unsupported recommendation algorithm")
    try:
        timestamp = datetime.fromisoformat(data["trained_at_utc"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("timezone missing")
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        raise BundleError("trained_at_utc must be an ISO timestamp with timezone") from error
    recommendations = data.get("recommendations")
    histories = data.get("seen_items")
    if not isinstance(recommendations, dict) or not isinstance(histories, dict):
        raise BundleError("recommendations and seen_items must be user mappings")
    for key, values in recommendations.items():
        if not user_key(key):
            raise BundleError("recommendations contains an invalid visitor ID")
        validate_items(values, f"recommendations[{key}]")
    for key, values in histories.items():
        if not user_key(key) or not isinstance(values, list) or any(not valid_id(item) for item in values):
            raise BundleError("seen_items contains an invalid visitor/item ID")
    # A served model user must have a history entry, including an explicit empty one.
    if set(recommendations) - set(histories):
        raise BundleError("Every exported recommendation user needs a seen_items entry")
    if not validate_items(data.get("popular_items"), "popular_items"):
        raise BundleError("popular_items must provide a usable cold-start fallback")
    if not isinstance(data.get("metrics"), dict):
        raise BundleError("metrics must be an object")
    session = data.get("session_model")
    if data.get("algorithm") == "graph_session":
        if not isinstance(session, dict) or not isinstance(session.get("configuration"), dict) or session.get("model_version") != version:
            raise BundleError("Graph bundle and scorer must share the same model_version")
        if session.get("metric_model_version") != data.get("metric_model_version"):
            raise BundleError("Graph evaluation version mismatch")
        if session.get("configuration", {}).get("fusion") != "strength":
            raise BundleError("Main Graph requires strength fusion configuration")
        if set(data.get("artifact_hashes", {})) != {"session_neighbors.sqlite", "seen_items.sqlite"}:
            raise BundleError("Graph requires both artifact checksums")
    if session is not None:
        if not isinstance(session, dict) or session.get("index_file") != "session_neighbors.sqlite":
            raise BundleError("session_model must declare the run-local session_neighbors.sqlite index")
        if not isinstance(session.get("configuration", {}), dict) or not isinstance(session.get("metrics", {}), dict):
            raise BundleError("Session configuration and metrics must be objects")
        for label in ("model_version", "metric_model_version"):
            if label in session and (not isinstance(session[label], str) or not session[label].strip() or len(session[label]) > 240):
                raise BundleError("Invalid session model version")
    return data


class BundleStore:
    """Requests see one fully validated version, never partially published state.

    If the same run's file fails reload, retain its previous valid version and
    expose stale=true. A pointer to a different invalid/missing run returns 503;
    the previous run is never served under the new run's identity.
    """
    def __init__(self, settings: Settings):
        self.settings = settings
        self.lock = RLock()
        self.current: LoadedBundle | None = None

    def selected_run(self) -> str:
        if self.settings.run_id:
            selected = self.settings.run_id
        else:
            pointer, _ = read_json(self.settings.output_root / "latest_run.json", 1024 * 1024)
            if not isinstance(pointer, dict):
                raise BundleError("latest_run.json must be an object")
            selected = pointer.get("run_id")
        if not isinstance(selected, str) or not RUN_PATTERN.fullmatch(selected):
            raise BundleError("Invalid selected run_id")
        return selected

    def load(self) -> tuple[dict | None, dict]:
        with self.lock:
            try:
                selected = self.selected_run()
            except (OSError, ValueError) as error:
                return None, {"ready": False, "run_id": None, "stale": False,
                              "error": f"Run selection unavailable: {type(error).__name__}"}
            path = self.settings.output_root / "runs" / selected / "serving" / "model_bundle.json"
            try:
                signature = _signature(path)
                if (self.current is not None and self.current.data["run_id"] == selected
                        and self.current.signature == signature):
                    data = self.current.data
                else:
                    candidate, signature = read_json(path, self.settings.max_bundle_bytes)
                    data = validate_bundle(candidate, selected)
                    if self.settings.require_graph and data["algorithm"] != "graph_session":
                        raise BundleError("Train a Phase 5 Graph artifact before serving")
                    self._check_history_store(data)
                    if data["algorithm"] == "graph_session":
                        index = self.session_index(data)
                        self._check_graph_artifacts(data, index)
                    self.current = LoadedBundle(data, signature)
                self._check_history_store(data)
                if data["algorithm"] == "graph_session":
                    self._check_graph_artifacts(data, self.session_index(data))
                return data, self._status(data, False)
            except (OSError, ValueError) as error:
                if self.current is not None and self.current.data["run_id"] == selected:
                    try:
                        self._check_history_store(self.current.data)
                        if self.current.data["algorithm"] == "graph_session":
                            self._check_graph_artifacts(self.current.data, self.session_index(self.current.data))
                    except BundleError:
                        return None, {"ready": False, "run_id": selected, "stale": False,
                                      "error": "Required local history sidecar unavailable"}
                    status = self._status(self.current.data, True)
                    status["reload_error"] = f"New bundle unavailable: {type(error).__name__}"
                    return self.current.data, status
                return None, {"ready": False, "run_id": selected, "stale": False,
                              "error": f"Selected model unavailable: {type(error).__name__}"}

    def _history_path(self, data: dict) -> Path | None:
        name = data.get("seen_items_store")
        if name is None:
            return None
        if name != "seen_items.sqlite":
            raise BundleError("Only the run-local seen_items.sqlite sidecar is supported")
        path = self.settings.output_root / "runs" / data["run_id"] / "serving" / name
        if path.is_symlink():
            raise BundleError("History sidecar must be a regular run-local file")
        return path

    def _check_history_store(self, data: dict) -> None:
        path = self._history_path(data)
        if path is not None:
            try:
                with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)) as db:
                    db.execute("SELECT visitorid, itemid FROM seen_items LIMIT 0")
            except (OSError, sqlite3.Error) as error:
                raise BundleError("Declared run-local history sidecar is unavailable") from error

    def session_index(self, data: dict) -> Path:
        """Resolve a fixed run-local graph; never follow a client-controlled path."""
        session = data.get("session_model")
        if not isinstance(session, dict) or session.get("index_file") != "session_neighbors.sqlite":
            raise BundleError("This model has no session recommendation index")
        path = self.settings.output_root / "runs" / data["run_id"] / "serving" / "session_neighbors.sqlite"
        if path.is_symlink() or not path.is_file():
            raise BundleError("Required run-local session recommendation index is unavailable")
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)) as db:
                db.execute("SELECT seed, candidate, score FROM neighbors LIMIT 0")
                if session.get("configuration", {}).get("transition_weight", 0):
                    db.execute("SELECT seed, candidate, score FROM transitions LIMIT 0")
        except (OSError, sqlite3.Error) as error:
            raise BundleError("Required run-local session recommendation index is unavailable") from error
        return path

    def seen_history(self, data: dict, visitorid: int) -> tuple[set[int], bool]:
        seen = set(data["seen_items"].get(str(visitorid), []))
        path = self._history_path(data)
        if path is not None:
            try:
                with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)) as db:
                    seen.update(row[0] for row in db.execute(
                        "SELECT itemid FROM seen_items WHERE visitorid = ?", (visitorid,))
                                if valid_id(row[0]))
            except (OSError, sqlite3.Error) as error:
                raise BundleError("History lookup failed; unsafe fallback refused") from error
        return seen, path is not None

    def _check_graph_artifacts(self, data, index):
        # Hash once per file signature/version, and again if a sidecar changes.
        for name, expected in data["artifact_hashes"].items():
            path = index.parent / name
            signature = _signature(path)
            key = (str(path), signature, expected)
            verified = getattr(self, "verified_hashes", set())
            if key not in verified:
                with path.open("rb") as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                        raise BundleError("Graph artifact checksum mismatch")
                self.verified_hashes = {entry for entry in verified if entry[0] != str(path)} | {key}
        try:
            with closing(sqlite3.connect(index.resolve().as_uri() + "?mode=ro", uri=True)) as db:
                row = db.execute("SELECT value FROM metadata WHERE key='model_version'").fetchone()
                if row is None or json.loads(row[0]) != data["model_version"]:
                    raise BundleError("Graph index version does not match bundle")
        except (sqlite3.Error, ValueError) as error:
            raise BundleError("Graph index identity unavailable") from error

    def recent_context(self, data, visitorid):
        """Last snapshot items for compatibility user endpoint; no ALS list."""
        path = self.session_index(data)
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            # A graph build without persisted context can safely use popularity.
            try:
                rows = db.execute("SELECT itemid,timestamp,event FROM user_context WHERE visitorid=? ORDER BY timestamp DESC,itemid LIMIT 100", (visitorid,)).fetchall()
            except sqlite3.OperationalError:
                rows = []
        return [{"itemid": int(i), "timestamp": int(t), "event": e} for i, t, e in reversed(rows)]

    @staticmethod
    def _status(data: dict, stale: bool) -> dict:
        return {"ready": True, "run_id": data["run_id"], "model_version": data["model_version"],
                "trained_at_utc": data["trained_at_utc"], "algorithm": data["algorithm"], "stale": stale}

    def kpis(self, run_id: str) -> tuple[dict | None, str | None]:
        try:
            data, _ = read_json(self.settings.output_root / "runs" / run_id / "metrics" / "kpis.json")
            if not isinstance(data, dict) or data.get("run_id", run_id) != run_id:
                raise BundleError("KPI run mismatch")
            return data, None
        except (OSError, ValueError) as error:
            return None, f"KPI file unavailable: {type(error).__name__}"
