"""Advanced features for royaltyguard — caching, pipeline, async, observability, diff, security."""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar

from royaltyguard.models import RoyaltyEntry, RoyaltyReport, StreamAnomaly

logger = logging.getLogger(__name__)
T = TypeVar("T")


# ─────────────────────────────────────────────────────────────────────────────
# CACHING
# ─────────────────────────────────────────────────────────────────────────────

class RoyaltyCache:
    """LRU + TTL cache for royalty analysis results."""

    def __init__(self, max_size: int = 256, ttl_seconds: float = 1800.0) -> None:
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def _key(self, *args: Any, **kwargs: Any) -> str:
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._store:
                self._misses += 1
                return None
            value, expires_at = self._store[key]
            if time.monotonic() > expires_at:
                del self._store[key]
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.monotonic() + self.ttl_seconds)
            while len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def memoize(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = self._key(fn.__name__, *args, **kwargs)
            cached = self.get(key)
            if cached is not None:
                return cached  # type: ignore[return-value]
            result = fn(*args, **kwargs)
            self.set(key, result)
            return result
        return wrapper

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {"hits": self._hits, "misses": self._misses, "hit_rate": round(self._hits / total, 3) if total else 0.0, "size": len(self._store)}

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def save(self, path: str) -> None:
        import pickle
        with self._lock:
            with open(path, "wb") as f:
                pickle.dump(dict(self._store), f)

    def load(self, path: str) -> None:
        import pickle
        with open(path, "rb") as f:
            data = pickle.load(f)
        with self._lock:
            self._store = OrderedDict(data)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _Step:
    name: str
    fn: Callable
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)


class RoyaltyPipeline:
    """Fluent pipeline for chaining royalty data transforms."""

    def __init__(self) -> None:
        self._steps: List[_Step] = []
        self._audit: List[Dict[str, Any]] = []
        self._retry_count = 0
        self._retry_delay = 0.5

    def map(self, fn: Callable[[List[RoyaltyEntry]], List[RoyaltyEntry]], name: str = "") -> "RoyaltyPipeline":
        self._steps.append(_Step(name=name or fn.__name__, fn=fn))
        return self

    def filter(self, predicate: Callable[[RoyaltyEntry], bool], name: str = "") -> "RoyaltyPipeline":
        def _f(entries: List[RoyaltyEntry]) -> List[RoyaltyEntry]:
            return [e for e in entries if predicate(e)]
        self._steps.append(_Step(name=name or "filter", fn=_f))
        return self

    def with_retry(self, count: int = 3, delay: float = 0.5) -> "RoyaltyPipeline":
        self._retry_count = count
        self._retry_delay = delay
        return self

    def run(self, entries: List[RoyaltyEntry]) -> List[RoyaltyEntry]:
        result = entries
        for step in self._steps:
            attempts = 0
            while True:
                try:
                    t0 = time.monotonic()
                    result = step.fn(result)
                    self._audit.append({"step": step.name, "in": len(entries), "out": len(result), "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "ok": True})
                    break
                except Exception as exc:
                    attempts += 1
                    if attempts > self._retry_count:
                        self._audit.append({"step": step.name, "error": str(exc), "ok": False})
                        raise
                    time.sleep(self._retry_delay)
        return result

    async def arun(self, entries: List[RoyaltyEntry]) -> List[RoyaltyEntry]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.run(entries))

    def audit_log(self) -> List[Dict[str, Any]]:
        return list(self._audit)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RoyaltyRule:
    rule_type: str  # "min_streams", "min_rate", "allowed_platforms", "max_period_days"
    value: Any
    message: str = ""


class RoyaltyValidator:
    """Declarative validator for royalty entries."""

    def __init__(self) -> None:
        self._rules: List[RoyaltyRule] = []

    def add_rule(self, rule: RoyaltyRule) -> "RoyaltyValidator":
        self._rules.append(rule)
        return self

    def validate(self, entry: RoyaltyEntry) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        for rule in self._rules:
            if rule.rule_type == "min_streams" and entry.streams < rule.value:
                errors.append(rule.message or f"Entry {entry.entry_id}: streams {entry.streams} below minimum {rule.value}")
            elif rule.rule_type == "min_rate" and (entry.rate_per_stream or 0) < rule.value:
                errors.append(rule.message or f"Entry {entry.entry_id}: rate per stream {entry.rate_per_stream} below minimum {rule.value}")
            elif rule.rule_type == "allowed_platforms" and entry.platform.value not in rule.value:
                errors.append(rule.message or f"Entry {entry.entry_id}: platform {entry.platform.value} not in allowed list")
        return len(errors) == 0, errors

    def validate_batch(self, entries: List[RoyaltyEntry]) -> Dict[str, List[str]]:
        return {e.entry_id: self.validate(e)[1] for e in entries if not self.validate(e)[0]}


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC & CONCURRENCY
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def async_acquire(self, tokens: float = 1.0) -> bool:
        while not self.acquire(tokens):
            await asyncio.sleep(0.05)
        return True


class CancellationToken:
    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled


def batch_analyze(
    creator_ids: List[str],
    entries_map: Dict[str, List[RoyaltyEntry]],
    analyze_fn: Callable[[str, List[RoyaltyEntry]], RoyaltyReport],
    max_workers: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[RoyaltyReport]:
    results: List[RoyaltyReport] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(analyze_fn, cid, entries_map.get(cid, [])): cid for cid in creator_ids}
        for future in as_completed(futures):
            if token and token.is_cancelled:
                break
            results.append(future.result())
    return results


async def abatch_analyze(
    creator_ids: List[str],
    entries_map: Dict[str, List[RoyaltyEntry]],
    analyze_fn: Callable[[str, List[RoyaltyEntry]], RoyaltyReport],
    max_concurrency: int = 4,
    token: Optional[CancellationToken] = None,
) -> List[RoyaltyReport]:
    sem = asyncio.Semaphore(max_concurrency)
    loop = asyncio.get_event_loop()

    async def run_one(cid: str) -> RoyaltyReport:
        async with sem:
            if token and token.is_cancelled:
                raise asyncio.CancelledError()
            return await loop.run_in_executor(None, lambda: analyze_fn(cid, entries_map.get(cid, [])))

    return list(await asyncio.gather(*[run_one(cid) for cid in creator_ids]))


# ─────────────────────────────────────────────────────────────────────────────
# OBSERVABILITY
# ─────────────────────────────────────────────────────────────────────────────

class RoyaltyProfiler:
    def __init__(self) -> None:
        self._records: List[Dict[str, Any]] = []

    def profile(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            t0 = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                self._records.append({"fn": fn.__name__, "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "ok": True})
                return result
            except Exception as exc:
                self._records.append({"fn": fn.__name__, "elapsed_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc), "ok": False})
                raise
        return wrapper

    def report(self) -> List[Dict[str, Any]]:
        return list(self._records)


class RoyaltyDriftDetector:
    def __init__(self, threshold: float = 0.20) -> None:
        self.threshold = threshold
        self._history: List[float] = []

    def record(self, total_royalties: float) -> None:
        self._history.append(total_royalties)

    def is_drifted(self) -> bool:
        if len(self._history) < 2:
            return False
        prev, latest = self._history[-2], self._history[-1]
        if prev == 0:
            return False
        return abs(latest - prev) / prev > self.threshold


class RoyaltyReportExporter:
    @staticmethod
    def to_json(report: RoyaltyReport) -> str:
        return json.dumps(report.to_dict(), indent=2)

    @staticmethod
    def to_csv(report: RoyaltyReport) -> str:
        lines = ["entry_id,creator_id,track_id,platform,streams,royalty_amount,rate_per_stream"]
        for e in report.entries:
            lines.append(f"{e.entry_id},{e.creator_id},{e.track_id},{e.platform.value},{e.streams},{e.royalty_amount},{e.rate_per_stream or ''}")
        return "\n".join(lines)

    @staticmethod
    def to_markdown(report: RoyaltyReport) -> str:
        d = report.to_dict()
        lines = [f"# Royalty Report — {report.creator_id}", ""]
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        for k, v in d.items():
            if not isinstance(v, (dict, list)):
                lines.append(f"| {k} | {v} |")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# STREAMING
# ─────────────────────────────────────────────────────────────────────────────

def stream_entries(entries: List[RoyaltyEntry]) -> Generator[RoyaltyEntry, None, None]:
    for e in entries:
        yield e


def entries_to_ndjson(entries: List[RoyaltyEntry]) -> Generator[str, None, None]:
    for e in entries:
        yield e.model_dump_json() + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# DIFF
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RoyaltyDiff:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    modified: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def summary(self) -> Dict[str, Any]:
        return {"added": len(self.added), "removed": len(self.removed), "modified": len(self.modified)}

    def to_json(self) -> str:
        return json.dumps({"added": self.added, "removed": self.removed, "modified": self.modified})


def diff_entries(a: List[RoyaltyEntry], b: List[RoyaltyEntry]) -> RoyaltyDiff:
    map_a = {e.entry_id: e for e in a}
    map_b = {e.entry_id: e for e in b}
    diff = RoyaltyDiff(
        added=[eid for eid in map_b if eid not in map_a],
        removed=[eid for eid in map_a if eid not in map_b],
    )
    for eid in set(map_a) & set(map_b):
        changes: Dict[str, Any] = {}
        for f in ("streams", "royalty_amount", "rate_per_stream"):
            va, vb = getattr(map_a[eid], f), getattr(map_b[eid], f)
            if va != vb:
                changes[f] = {"old": va, "new": vb}
        if changes:
            diff.modified[eid] = changes
    return diff


# ─────────────────────────────────────────────────────────────────────────────
# SECURITY
# ─────────────────────────────────────────────────────────────────────────────

class AuditLog:
    def __init__(self) -> None:
        self._entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(self, action: str, creator_id: str, detail: Optional[str] = None) -> None:
        with self._lock:
            self._entries.append({"ts": datetime.utcnow().isoformat(), "action": action, "creator_id": creator_id, "detail": detail})

    def export(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)


class PIIScrubber:
    import re as _re
    _EMAIL = _re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

    @classmethod
    def scrub(cls, text: str) -> str:
        return cls._EMAIL.sub("[EMAIL]", text)
