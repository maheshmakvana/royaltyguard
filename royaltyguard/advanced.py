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


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: PLATFORM RATE BENCHMARKER
# ─────────────────────────────────────────────────────────────────────────────

# Industry-standard per-stream royalty rates (USD) as of 2025-2026
_PLATFORM_BENCHMARK_RATES: Dict[str, Dict[str, float]] = {
    "spotify":       {"min": 0.003, "typical": 0.004,  "max": 0.005},
    "apple_music":   {"min": 0.007, "typical": 0.008,  "max": 0.010},
    "amazon_music":  {"min": 0.004, "typical": 0.0059, "max": 0.007},
    "youtube_music": {"min": 0.001, "typical": 0.002,  "max": 0.003},
    "tidal":         {"min": 0.009, "typical": 0.0125, "max": 0.015},
    "deezer":        {"min": 0.003, "typical": 0.0064, "max": 0.008},
    "soundcloud":    {"min": 0.001, "typical": 0.003,  "max": 0.005},
    "custom":        {"min": 0.001, "typical": 0.005,  "max": 0.020},
}


@dataclass
class RateBenchmarkResult:
    """Comparison of a royalty entry's rate vs. industry benchmarks."""
    entry_id: str
    platform: str
    actual_rate: float
    benchmark_min: float
    benchmark_typical: float
    benchmark_max: float
    status: str      # "above_typical", "at_typical", "below_typical", "below_minimum"
    underpayment_estimate: float  # estimated missing royalties if below_minimum

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "platform": self.platform,
            "actual_rate": round(self.actual_rate, 7),
            "benchmark_typical": self.benchmark_typical,
            "status": self.status,
            "underpayment_estimate": round(self.underpayment_estimate, 4),
        }


class PlatformRateBenchmarker:
    """
    Compare per-stream royalty rates against published industry benchmarks.

    Identifies underpaid royalty entries and quantifies estimated underpayment
    so creators and their legal teams can dispute payouts with DSPs backed by
    concrete data.

    Usage::

        benchmarker = PlatformRateBenchmarker()
        results = benchmarker.benchmark_entries(royalty_entries)
        print(benchmarker.to_markdown(results))
    """

    def benchmark_entry(self, entry: RoyaltyEntry) -> RateBenchmarkResult:
        """Benchmark a single royalty entry against platform norms."""
        platform_key = entry.platform.value
        rates = _PLATFORM_BENCHMARK_RATES.get(platform_key, _PLATFORM_BENCHMARK_RATES["custom"])
        actual = entry.rate_per_stream or 0.0

        if actual > rates["typical"]:
            status = "above_typical"
        elif actual >= rates["typical"] * 0.95:
            status = "at_typical"
        elif actual >= rates["min"]:
            status = "below_typical"
        else:
            status = "below_minimum"

        underpayment = 0.0
        if actual < rates["min"] and entry.streams > 0:
            underpayment = (rates["min"] - actual) * entry.streams

        return RateBenchmarkResult(
            entry_id=entry.entry_id,
            platform=platform_key,
            actual_rate=actual,
            benchmark_min=rates["min"],
            benchmark_typical=rates["typical"],
            benchmark_max=rates["max"],
            status=status,
            underpayment_estimate=underpayment,
        )

    def benchmark_entries(self, entries: List[RoyaltyEntry]) -> List[RateBenchmarkResult]:
        """Benchmark all entries; sorted by underpayment_estimate descending."""
        results = [self.benchmark_entry(e) for e in entries]
        return sorted(results, key=lambda x: x.underpayment_estimate, reverse=True)

    def underpaid_entries(self, entries: List[RoyaltyEntry]) -> List[RateBenchmarkResult]:
        """Return only entries where actual rate is below the platform minimum."""
        return [r for r in self.benchmark_entries(entries) if r.status == "below_minimum"]

    def total_underpayment(self, entries: List[RoyaltyEntry]) -> float:
        """Sum of estimated underpayments across all entries."""
        return sum(r.underpayment_estimate for r in self.benchmark_entries(entries))

    def to_markdown(self, results: List[RateBenchmarkResult]) -> str:
        """Render a Markdown benchmarking report table."""
        lines = ["# Royalty Rate Benchmark Report", "",
                 "| Entry ID | Platform | Actual Rate | Typical Rate | Status | Underpayment Est. |",
                 "|----------|----------|------------|--------------|--------|-------------------|"]
        for r in results:
            lines.append(
                f"| {r.entry_id} | {r.platform} | ${r.actual_rate:.6f} | "
                f"${r.benchmark_typical:.6f} | {r.status.replace('_', ' ').upper()} | "
                f"${r.underpayment_estimate:.4f} |"
            )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: STREAMING FRAUD PATTERN LIBRARY
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FraudPatternMatch:
    """Result of matching royalty entries against a known fraud pattern."""
    pattern_id: str
    pattern_name: str
    matched_entries: List[str]     # entry_ids
    confidence: float
    estimated_fraudulent_streams: int
    estimated_fraud_amount: float
    description: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pattern_id": self.pattern_id,
            "pattern_name": self.pattern_name,
            "matched_entry_count": len(self.matched_entries),
            "confidence": round(self.confidence, 3),
            "estimated_fraudulent_streams": self.estimated_fraudulent_streams,
            "estimated_fraud_amount": round(self.estimated_fraud_amount, 4),
            "description": self.description,
        }


class StreamingFraudPatternLibrary:
    """
    Library of known streaming fraud patterns applied as heuristic detectors.

    Each pattern uses purely structural signals from RoyaltyEntry objects —
    no ML required. Patterns are based on documented DSP fraud investigation
    reports and music industry body (IFPI) guidelines.

    Usage::

        library = StreamingFraudPatternLibrary()
        matches = library.scan(entries)
        for match in matches:
            print(match.pattern_name, match.estimated_fraud_amount)
    """

    def scan(self, entries: List[RoyaltyEntry]) -> List[FraudPatternMatch]:
        """Run all patterns against the entry list; return non-empty matches."""
        patterns = [
            self._detect_round_number_streams,
            self._detect_rate_collapse,
            self._detect_territory_concentration,
            self._detect_burst_streaming,
            self._detect_zero_rate_entries,
        ]
        matches = []
        for detector in patterns:
            result = detector(entries)
            if result and result.matched_entries:
                matches.append(result)
        return sorted(matches, key=lambda x: x.estimated_fraud_amount, reverse=True)

    def _detect_round_number_streams(self, entries: List[RoyaltyEntry]) -> FraudPatternMatch:
        """Bot streams often produce suspiciously round stream counts."""
        matched = [e for e in entries if e.streams > 0 and e.streams % 1000 == 0]
        fraud_streams = sum(e.streams for e in matched)
        fraud_amount = sum((e.rate_per_stream or 0) * e.streams for e in matched)
        return FraudPatternMatch(
            pattern_id="SFP-001",
            pattern_name="Round-Number Stream Counts",
            matched_entries=[e.entry_id for e in matched],
            confidence=0.55 if matched else 0.0,
            estimated_fraudulent_streams=fraud_streams,
            estimated_fraud_amount=fraud_amount,
            description="Entries with stream counts that are exact multiples of 1,000 — a common signature of bot-generated traffic.",
        )

    def _detect_rate_collapse(self, entries: List[RoyaltyEntry]) -> FraudPatternMatch:
        """Royalty siphoning: rate per stream is ≥50% below the platform minimum."""
        matched = []
        for e in entries:
            benchmark = _PLATFORM_BENCHMARK_RATES.get(e.platform.value, _PLATFORM_BENCHMARK_RATES["custom"])
            actual = e.rate_per_stream or 0.0
            if actual > 0 and actual < benchmark["min"] * 0.5:
                matched.append(e)
        fraud_amount = sum(
            (_PLATFORM_BENCHMARK_RATES.get(e.platform.value, _PLATFORM_BENCHMARK_RATES["custom"])["min"] - (e.rate_per_stream or 0)) * e.streams
            for e in matched
        )
        return FraudPatternMatch(
            pattern_id="SFP-002",
            pattern_name="Royalty Rate Collapse",
            matched_entries=[e.entry_id for e in matched],
            confidence=0.80 if matched else 0.0,
            estimated_fraudulent_streams=0,
            estimated_fraud_amount=max(0.0, fraud_amount),
            description="Entries where the per-stream rate is >50% below the known platform minimum — indicative of royalty siphoning or metadata fraud.",
        )

    def _detect_territory_concentration(self, entries: List[RoyaltyEntry]) -> FraudPatternMatch:
        """Click-farm signal: >90% of streams from a single non-global territory."""
        territory_counts: Dict[str, int] = {}
        for e in entries:
            if e.territory != "GLOBAL":
                territory_counts[e.territory] = territory_counts.get(e.territory, 0) + e.streams
        total = sum(e.streams for e in entries)
        matched = []
        if total > 0:
            for territory, count in territory_counts.items():
                if count / total > 0.90:
                    matched = [e for e in entries if e.territory == territory]
                    break
        return FraudPatternMatch(
            pattern_id="SFP-003",
            pattern_name="Territory Concentration",
            matched_entries=[e.entry_id for e in matched],
            confidence=0.65 if matched else 0.0,
            estimated_fraudulent_streams=sum(e.streams for e in matched),
            estimated_fraud_amount=sum((e.rate_per_stream or 0) * e.streams for e in matched) * 0.5,
            description="More than 90% of all streams concentrated in a single non-global territory — typical of click-farm operations.",
        )

    def _detect_burst_streaming(self, entries: List[RoyaltyEntry]) -> FraudPatternMatch:
        """Burst anomaly: a single period has streams > 10× the average across all periods."""
        if len(entries) < 3:
            return FraudPatternMatch("SFP-004", "Burst Streaming", [], 0.0, 0, 0.0, "")
        stream_counts = [e.streams for e in entries]
        avg = sum(stream_counts) / len(stream_counts)
        matched = [e for e in entries if avg > 0 and e.streams > avg * 10]
        return FraudPatternMatch(
            pattern_id="SFP-004",
            pattern_name="Burst Streaming Anomaly",
            matched_entries=[e.entry_id for e in matched],
            confidence=0.70 if matched else 0.0,
            estimated_fraudulent_streams=sum(max(0, e.streams - int(avg * 3)) for e in matched),
            estimated_fraud_amount=sum((e.rate_per_stream or 0) * max(0, e.streams - int(avg * 3)) for e in matched),
            description="One or more periods with stream counts exceeding 10× the average — a strong signal of artificial stream injection.",
        )

    def _detect_zero_rate_entries(self, entries: List[RoyaltyEntry]) -> FraudPatternMatch:
        """Zero-rate entries: streams > 0 but royalty_amount == 0."""
        matched = [e for e in entries if e.streams > 0 and e.royalty_amount == 0]
        return FraudPatternMatch(
            pattern_id="SFP-005",
            pattern_name="Zero-Rate Payout Entries",
            matched_entries=[e.entry_id for e in matched],
            confidence=0.90 if matched else 0.0,
            estimated_fraudulent_streams=sum(e.streams for e in matched),
            estimated_fraud_amount=sum(
                _PLATFORM_BENCHMARK_RATES.get(e.platform.value, _PLATFORM_BENCHMARK_RATES["custom"])["min"] * e.streams
                for e in matched
            ),
            description="Entries with non-zero stream counts but zero royalty payout — potential metadata fraud or distributor siphoning.",
        )

    def to_markdown(self, matches: List[FraudPatternMatch]) -> str:
        """Render a Markdown fraud pattern scan summary."""
        lines = ["# Streaming Fraud Pattern Scan", "",
                 "| Pattern | Matches | Confidence | Est. Fraud Amount |",
                 "|---------|---------|------------|-------------------|"]
        for m in matches:
            lines.append(
                f"| {m.pattern_name} | {len(m.matched_entries)} | "
                f"{m.confidence:.0%} | ${m.estimated_fraud_amount:.4f} |"
            )
        if not matches:
            lines.append("| _No patterns matched_ | — | — | — |")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT: ROYALTY SPAN EMITTER (OpenTelemetry with stdlib fallback)
# ─────────────────────────────────────────────────────────────────────────────

class RoyaltySpanEmitter:
    """
    Emit OpenTelemetry spans for royalty analysis operations.
    Falls back to structured logging when opentelemetry-sdk is not installed.
    """

    def __init__(self, service_name: str = "royaltyguard") -> None:
        self._service = service_name
        self._otel_available = False
        self._tracer: Any = None
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            provider = TracerProvider()
            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(service_name)
            self._otel_available = True
            logger.debug("RoyaltySpanEmitter: OpenTelemetry tracer initialised")
        except ImportError:
            logger.debug("RoyaltySpanEmitter: opentelemetry not installed — using log fallback")

    def span(self, operation: str, attributes: Optional[Dict[str, Any]] = None) -> Any:
        """Context manager: emit an OTEL span or log span start/end."""
        if self._otel_available and self._tracer is not None:
            span = self._tracer.start_span(operation)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, str(v))
            return span
        return _LogSpan(operation, attributes or {}, self._service)

    def emit_anomaly(self, anomaly: StreamAnomaly) -> None:
        """Emit a span for a detected stream anomaly."""
        attrs = {
            "anomaly_id": anomaly.anomaly_id,
            "creator_id": anomaly.creator_id,
            "fraud_type": anomaly.fraud_type.value,
            "confidence": anomaly.confidence,
            "estimated_fraud_amount": anomaly.estimated_fraud_amount,
        }
        with self.span("royaltyguard.anomaly_detected", attrs):
            pass

    def emit_report(self, report: RoyaltyReport) -> None:
        """Emit a span summarising a royalty report."""
        attrs = {
            "creator_id": report.creator_id,
            "entry_count": len(report.entries),
            "anomaly_count": len(report.anomalies),
        }
        with self.span("royaltyguard.report_generated", attrs):
            pass


class _LogSpan:
    """Stdlib-logging fallback span used when OTEL is unavailable."""

    def __init__(self, name: str, attrs: Dict[str, Any], service: str) -> None:
        self._name = name
        self._attrs = attrs
        self._service = service
        self._t0 = time.monotonic()

    def __enter__(self) -> "_LogSpan":
        logger.debug("[span:start] service=%s operation=%s attrs=%s", self._service, self._name, self._attrs)
        return self

    def __exit__(self, *args: Any) -> None:
        elapsed = round((time.monotonic() - self._t0) * 1000, 2)
        logger.debug("[span:end] service=%s operation=%s elapsed_ms=%s", self._service, self._name, elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT v1.2.0: ROYALTY RECONCILIATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReconciliationItem:
    """A single entry from a DSP statement matched against expected royalty."""
    entry_id: str
    platform: str
    track_id: str
    reported_streams: int
    reported_amount: float
    expected_amount: float       # based on benchmark typical rate
    discrepancy: float           # reported - expected
    discrepancy_pct: float
    status: str                  # "matched", "underpaid", "overpaid", "missing"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "platform": self.platform,
            "track_id": self.track_id,
            "reported_streams": self.reported_streams,
            "reported_amount": round(self.reported_amount, 4),
            "expected_amount": round(self.expected_amount, 4),
            "discrepancy": round(self.discrepancy, 4),
            "discrepancy_pct": round(self.discrepancy_pct, 3),
            "status": self.status,
        }


@dataclass
class ReconciliationReport:
    """Summary of a royalty reconciliation run."""
    creator_id: str
    period: str
    total_reported: float
    total_expected: float
    net_discrepancy: float
    items: List[ReconciliationItem]
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def summary(self) -> Dict[str, Any]:
        underpaid = [i for i in self.items if i.status == "underpaid"]
        overpaid = [i for i in self.items if i.status == "overpaid"]
        missing = [i for i in self.items if i.status == "missing"]
        return {
            "creator_id": self.creator_id,
            "period": self.period,
            "total_reported": round(self.total_reported, 4),
            "total_expected": round(self.total_expected, 4),
            "net_discrepancy": round(self.net_discrepancy, 4),
            "underpaid_count": len(underpaid),
            "underpaid_total": round(sum(abs(i.discrepancy) for i in underpaid), 4),
            "overpaid_count": len(overpaid),
            "missing_count": len(missing),
        }


class RoyaltyReconciliationEngine:
    """
    Reconcile DSP royalty statements against expected earnings.

    Compares each RoyaltyEntry's actual payment against the expected amount
    derived from platform benchmark rates. Classifies discrepancies as
    underpaid, overpaid, or matched. Produces a structured reconciliation
    report and Markdown dispute letter template.

    Usage::

        engine = RoyaltyReconciliationEngine(tolerance_pct=0.05)
        report = engine.reconcile(entries, creator_id="creator-001", period="2026-Q1")
        print(engine.to_markdown(report))
    """

    def __init__(self, tolerance_pct: float = 0.05) -> None:
        """
        Args:
            tolerance_pct: Fractional tolerance before flagging a discrepancy.
                           0.05 = entries within ±5% are considered 'matched'.
        """
        self.tolerance_pct = tolerance_pct

    def reconcile(
        self,
        entries: List[RoyaltyEntry],
        creator_id: str,
        period: str,
    ) -> ReconciliationReport:
        """Run reconciliation over a list of royalty entries."""
        items: List[ReconciliationItem] = []
        total_reported = 0.0
        total_expected = 0.0

        for entry in entries:
            platform_key = entry.platform.value
            rates = _PLATFORM_BENCHMARK_RATES.get(platform_key, _PLATFORM_BENCHMARK_RATES["custom"])
            expected = rates["typical"] * entry.streams
            reported = entry.royalty_amount
            total_reported += reported
            total_expected += expected

            if expected == 0:
                status = "missing" if reported == 0 else "matched"
                disc_pct = 0.0
            else:
                disc_pct = (reported - expected) / expected
                if abs(disc_pct) <= self.tolerance_pct:
                    status = "matched"
                elif disc_pct < 0:
                    status = "underpaid"
                else:
                    status = "overpaid"

            items.append(ReconciliationItem(
                entry_id=entry.entry_id,
                platform=platform_key,
                track_id=entry.track_id,
                reported_streams=entry.streams,
                reported_amount=reported,
                expected_amount=expected,
                discrepancy=reported - expected,
                discrepancy_pct=disc_pct,
                status=status,
            ))

        return ReconciliationReport(
            creator_id=creator_id,
            period=period,
            total_reported=total_reported,
            total_expected=total_expected,
            net_discrepancy=total_reported - total_expected,
            items=items,
        )

    def dispute_entries(self, report: ReconciliationReport) -> List[ReconciliationItem]:
        """Return only underpaid entries for dispute filing."""
        return [i for i in report.items if i.status == "underpaid"]

    def to_markdown(self, report: ReconciliationReport) -> str:
        """Render a full Markdown reconciliation report with dispute letter template."""
        s = report.summary()
        lines = [
            f"# Royalty Reconciliation Report — {report.creator_id}",
            f"**Period**: {report.period}  |  **Generated**: {report.generated_at}",
            "",
            "## Summary",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Reported | ${s['total_reported']:.4f} |",
            f"| Total Expected | ${s['total_expected']:.4f} |",
            f"| Net Discrepancy | ${s['net_discrepancy']:+.4f} |",
            f"| Underpaid Entries | {s['underpaid_count']} (${s['underpaid_total']:.4f}) |",
            f"| Overpaid Entries | {s['overpaid_count']} |",
            f"| Missing Entries | {s['missing_count']} |",
            "",
            "## Entry Detail",
            "",
            "| Entry | Platform | Streams | Reported | Expected | Discrepancy | Status |",
            "|-------|----------|---------|----------|----------|-------------|--------|",
        ]
        for item in sorted(report.items, key=lambda x: x.discrepancy):
            lines.append(
                f"| {item.entry_id} | {item.platform} | {item.reported_streams:,} | "
                f"${item.reported_amount:.4f} | ${item.expected_amount:.4f} | "
                f"${item.discrepancy:+.4f} | {item.status.upper()} |"
            )
        if s["underpaid_count"] > 0:
            lines += [
                "",
                "## Dispute Letter Template",
                "",
                f"To: {report.items[0].platform if report.items else 'DSP'} Royalty Disputes Team  ",
                f"From: {report.creator_id}  ",
                f"Re: Royalty Underpayment Dispute — {report.period}  ",
                "",
                f"We have identified {s['underpaid_count']} entries totalling ${s['underpaid_total']:.4f} "
                f"where reported royalties fall below the published per-stream rate for your platform. "
                f"Please review the attached reconciliation report and remit the outstanding balance "
                f"within 30 days. Entry IDs in dispute: "
                + ", ".join(i.entry_id for i in self.dispute_entries(report)[:10]) + ".",
            ]
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EXPERT v1.2.0: CREATOR EARNINGS FORECASTER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EarningsForecast:
    """Projected earnings for a creator over a future horizon."""
    creator_id: str
    historical_periods: int
    historical_total: float
    avg_per_period: float
    trend_pct_per_period: float   # growth or decline rate
    forecast_periods: int
    forecast_values: List[float]  # per-period projection
    forecast_total: float
    confidence: str               # "high", "medium", "low"
    lower_bound: float
    upper_bound: float
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def summary(self) -> Dict[str, Any]:
        return {
            "creator_id": self.creator_id,
            "historical_periods": self.historical_periods,
            "avg_per_period": round(self.avg_per_period, 4),
            "trend_pct_per_period": round(self.trend_pct_per_period * 100, 2),
            "forecast_periods": self.forecast_periods,
            "forecast_total": round(self.forecast_total, 4),
            "lower_bound": round(self.lower_bound, 4),
            "upper_bound": round(self.upper_bound, 4),
            "confidence": self.confidence,
        }


class CreatorEarningsForecaster:
    """
    Project future royalty earnings using linear trend extrapolation.

    Computes a per-period growth rate from historical RoyaltyReports and
    extrapolates N periods forward with uncertainty bounds. Confidence is
    downgraded when historical data is sparse or highly volatile.

    Usage::

        forecaster = CreatorEarningsForecaster()
        forecast = forecaster.forecast(
            creator_id="creator-001",
            historical_reports=reports,   # list of RoyaltyReport, chronological
            periods=4,                    # e.g. 4 quarters ahead
        )
        print(forecast.summary())
        print(forecaster.to_markdown(forecast))
    """

    def forecast(
        self,
        creator_id: str,
        historical_reports: List[RoyaltyReport],
        periods: int = 4,
    ) -> EarningsForecast:
        """
        Generate an earnings forecast from a chronological list of RoyaltyReports.

        Args:
            creator_id: Identifier for logging.
            historical_reports: Chronologically ordered RoyaltyReport list (oldest first).
            periods: Number of future periods to forecast.
        """
        import statistics

        earnings = [r.total_royalties for r in historical_reports]
        n = len(earnings)
        if n == 0:
            return EarningsForecast(
                creator_id=creator_id, historical_periods=0,
                historical_total=0.0, avg_per_period=0.0,
                trend_pct_per_period=0.0, forecast_periods=periods,
                forecast_values=[0.0] * periods, forecast_total=0.0,
                confidence="low", lower_bound=0.0, upper_bound=0.0,
            )

        avg = sum(earnings) / n
        total = sum(earnings)

        # Compute trend as mean growth rate between consecutive periods
        growth_rates: List[float] = []
        for i in range(1, n):
            if earnings[i - 1] > 0:
                growth_rates.append((earnings[i] - earnings[i - 1]) / earnings[i - 1])

        trend = sum(growth_rates) / len(growth_rates) if growth_rates else 0.0
        volatility = statistics.stdev(growth_rates) if len(growth_rates) >= 2 else 0.0

        # Project forward
        last = earnings[-1] if earnings else avg
        forecast_values: List[float] = []
        for _ in range(periods):
            last = last * (1 + trend)
            forecast_values.append(max(0.0, last))
        forecast_total = sum(forecast_values)

        # Uncertainty: ±2 std deviations applied to forecast total
        uncertainty = forecast_total * volatility * 2
        lower = max(0.0, forecast_total - uncertainty)
        upper = forecast_total + uncertainty

        if n >= 6 and volatility < 0.2:
            confidence = "high"
        elif n >= 3 and volatility < 0.5:
            confidence = "medium"
        else:
            confidence = "low"

        return EarningsForecast(
            creator_id=creator_id,
            historical_periods=n,
            historical_total=round(total, 4),
            avg_per_period=round(avg, 4),
            trend_pct_per_period=round(trend, 6),
            forecast_periods=periods,
            forecast_values=[round(v, 4) for v in forecast_values],
            forecast_total=round(forecast_total, 4),
            confidence=confidence,
            lower_bound=round(lower, 4),
            upper_bound=round(upper, 4),
        )

    def to_markdown(self, forecast: EarningsForecast) -> str:
        """Render a Markdown earnings forecast report."""
        trend_sign = "+" if forecast.trend_pct_per_period >= 0 else ""
        lines = [
            f"# Earnings Forecast — {forecast.creator_id}",
            f"**Historical Periods**: {forecast.historical_periods}  |  "
            f"**Avg/Period**: ${forecast.avg_per_period:.4f}  |  "
            f"**Trend**: {trend_sign}{forecast.trend_pct_per_period:.2f}%/period  |  "
            f"**Confidence**: {forecast.confidence.upper()}",
            "",
            "## Projected Earnings",
            "",
            "| Period | Projected Earnings |",
            "|--------|--------------------|",
        ]
        for i, val in enumerate(forecast.forecast_values, 1):
            lines.append(f"| +{i} | ${val:.4f} |")
        lines += [
            "",
            f"**Total Forecast ({forecast.forecast_periods} periods)**: ${forecast.forecast_total:.4f}",
            f"**90% Range**: ${forecast.lower_bound:.4f} — ${forecast.upper_bound:.4f}",
        ]
        return "\n".join(lines)
