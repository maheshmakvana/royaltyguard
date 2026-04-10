"""Tests for royaltyguard — creator royalty tracking and fraud detection."""
import asyncio
import json
import pytest
from datetime import datetime

from royaltyguard import (
    AnomalyDetector,
    RoyaltyEntry,
    Platform,
    FraudType,
    RoyaltyCache,
    RoyaltyPipeline,
    RoyaltyValidator,
    RoyaltyRule,
    RoyaltyDriftDetector,
    RoyaltyReportExporter,
    diff_entries,
    stream_entries,
    entries_to_ndjson,
    AuditLog,
    PIIScrubber,
    RateLimiter,
    CancellationToken,
)

P1 = datetime(2025, 1, 1)
P2 = datetime(2025, 1, 31)
P3 = datetime(2025, 2, 1)
P4 = datetime(2025, 2, 28)


def make_entry(eid="E1", streams=50000, royalty=175.0, platform=Platform.SPOTIFY, creator="ARTIST-1", track="TRACK-1") -> RoyaltyEntry:
    return RoyaltyEntry(
        entry_id=eid,
        creator_id=creator,
        track_id=track,
        platform=platform,
        period_start=P1,
        period_end=P2,
        streams=streams,
        royalty_amount=royalty,
    )


# ─── Models ───────────────────────────────────────────────────────────────────

def test_royalty_entry_rate_calculated():
    entry = make_entry(streams=100000, royalty=400.0)
    assert abs(entry.rate_per_stream - 0.004) < 1e-9


def test_creator_summary_fraud_loss_pct():
    from royaltyguard.models import CreatorRoyaltySummary
    summary = CreatorRoyaltySummary(creator_id="A", total_royalties=1000.0, estimated_fraud_loss=200.0)
    assert abs(summary.fraud_loss_percentage() - 20.0) < 1e-6


# ─── Detector ─────────────────────────────────────────────────────────────────

def test_no_anomaly_clean_data():
    detector = AnomalyDetector()
    entries = [make_entry(f"E{i}", streams=50000 + i * 1000, royalty=175.0 + i * 3.5) for i in range(5)]
    report = detector.analyze("ARTIST-1", entries)
    assert report.summary.anomalies_detected == 0


def test_bot_stream_spike_detected():
    # Use many consistent baseline entries + an enormous spike well above 5x stdev
    detector = AnomalyDetector(spike_multiplier=3.0)
    entries = [make_entry(f"E{i}", streams=50000) for i in range(20)]
    # 50M streams when baseline is 50K — clearly anomalous
    entries.append(RoyaltyEntry(
        entry_id="E_SPIKE", creator_id="ARTIST-1", track_id="TRACK-1",
        platform=Platform.SPOTIFY,
        period_start=P3, period_end=P4,
        streams=50000000,
        royalty_amount=175000.0,
    ))
    report = detector.analyze("ARTIST-1", entries)
    anomaly_types = [a.fraud_type for a in report.anomalies]
    assert FraudType.BOT_STREAMS in anomaly_types


def test_zero_rate_detected():
    detector = AnomalyDetector(zero_rate_threshold=0.0005, min_rate_usd=0.003)
    entries = [RoyaltyEntry(
        entry_id="E_ZERO", creator_id="ARTIST-1", track_id="TRACK-1",
        platform=Platform.SPOTIFY,
        period_start=P1, period_end=P2,
        streams=200000,
        royalty_amount=0.05,  # near-zero rate
    )]
    report = detector.analyze("ARTIST-1", entries)
    anomaly_types = [a.fraud_type for a in report.anomalies]
    assert FraudType.ZERO_RATE_PAYOUTS in anomaly_types


def test_duplicate_claim_detected():
    detector = AnomalyDetector(duplicate_window_days=10)
    entries = [
        RoyaltyEntry(entry_id="E1", creator_id="A", track_id="T1", platform=Platform.SPOTIFY, period_start=P1, period_end=P2, streams=10000, royalty_amount=35.0),
        RoyaltyEntry(entry_id="E2", creator_id="A", track_id="T1", platform=Platform.SPOTIFY, period_start=datetime(2025, 1, 5), period_end=P2, streams=10000, royalty_amount=35.0),
    ]
    report = detector.analyze("A", entries)
    anomaly_types = [a.fraud_type for a in report.anomalies]
    assert FraudType.DUPLICATE_CLAIM in anomaly_types


def test_report_has_recommendations():
    detector = AnomalyDetector()
    entries = [make_entry()]
    report = detector.analyze("ARTIST-1", entries)
    assert len(report.recommendations) > 0


def test_report_summary_fields():
    detector = AnomalyDetector()
    entries = [make_entry("E1", streams=100000, royalty=400.0), make_entry("E2", streams=80000, royalty=320.0)]
    report = detector.analyze("ARTIST-1", entries)
    assert report.summary.total_streams == 180000
    assert abs(report.summary.total_royalties - 720.0) < 1e-6


# ─── Cache ────────────────────────────────────────────────────────────────────

def test_cache_basic():
    cache = RoyaltyCache(max_size=10, ttl_seconds=60)
    cache.set("k", "v")
    assert cache.get("k") == "v"
    assert cache.get("no") is None


def test_cache_memoize():
    cache = RoyaltyCache()
    calls = [0]

    @cache.memoize
    def analyze_fn(cid):
        calls[0] += 1
        return cid

    assert analyze_fn("X") == "X"
    assert analyze_fn("X") == "X"
    assert calls[0] == 1


def test_cache_stats():
    cache = RoyaltyCache(max_size=10, ttl_seconds=60)
    cache.set("k", "v")
    cache.get("k")
    cache.get("miss")
    s = cache.stats()
    assert s["hits"] == 1
    assert s["misses"] == 1


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def test_pipeline_filter():
    entries = [make_entry(f"E{i}", streams=i * 10000) for i in range(1, 6)]
    pipeline = RoyaltyPipeline().filter(lambda e: e.streams > 20000)
    result = pipeline.run(entries)
    assert all(e.streams > 20000 for e in result)


def test_pipeline_audit_log():
    entries = [make_entry()]
    pipeline = RoyaltyPipeline().filter(lambda e: True, name="pass")
    pipeline.run(entries)
    log = pipeline.audit_log()
    assert log[0]["ok"] is True


def test_pipeline_async():
    entries = [make_entry()]
    pipeline = RoyaltyPipeline().filter(lambda e: True)
    result = asyncio.run(pipeline.arun(entries))
    assert len(result) == 1


# ─── Validator ────────────────────────────────────────────────────────────────

def test_validator_min_streams():
    validator = RoyaltyValidator()
    validator.add_rule(RoyaltyRule("min_streams", 1000))
    entry = make_entry(streams=500)
    ok, errors = validator.validate(entry)
    assert not ok


def test_validator_passes():
    validator = RoyaltyValidator()
    validator.add_rule(RoyaltyRule("min_streams", 100))
    entry = make_entry(streams=5000)
    ok, _ = validator.validate(entry)
    assert ok


# ─── Drift Detector ───────────────────────────────────────────────────────────

def test_drift_detector():
    d = RoyaltyDriftDetector(threshold=0.15)
    d.record(1000.0)
    d.record(1300.0)
    assert d.is_drifted()


def test_no_drift():
    d = RoyaltyDriftDetector(threshold=0.20)
    d.record(1000.0)
    d.record(1050.0)
    assert not d.is_drifted()


# ─── Exporter ─────────────────────────────────────────────────────────────────

def test_exporter_to_json():
    detector = AnomalyDetector()
    report = detector.analyze("ARTIST-1", [make_entry()])
    j = RoyaltyReportExporter.to_json(report)
    data = json.loads(j)
    assert "creator_id" in data


def test_exporter_to_csv():
    detector = AnomalyDetector()
    report = detector.analyze("ARTIST-1", [make_entry()])
    csv = RoyaltyReportExporter.to_csv(report)
    assert "entry_id" in csv


def test_exporter_to_markdown():
    detector = AnomalyDetector()
    report = detector.analyze("ARTIST-1", [make_entry()])
    md = RoyaltyReportExporter.to_markdown(report)
    assert "# Royalty Report" in md


# ─── Diff ─────────────────────────────────────────────────────────────────────

def test_diff_entries():
    a = [make_entry("E1", streams=50000), make_entry("E2", streams=30000)]
    b = [make_entry("E1", streams=60000), make_entry("E3", streams=10000)]
    diff = diff_entries(a, b)
    assert "E3" in diff.added
    assert "E2" in diff.removed
    assert "E1" in diff.modified


# ─── Streaming ────────────────────────────────────────────────────────────────

def test_stream_entries():
    entries = [make_entry(f"E{i}") for i in range(5)]
    result = list(stream_entries(entries))
    assert len(result) == 5


def test_entries_to_ndjson():
    entries = [make_entry("E1")]
    lines = list(entries_to_ndjson(entries))
    assert len(lines) == 1
    assert lines[0].endswith("\n")


# ─── Audit & PII ──────────────────────────────────────────────────────────────

def test_audit_log():
    log = AuditLog()
    log.record("analyzed", "ARTIST-1", detail="3 anomalies")
    entries = log.export()
    assert entries[0]["creator_id"] == "ARTIST-1"


def test_pii_scrubber():
    result = PIIScrubber.scrub("Contact: artist@label.com")
    assert "[EMAIL]" in result
