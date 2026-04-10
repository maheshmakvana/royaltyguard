# royaltyguard

**Creator royalty tracking and streaming fraud detection** — detect bot streams, zero-rate payouts, duplicate claims, and royalty siphoning for indie artists, labels, and music platforms.

$2B/year is lost to streaming fraud. Indie creators have zero monitoring tools — enterprise solutions only. `royaltyguard` changes that.

[![PyPI version](https://badge.fury.io/py/royaltyguard.svg)](https://pypi.org/project/royaltyguard/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

## The Problem

- $2B/year in streaming royalty fraud
- Bot streams inflate play counts, diluting the royalty pool for legitimate creators
- Zero-rate payout manipulation cheats creators on per-stream rates
- Indie artists have no affordable monitoring tool — only enterprise DSP solutions exist

## Installation

```bash
pip install royaltyguard
```

## Quick Start

```python
from royaltyguard import AnomalyDetector, RoyaltyEntry, Platform
from datetime import datetime

detector = AnomalyDetector(
    spike_multiplier=5.0,
    min_rate_usd=0.003,
    zero_rate_threshold=0.0005,
)

entries = [
    RoyaltyEntry(
        entry_id="E001", creator_id="ARTIST-42", track_id="TRACK-99",
        platform=Platform.SPOTIFY,
        period_start=datetime(2025, 1, 1), period_end=datetime(2025, 1, 31),
        streams=50000, royalty_amount=175.0,
    ),
    RoyaltyEntry(
        entry_id="E002", creator_id="ARTIST-42", track_id="TRACK-99",
        platform=Platform.SPOTIFY,
        period_start=datetime(2025, 2, 1), period_end=datetime(2025, 2, 28),
        streams=2500000,   # ← massive spike
        royalty_amount=8750.0,
    ),
]

report = detector.analyze("ARTIST-42", entries)

print(f"Anomalies: {report.summary.anomalies_detected}")
print(f"Estimated fraud loss: ${report.summary.estimated_fraud_loss:.2f}")
print(report.recommendations)
```

## Fraud Types Detected

| Fraud Type | Description |
|---|---|
| `BOT_STREAMS` | Abnormal stream spike (5x+ standard deviation) |
| `ZERO_RATE_PAYOUTS` | Rate per stream below minimum threshold |
| `DUPLICATE_CLAIM` | Same track/platform reported twice in overlapping window |
| `ROYALTY_SIPHONING` | Systematic underpayment pattern |
| `STREAM_MANIPULATION` | Statistical manipulation of play counts |

## Platforms Supported

Spotify, Apple Music, YouTube Music, Amazon Music, Tidal, Deezer, SoundCloud, and custom platforms.

## Advanced Features

### Pipeline

```python
from royaltyguard import RoyaltyPipeline

pipeline = (
    RoyaltyPipeline()
    .filter(lambda e: e.streams > 1000, name="min_streams")
    .map(lambda entries: sorted(entries, key=lambda e: -e.royalty_amount), name="sort_by_value")
    .with_retry(count=2)
)

filtered = pipeline.run(entries)
print(pipeline.audit_log())
```

### Caching

```python
from royaltyguard import RoyaltyCache

cache = RoyaltyCache(max_size=512, ttl_seconds=1800)

@cache.memoize
def get_creator_report(creator_id):
    return detector.analyze(creator_id, entries_map[creator_id])

cache.save("royalty_cache.pkl")
print(cache.stats())
```

### Validation

```python
from royaltyguard import RoyaltyValidator, RoyaltyRule

validator = RoyaltyValidator()
validator.add_rule(RoyaltyRule("min_streams", 100, "Ignore micro-plays"))
validator.add_rule(RoyaltyRule("allowed_platforms", ["spotify", "apple_music"]))

valid, errors = validator.validate(entry)
```

### Batch Analysis

```python
from royaltyguard import batch_analyze, abatch_analyze

# Sync
reports = batch_analyze(
    creator_ids=["ARTIST-1", "ARTIST-2"],
    entries_map=entries_by_creator,
    analyze_fn=detector.analyze,
    max_workers=4,
)

# Async
reports = await abatch_analyze(
    creator_ids,
    entries_map,
    detector.analyze,
    max_concurrency=8,
)
```

### Export Reports

```python
from royaltyguard import RoyaltyReportExporter

print(RoyaltyReportExporter.to_json(report))
print(RoyaltyReportExporter.to_csv(report))
print(RoyaltyReportExporter.to_markdown(report))
```

### Diff Between Periods

```python
from royaltyguard import diff_entries

diff = diff_entries(q1_entries, q2_entries)
print(diff.summary())   # {'added': 5, 'removed': 0, 'modified': 12}
print(diff.to_json())
```

### Drift Detection

```python
from royaltyguard import RoyaltyDriftDetector

detector_drift = RoyaltyDriftDetector(threshold=0.20)
for period_total in monthly_royalties:
    detector_drift.record(period_total)

if detector_drift.is_drifted():
    print("Royalty drift detected — investigate payout rates")
```

### Streaming

```python
from royaltyguard import stream_entries, entries_to_ndjson

for entry in stream_entries(all_entries):
    process(entry)

for line in entries_to_ndjson(all_entries):
    output.write(line)
```

## Changelog

### v1.2.1 (2026-04-10)
- Added Changelog section to README for release traceability

### v1.2.0
- Added `RoyaltyReconciliationEngine` — reconcile streaming payouts against distributor statements
- Added `CreatorEarningsForecaster` — forecast creator earnings from streaming trend data
- Expanded SEO keywords for PyPI discoverability

### v1.0.1
- Advanced features: pipeline, caching, validation, diff/trend, streaming, audit log

### v1.0.0
- Initial release: streaming fraud detection, bot stream detection, royalty siphoning, payout anomalies

## License

MIT
