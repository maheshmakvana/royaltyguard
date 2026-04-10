"""royaltyguard — Creator royalty tracking and streaming fraud detection."""
from royaltyguard.models import (
    CreatorRoyaltySummary,
    FraudType,
    Platform,
    RoyaltyEntry,
    RoyaltyReport,
    StreamAnomaly,
)
from royaltyguard.detector import AnomalyDetector
from royaltyguard.exceptions import (
    DetectionError,
    ReportError,
    RoyaltyGuardError,
    ValidationError,
)
from royaltyguard.advanced import (
    AuditLog,
    CancellationToken,
    PIIScrubber,
    RateLimiter,
    RoyaltyCache,
    RoyaltyDiff,
    RoyaltyDriftDetector,
    RoyaltyPipeline,
    RoyaltyProfiler,
    RoyaltyReportExporter,
    RoyaltyRule,
    RoyaltyValidator,
    abatch_analyze,
    batch_analyze,
    diff_entries,
    entries_to_ndjson,
    stream_entries,
)

__version__ = "1.0.0"
__all__ = [
    # Core
    "AnomalyDetector",
    "RoyaltyEntry",
    "RoyaltyReport",
    "StreamAnomaly",
    "CreatorRoyaltySummary",
    "FraudType",
    "Platform",
    # Exceptions
    "RoyaltyGuardError",
    "DetectionError",
    "ValidationError",
    "ReportError",
    # Advanced
    "RoyaltyCache",
    "RoyaltyPipeline",
    "RoyaltyValidator",
    "RoyaltyRule",
    "RateLimiter",
    "CancellationToken",
    "batch_analyze",
    "abatch_analyze",
    "RoyaltyProfiler",
    "RoyaltyDriftDetector",
    "RoyaltyReportExporter",
    "stream_entries",
    "entries_to_ndjson",
    "RoyaltyDiff",
    "diff_entries",
    "AuditLog",
    "PIIScrubber",
]
