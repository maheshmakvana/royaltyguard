"""Data models for royaltyguard — creator royalty tracking & fraud detection."""
from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class Platform(str, Enum):
    SPOTIFY = "spotify"
    APPLE_MUSIC = "apple_music"
    YOUTUBE_MUSIC = "youtube_music"
    AMAZON_MUSIC = "amazon_music"
    TIDAL = "tidal"
    DEEZER = "deezer"
    SOUNDCLOUD = "soundcloud"
    CUSTOM = "custom"


class FraudType(str, Enum):
    STREAM_MANIPULATION = "stream_manipulation"
    BOT_STREAMS = "bot_streams"
    CLICK_FARM = "click_farm"
    ROYALTY_SIPHONING = "royalty_siphoning"
    DUPLICATE_CLAIM = "duplicate_claim"
    METADATA_FRAUD = "metadata_fraud"
    ZERO_RATE_PAYOUTS = "zero_rate_payouts"


class RoyaltyEntry(BaseModel):
    """A single royalty payment record."""

    entry_id: str
    creator_id: str
    track_id: str
    platform: Platform
    period_start: datetime
    period_end: datetime
    streams: int = Field(ge=0)
    royalty_amount: float = Field(ge=0)
    currency: str = "USD"
    rate_per_stream: Optional[float] = None
    territory: str = "GLOBAL"
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("entry_id", "creator_id", "track_id")
    @classmethod
    def ids_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("ID must not be empty")
        return v.strip()

    def model_post_init(self, __context: Any) -> None:
        if self.streams > 0 and self.rate_per_stream is None:
            object.__setattr__(self, "rate_per_stream", self.royalty_amount / self.streams)


class StreamAnomaly(BaseModel):
    """Detected anomaly in streaming data."""

    anomaly_id: str
    creator_id: str
    track_id: str
    platform: Platform
    fraud_type: FraudType
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: Dict[str, Any] = Field(default_factory=dict)
    estimated_fraud_amount: float = 0.0
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    description: str = ""


class CreatorRoyaltySummary(BaseModel):
    """Aggregated royalty summary for a creator."""

    creator_id: str
    total_streams: int = 0
    total_royalties: float = 0.0
    platform_breakdown: Dict[str, float] = Field(default_factory=dict)
    anomalies_detected: int = 0
    estimated_fraud_loss: float = 0.0
    average_rate_per_stream: float = 0.0
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None

    def fraud_loss_percentage(self) -> float:
        if self.total_royalties == 0:
            return 0.0
        return (self.estimated_fraud_loss / self.total_royalties) * 100


class RoyaltyReport(BaseModel):
    """Full royalty audit report for a creator."""

    creator_id: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    summary: CreatorRoyaltySummary
    entries: List[RoyaltyEntry] = Field(default_factory=list)
    anomalies: List[StreamAnomaly] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        summary_data = self.summary.model_dump()
        # Convert any datetime fields to ISO strings for JSON serialisation
        for k, v in summary_data.items():
            if hasattr(v, "isoformat"):
                summary_data[k] = v.isoformat()
        return {
            "creator_id": self.creator_id,
            "generated_at": self.generated_at.isoformat(),
            "summary": summary_data,
            "anomaly_count": len(self.anomalies),
            "recommendations": self.recommendations,
        }
