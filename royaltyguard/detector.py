"""Royalty fraud detection engine for royaltyguard."""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from royaltyguard.exceptions import DetectionError
from royaltyguard.models import (
    CreatorRoyaltySummary,
    FraudType,
    RoyaltyEntry,
    RoyaltyReport,
    StreamAnomaly,
)

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detect streaming fraud and royalty anomalies."""

    def __init__(
        self,
        spike_multiplier: float = 5.0,
        min_rate_usd: float = 0.002,
        zero_rate_threshold: float = 0.0001,
        duplicate_window_days: int = 7,
    ) -> None:
        self.spike_multiplier = spike_multiplier
        self.min_rate_usd = min_rate_usd
        self.zero_rate_threshold = zero_rate_threshold
        self.duplicate_window_days = duplicate_window_days

    def _detect_stream_spike(
        self, entries: List[RoyaltyEntry], anomaly_id_prefix: str
    ) -> List[StreamAnomaly]:
        """Flag periods with abnormal stream spikes (bot/click-farm signal)."""
        anomalies: List[StreamAnomaly] = []
        if len(entries) < 3:
            return anomalies

        stream_counts = [e.streams for e in entries]
        mean = statistics.mean(stream_counts)
        stdev = statistics.stdev(stream_counts) if len(stream_counts) > 1 else 0.0

        for i, entry in enumerate(entries):
            if stdev > 0 and entry.streams > mean + self.spike_multiplier * stdev:
                confidence = min(1.0, (entry.streams - mean) / (mean + 1))
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{anomaly_id_prefix}_spike_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.BOT_STREAMS,
                    confidence=round(confidence, 3),
                    evidence={"period": entry.period_start.isoformat(), "streams": entry.streams, "mean": round(mean), "stdev": round(stdev)},
                    estimated_fraud_amount=entry.royalty_amount * confidence,
                    description=f"Stream count {entry.streams:,} is {(entry.streams / mean):.1f}x above average",
                ))
        return anomalies

    def _detect_zero_rate(
        self, entries: List[RoyaltyEntry], anomaly_id_prefix: str
    ) -> List[StreamAnomaly]:
        """Flag entries where rate-per-stream is suspiciously near zero."""
        anomalies: List[StreamAnomaly] = []
        for i, entry in enumerate(entries):
            rate = entry.rate_per_stream or 0.0
            if entry.streams > 100 and 0 < rate < self.zero_rate_threshold:
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{anomaly_id_prefix}_zerorate_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.ZERO_RATE_PAYOUTS,
                    confidence=0.85,
                    evidence={"rate_per_stream": rate, "streams": entry.streams, "expected_min": self.min_rate_usd},
                    estimated_fraud_amount=(self.min_rate_usd - rate) * entry.streams,
                    description=f"Rate per stream ${rate:.6f} is below minimum threshold ${self.min_rate_usd}",
                ))
        return anomalies

    def _detect_duplicates(
        self, entries: List[RoyaltyEntry], anomaly_id_prefix: str
    ) -> List[StreamAnomaly]:
        """Flag duplicate entries within a short window."""
        anomalies: List[StreamAnomaly] = []
        seen: Dict[Tuple[str, str], List[datetime]] = {}
        for entry in entries:
            key = (entry.track_id, entry.platform.value)
            if key not in seen:
                seen[key] = []
            for prev_date in seen[key]:
                delta = abs((entry.period_start - prev_date).days)
                if 0 < delta < self.duplicate_window_days:
                    anomalies.append(StreamAnomaly(
                        anomaly_id=f"{anomaly_id_prefix}_dup_{entry.entry_id}",
                        creator_id=entry.creator_id,
                        track_id=entry.track_id,
                        platform=entry.platform,
                        fraud_type=FraudType.DUPLICATE_CLAIM,
                        confidence=0.9,
                        evidence={"period_start": entry.period_start.isoformat(), "overlap_days": delta},
                        estimated_fraud_amount=entry.royalty_amount,
                        description=f"Duplicate royalty claim within {delta} days",
                    ))
            seen[key].append(entry.period_start)
        return anomalies

    def analyze(
        self,
        creator_id: str,
        entries: List[RoyaltyEntry],
    ) -> RoyaltyReport:
        """Run full fraud detection on a creator's royalty entries."""
        all_anomalies: List[StreamAnomaly] = []
        prefix = creator_id[:8]

        all_anomalies.extend(self._detect_stream_spike(entries, prefix))
        all_anomalies.extend(self._detect_zero_rate(entries, prefix))
        all_anomalies.extend(self._detect_duplicates(entries, prefix))

        total_streams = sum(e.streams for e in entries)
        total_royalties = sum(e.royalty_amount for e in entries)
        fraud_loss = sum(a.estimated_fraud_amount for a in all_anomalies)

        platform_breakdown: Dict[str, float] = {}
        for e in entries:
            platform_breakdown[e.platform.value] = platform_breakdown.get(e.platform.value, 0.0) + e.royalty_amount

        avg_rate = (total_royalties / total_streams) if total_streams > 0 else 0.0

        summary = CreatorRoyaltySummary(
            creator_id=creator_id,
            total_streams=total_streams,
            total_royalties=total_royalties,
            platform_breakdown=platform_breakdown,
            anomalies_detected=len(all_anomalies),
            estimated_fraud_loss=fraud_loss,
            average_rate_per_stream=avg_rate,
            period_start=min((e.period_start for e in entries), default=None),
            period_end=max((e.period_end for e in entries), default=None),
        )

        recs: List[str] = []
        if any(a.fraud_type == FraudType.BOT_STREAMS for a in all_anomalies):
            recs.append("Dispute bot-stream periods with DSP using provided evidence")
        if any(a.fraud_type == FraudType.ZERO_RATE_PAYOUTS for a in all_anomalies):
            recs.append("Request rate audit from distributor for zero-rate entries")
        if any(a.fraud_type == FraudType.DUPLICATE_CLAIM for a in all_anomalies):
            recs.append("Remove duplicate royalty claims before next statement submission")
        if not all_anomalies:
            recs.append("No anomalies detected — royalty data appears clean")

        report = RoyaltyReport(
            creator_id=creator_id,
            summary=summary,
            entries=entries,
            anomalies=all_anomalies,
            recommendations=recs,
        )
        logger.info(
            "Royalty analysis for %s: %d entries, %d anomalies, estimated fraud loss $%.2f",
            creator_id, len(entries), len(all_anomalies), fraud_loss,
        )
        return report
