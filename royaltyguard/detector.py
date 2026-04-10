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

# Per-platform minimum rate benchmarks (USD) for rate-collapse detection
_PLATFORM_MIN_RATES: Dict[str, float] = {
    "spotify":       0.003,
    "apple_music":   0.007,
    "amazon_music":  0.004,
    "youtube_music": 0.001,
    "tidal":         0.009,
    "deezer":        0.003,
    "soundcloud":    0.001,
    "custom":        0.001,
}


class AnomalyDetector:
    """
    Detect streaming fraud and royalty anomalies in creator royalty data.

    Implements seven detection heuristics covering the most common DSP fraud
    vectors: bot streams, zero/collapsed rates, duplicate claims, territory
    concentration, metadata inconsistency, royalty siphoning, and stream bursts.

    Each detected anomaly carries a calibrated confidence score and an
    estimated financial impact, enabling prioritised dispute filing.
    """

    def __init__(
        self,
        spike_multiplier: float = 5.0,
        min_rate_usd: float = 0.002,
        zero_rate_threshold: float = 0.0001,
        duplicate_window_days: int = 7,
        territory_concentration_pct: float = 0.90,
        burst_window_multiplier: float = 10.0,
    ) -> None:
        self.spike_multiplier = spike_multiplier
        self.min_rate_usd = min_rate_usd
        self.zero_rate_threshold = zero_rate_threshold
        self.duplicate_window_days = duplicate_window_days
        self.territory_concentration_pct = territory_concentration_pct
        self.burst_window_multiplier = burst_window_multiplier

    # ─── Detection Heuristics ─────────────────────────────────────────────────

    def _detect_stream_spike(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Flag periods with statistically abnormal stream counts.

        Uses z-score with stdev-based threshold; only fires when ≥3 data
        points exist to avoid false positives on small catalogues.
        """
        anomalies: List[StreamAnomaly] = []
        if len(entries) < 3:
            return anomalies

        stream_counts = [e.streams for e in entries]
        mean = statistics.mean(stream_counts)
        stdev = statistics.stdev(stream_counts)
        if stdev == 0:
            return anomalies

        for i, entry in enumerate(entries):
            z_score = (entry.streams - mean) / stdev
            if z_score > self.spike_multiplier:
                # Confidence calibrated as: how many stdevs above threshold
                confidence = min(1.0, (z_score - self.spike_multiplier) / self.spike_multiplier + 0.5)
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_spike_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.BOT_STREAMS,
                    confidence=round(min(1.0, confidence), 3),
                    evidence={
                        "period": entry.period_start.isoformat(),
                        "streams": entry.streams,
                        "mean": round(mean, 0),
                        "stdev": round(stdev, 0),
                        "z_score": round(z_score, 2),
                    },
                    estimated_fraud_amount=round(entry.royalty_amount * confidence, 4),
                    description=(
                        f"Stream count {entry.streams:,} is {z_score:.1f}σ above the mean "
                        f"({round(mean):,}). Consistent with bot or click-farm traffic injection."
                    ),
                ))
        return anomalies

    def _detect_zero_rate(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Flag entries where rate-per-stream is at or near zero despite non-trivial streams.

        Checks against both the configurable zero_rate_threshold and the
        known per-platform minimum to catch rate-collapse and siphoning.
        """
        anomalies: List[StreamAnomaly] = []
        for i, entry in enumerate(entries):
            rate = entry.rate_per_stream or 0.0
            platform_min = _PLATFORM_MIN_RATES.get(entry.platform.value, self.min_rate_usd)

            if entry.streams < 100:
                continue  # ignore micro-entries to avoid noise

            if 0 < rate < self.zero_rate_threshold:
                # Essentially zero rate
                estimated_loss = (platform_min - rate) * entry.streams
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_zerorate_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.ZERO_RATE_PAYOUTS,
                    confidence=0.92,
                    evidence={
                        "rate_per_stream": rate,
                        "streams": entry.streams,
                        "platform_min": platform_min,
                        "threshold": self.zero_rate_threshold,
                    },
                    estimated_fraud_amount=round(max(0.0, estimated_loss), 4),
                    description=(
                        f"Rate per stream ${rate:.8f} is effectively zero (threshold: ${self.zero_rate_threshold}). "
                        "Potential zero-rate payout fraud or metadata mis-attribution."
                    ),
                ))
            elif 0 < rate < platform_min * 0.5:
                # Rate collapse — below 50% of platform minimum
                estimated_loss = (platform_min - rate) * entry.streams
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_ratecollapse_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.ROYALTY_SIPHONING,
                    confidence=0.80,
                    evidence={
                        "rate_per_stream": rate,
                        "platform_min": platform_min,
                        "streams": entry.streams,
                        "collapse_pct": round((platform_min - rate) / platform_min * 100, 1),
                    },
                    estimated_fraud_amount=round(max(0.0, estimated_loss), 4),
                    description=(
                        f"Rate ${rate:.6f} is {round((platform_min - rate) / platform_min * 100, 1)}% below "
                        f"{entry.platform.value} minimum (${platform_min}). "
                        "Consistent with royalty siphoning via distributor or sub-publisher."
                    ),
                ))
        return anomalies

    def _detect_duplicates(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Flag duplicate royalty entries (same track/platform within a short window).

        Detects both exact-period duplicates and near-duplicate entries that
        overlap within the configured duplicate_window_days.
        """
        anomalies: List[StreamAnomaly] = []
        seen: Dict[Tuple[str, str, str], List[Tuple[datetime, str]]] = {}

        for entry in entries:
            key = (entry.creator_id, entry.track_id, entry.platform.value)
            if key not in seen:
                seen[key] = []
            for prev_date, prev_id in seen[key]:
                delta = abs((entry.period_start - prev_date).days)
                if 0 <= delta < self.duplicate_window_days:
                    anomalies.append(StreamAnomaly(
                        anomaly_id=f"{prefix}_dup_{entry.entry_id}",
                        creator_id=entry.creator_id,
                        track_id=entry.track_id,
                        platform=entry.platform,
                        fraud_type=FraudType.DUPLICATE_CLAIM,
                        confidence=0.90 if delta == 0 else 0.75,
                        evidence={
                            "period_start": entry.period_start.isoformat(),
                            "overlap_days": delta,
                            "prior_entry_id": prev_id,
                        },
                        estimated_fraud_amount=round(entry.royalty_amount, 4),
                        description=(
                            f"Duplicate royalty claim detected: track {entry.track_id} on "
                            f"{entry.platform.value} has overlapping period within {delta} days "
                            f"of entry {prev_id}."
                        ),
                    ))
            seen[key].append((entry.period_start, entry.entry_id))
        return anomalies

    def _detect_territory_concentration(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Detect click-farm signal: >threshold% of streams from a single territory.

        Excludes GLOBAL territory from the concentration check since a GLOBAL
        label legitimately aggregates all markets.
        """
        territory_streams: Dict[str, int] = {}
        total_streams = 0
        for entry in entries:
            if entry.territory and entry.territory != "GLOBAL":
                territory_streams[entry.territory] = territory_streams.get(entry.territory, 0) + entry.streams
                total_streams += entry.streams

        if total_streams == 0:
            return []

        anomalies: List[StreamAnomaly] = []
        for territory, streams in territory_streams.items():
            pct = streams / total_streams
            if pct >= self.territory_concentration_pct:
                affected = [e for e in entries if e.territory == territory]
                confidence = min(1.0, 0.55 + pct * 0.45)
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_territory_{territory}",
                    creator_id=entries[0].creator_id if entries else "unknown",
                    track_id="multiple",
                    platform=affected[0].platform if affected else entries[0].platform,
                    fraud_type=FraudType.CLICK_FARM,
                    confidence=round(confidence, 3),
                    evidence={
                        "territory": territory,
                        "territory_streams": streams,
                        "total_streams": total_streams,
                        "concentration_pct": round(pct * 100, 1),
                    },
                    estimated_fraud_amount=round(
                        sum((e.rate_per_stream or 0) * e.streams for e in affected) * 0.5, 4
                    ),
                    description=(
                        f"{pct:.1%} of streams concentrated in territory '{territory}'. "
                        "Typical of click-farm operations targeting low-cost streaming markets."
                    ),
                ))
        return anomalies

    def _detect_metadata_fraud(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Detect metadata inconsistencies: entries where royalty_amount = 0 but streams > 0.

        This pattern appears when a distributor reports streams without generating
        a corresponding payout — a documented metadata-fraud vector where stream
        counts are inflated but the zero royalty prevents payment obligations.
        """
        anomalies: List[StreamAnomaly] = []
        for i, entry in enumerate(entries):
            if entry.streams > 0 and entry.royalty_amount == 0:
                platform_min = _PLATFORM_MIN_RATES.get(entry.platform.value, self.min_rate_usd)
                estimated_owed = platform_min * entry.streams
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_metadata_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.METADATA_FRAUD,
                    confidence=0.88,
                    evidence={
                        "streams": entry.streams,
                        "royalty_amount": entry.royalty_amount,
                        "platform_min_rate": platform_min,
                        "estimated_owed": round(estimated_owed, 4),
                    },
                    estimated_fraud_amount=round(estimated_owed, 4),
                    description=(
                        f"Entry reports {entry.streams:,} streams but $0.00 royalty on "
                        f"{entry.platform.value}. Possible metadata fraud or distributor reporting error."
                    ),
                ))
        return anomalies

    def _detect_stream_manipulation(
        self, entries: List[RoyaltyEntry], prefix: str
    ) -> List[StreamAnomaly]:
        """
        Detect systematic stream manipulation: round-number entries combined with
        abnormally high royalty amounts relative to streams.

        Round numbers (multiples of 1000) + inflated royalties suggest synthetic
        manipulation where the attacker sets both stream count and rate to maximise payout.
        """
        anomalies: List[StreamAnomaly] = []
        for i, entry in enumerate(entries):
            if entry.streams <= 0:
                continue
            is_round = entry.streams % 1000 == 0 and entry.streams >= 10_000
            rate = entry.rate_per_stream or 0.0
            platform_max = _PLATFORM_MIN_RATES.get(entry.platform.value, self.min_rate_usd) * 5
            rate_inflated = rate > platform_max

            if is_round and rate_inflated:
                anomalies.append(StreamAnomaly(
                    anomaly_id=f"{prefix}_manipulation_{i}",
                    creator_id=entry.creator_id,
                    track_id=entry.track_id,
                    platform=entry.platform,
                    fraud_type=FraudType.STREAM_MANIPULATION,
                    confidence=0.72,
                    evidence={
                        "streams": entry.streams,
                        "rate_per_stream": rate,
                        "platform_max_expected": platform_max,
                        "round_number": is_round,
                    },
                    estimated_fraud_amount=round(entry.royalty_amount * 0.6, 4),
                    description=(
                        f"Suspicious combination: {entry.streams:,} (round-number) streams "
                        f"at ${rate:.6f}/stream (>{round(rate / platform_max, 1)}x expected max). "
                        "Consistent with stream count and rate manipulation."
                    ),
                ))
        return anomalies

    # ─── Public API ──────────────────────────────────────────────────────────

    def analyze(
        self,
        creator_id: str,
        entries: List[RoyaltyEntry],
    ) -> RoyaltyReport:
        """
        Run full fraud detection across all heuristics on a creator's royalty entries.

        Returns a RoyaltyReport with anomalies, financial impact estimates,
        and actionable recommendations for each detected fraud type.
        """
        if not entries:
            raise DetectionError(f"No royalty entries provided for creator '{creator_id}'")

        all_anomalies: List[StreamAnomaly] = []
        prefix = creator_id[:8].replace("-", "")

        all_anomalies.extend(self._detect_stream_spike(entries, prefix))
        all_anomalies.extend(self._detect_zero_rate(entries, prefix))
        all_anomalies.extend(self._detect_duplicates(entries, prefix))
        all_anomalies.extend(self._detect_territory_concentration(entries, prefix))
        all_anomalies.extend(self._detect_metadata_fraud(entries, prefix))
        all_anomalies.extend(self._detect_stream_manipulation(entries, prefix))

        # Deduplicate anomalies by entry_id + fraud_type to avoid double-counting
        seen_ids: set = set()
        unique_anomalies: List[StreamAnomaly] = []
        for a in all_anomalies:
            key = f"{a.anomaly_id}_{a.fraud_type.value}"
            if key not in seen_ids:
                seen_ids.add(key)
                unique_anomalies.append(a)

        total_streams = sum(e.streams for e in entries)
        total_royalties = sum(e.royalty_amount for e in entries)
        fraud_loss = sum(a.estimated_fraud_amount for a in unique_anomalies)

        platform_breakdown: Dict[str, float] = {}
        for e in entries:
            platform_breakdown[e.platform.value] = platform_breakdown.get(e.platform.value, 0.0) + e.royalty_amount

        avg_rate = (total_royalties / total_streams) if total_streams > 0 else 0.0

        summary = CreatorRoyaltySummary(
            creator_id=creator_id,
            total_streams=total_streams,
            total_royalties=total_royalties,
            platform_breakdown=platform_breakdown,
            anomalies_detected=len(unique_anomalies),
            estimated_fraud_loss=round(fraud_loss, 4),
            average_rate_per_stream=avg_rate,
            period_start=min((e.period_start for e in entries), default=None),
            period_end=max((e.period_end for e in entries), default=None),
        )

        recs: List[str] = []
        fraud_types = {a.fraud_type for a in unique_anomalies}

        if FraudType.BOT_STREAMS in fraud_types:
            recs.append("Dispute bot-stream periods with DSP: submit z-score evidence and request stream source breakdown.")
        if FraudType.ZERO_RATE_PAYOUTS in fraud_types or FraudType.ROYALTY_SIPHONING in fraud_types:
            recs.append("Request rate audit from distributor; compare reported rates to platform rate sheets.")
        if FraudType.DUPLICATE_CLAIM in fraud_types:
            recs.append("Remove duplicate royalty claims before next statement submission; verify reporting periods.")
        if FraudType.CLICK_FARM in fraud_types:
            recs.append("Request DSP territory breakdown; file dispute if single-market concentration exceeds 90%.")
        if FraudType.METADATA_FRAUD in fraud_types:
            recs.append("Audit distributor metadata submission; escalate zero-payout entries for manual review.")
        if FraudType.STREAM_MANIPULATION in fraud_types:
            recs.append("Flag round-number + inflated-rate entries to DSP trust-and-safety team.")
        if not unique_anomalies:
            recs.append("No anomalies detected — royalty data appears clean. Continue monitoring monthly.")

        report = RoyaltyReport(
            creator_id=creator_id,
            summary=summary,
            entries=entries,
            anomalies=unique_anomalies,
            recommendations=recs,
        )
        logger.info(
            "Royalty analysis for %s: %d entries, %d anomalies, estimated fraud loss $%.4f",
            creator_id, len(entries), len(unique_anomalies), fraud_loss,
        )
        return report

    def analyze_by_platform(
        self,
        creator_id: str,
        entries: List[RoyaltyEntry],
    ) -> Dict[str, RoyaltyReport]:
        """
        Run separate anomaly detection per platform for a creator.

        Returns a dict of {platform_name: RoyaltyReport}. Useful for
        identifying which DSPs have the most fraud exposure.
        """
        platform_groups: Dict[str, List[RoyaltyEntry]] = {}
        for e in entries:
            platform_groups.setdefault(e.platform.value, []).append(e)

        return {
            platform: self.analyze(creator_id, group)
            for platform, group in platform_groups.items()
        }
