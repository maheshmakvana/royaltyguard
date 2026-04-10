"""Exceptions for royaltyguard."""


class RoyaltyGuardError(Exception):
    """Base exception for royaltyguard."""


class DetectionError(RoyaltyGuardError):
    """Raised when anomaly detection fails."""


class ValidationError(RoyaltyGuardError):
    """Raised on invalid royalty data."""


class ReportError(RoyaltyGuardError):
    """Raised on report generation failure."""
