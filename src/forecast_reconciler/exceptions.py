from __future__ import annotations


class ForecastReconcilerError(Exception):
    """
    Base exception for all forecast reconciler domain errors.
    """


class ConfigurationError(ForecastReconcilerError):
    """
    Raised when platform configuration is invalid or internally inconsistent.
    """


class SchemaValidationError(ForecastReconcilerError):
    """
    Raised when an input dataset does not satisfy the required schema contract.
    """


class DataValidationError(ForecastReconcilerError):
    """
    Raised when input data values are structurally present but semantically invalid.
    """


class ReconciliationError(ForecastReconcilerError):
    """
    Raised when reconciliation cannot be completed safely.
    """


class ZeroBaselineError(ReconciliationError):
    """
    Raised when a reconciliation group has no usable baseline denominator.
    """


class IntegrityCheckError(ReconciliationError):
    """
    Raised when control totals or validation invariants are violated.
    """