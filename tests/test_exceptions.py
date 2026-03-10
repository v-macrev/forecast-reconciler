from forecast_reconciler.exceptions import (
    ConfigurationError,
    DataValidationError,
    ForecastReconcilerError,
    IntegrityCheckError,
    ReconciliationError,
    SchemaValidationError,
    ZeroBaselineError,
)


def test_exception_hierarchy_is_correct():
    assert issubclass(ConfigurationError, ForecastReconcilerError)
    assert issubclass(SchemaValidationError, ForecastReconcilerError)
    assert issubclass(DataValidationError, ForecastReconcilerError)
    assert issubclass(ReconciliationError, ForecastReconcilerError)
    assert issubclass(ZeroBaselineError, ReconciliationError)
    assert issubclass(IntegrityCheckError, ReconciliationError)


def test_domain_exceptions_preserve_messages():
    error = ZeroBaselineError("No baseline quantity available for group.")

    assert str(error) == "No baseline quantity available for group."