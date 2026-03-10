import forecast_reconciler


def test_package_importable():
    assert forecast_reconciler is not None


def test_version_exists():
    assert hasattr(forecast_reconciler, "__version__")


def test_version_type():
    assert isinstance(forecast_reconciler.__version__, str)