"""Unit tests for utils class."""

import os

import pytest

from src.house_price.utils import utils


def test_get_project_root_returns_correct_path() -> None:
    """Test that get_project_root returns the correct project root path."""
    # The project root should be two levels up from this test file
    expected_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    actual_root = utils.get_project_root()
    assert actual_root == expected_root


def test_is_databricks_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that is_databricks returns True when DATABRICKS_RUNTIME_VERSION is set."""
    monkeypatch.setitem(os.environ, "DATABRICKS_RUNTIME_VERSION", "13.0")
    assert utils.is_databricks() is True


def test_is_databricks_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that is_databricks returns False when DATABRICKS_RUNTIME_VERSION is not set."""
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert utils.is_databricks() is False
