"""KaggleDownloader fixture for testing."""

import pytest
from dotenv import dotenv_values

from house_price.kaggle_downloader import KaggleDownloader


@pytest.fixture
def kaggle_env(monkeypatch: pytest.MonkeyPatch) -> None:  # type: ignore
    """Fixture to set Kaggle credentials in the environment."""
    monkeypatch.setenv("KAGGLE_USERNAME", dotenv_values().get("KAGGLE_USERNAME", "dummy_user"))
    monkeypatch.setenv("KAGGLE_KEY", dotenv_values().get("KAGGLE_KEY", "dummy_key"))
    yield
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)


@pytest.fixture
def downloader_local(kaggle_env: None) -> KaggleDownloader:
    """Fixture for a KaggleDownloader in local mode."""
    return KaggleDownloader(
        kaggle_dataset="house-prices-advanced-regression-techniques",
        dataset_files=["test.csv", "train.csv"],
        catalog="mlops-test",
        schema="krzyszto",
        volume="raw_kaggle",
        databricks=False,
    )


@pytest.fixture
def downloader_databricks(kaggle_env: None) -> KaggleDownloader:
    """Fixture for a KaggleDownloader in local mode."""
    return KaggleDownloader(
        kaggle_dataset="house-prices-advanced-regression-techniques",
        dataset_files=["test.csv", "train.csv"],
        catalog="mlops-test",
        schema="krzyszto",
        volume="raw_kaggle",
        databricks=True,
    )
