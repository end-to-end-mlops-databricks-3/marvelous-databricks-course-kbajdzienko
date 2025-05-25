"""Unit tests for KaggleDownloader class."""

import pandas as pd
import pytest

from house_price.kaggle_downloader import KaggleDownloader


def test_local_path_and_env(downloader_local: KaggleDownloader) -> None:
    """Test local path creation and env loading."""
    assert downloader_local.download_path.endswith("data/mlops-test/krzyszto/raw_kaggle")


def test_init_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that OSError is raised if Kaggle env vars are missing."""
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)
    import pytest

    with pytest.raises(OSError):
        KaggleDownloader(
            kaggle_dataset="dummy",
            dataset_files=[],
            catalog="cat",
            schema="sch",
            volume="vol",
            databricks=False,
        )


def test_databricks_path_not_exists() -> None:
    """Test FileNotFoundError if Databricks path does not exist."""
    with pytest.raises(FileNotFoundError):
        KaggleDownloader(
            kaggle_dataset="dummy",
            dataset_files=[],
            catalog="cat",
            schema="sch",
            volume="vol",
            databricks=True,
        )


def test_rename_to_timestamped_format(downloader_local: KaggleDownloader) -> None:
    """Test that rename_to_timestamped adds a timestamp."""
    name = "file.csv"
    new_name = downloader_local.rename_to_timestamped(name)
    assert new_name.startswith("file_") and new_name.endswith(".csv")
    assert len(new_name) > len(name)


def test_slice_random_fraction(downloader_local: KaggleDownloader) -> None:
    """Test that slice_random returns correct fraction of rows."""
    df = pd.DataFrame({"a": range(100)})
    sliced = downloader_local.slice_random(df, fraction=0.2)
    assert len(sliced) == 20
