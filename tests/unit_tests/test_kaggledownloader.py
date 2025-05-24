"""Unit tests for KaggleDownloader class."""

import os

from house_price.kaggle_downloader import KaggleDownloader


def test_local_path_and_env(downloader_local: KaggleDownloader) -> None:
    """Test local path creation and env loading."""
    assert downloader_local.download_path.endswith("data/mlops-test/krzyszto/raw_kaggle")
    downloader_local.download()
    assert os.path.exists(downloader_local.download_path)
