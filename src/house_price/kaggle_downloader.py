"""Download datasets from Kaggle, and save a random slice of 10% rows to a specified directory with a timestamped filename."""

import os

import kaggle
import pandas as pd

from house_price import PROJECT_DIR


class KaggleDownloader:
    """A class to download datasets from Kaggle and save them to a specified directory.

    Downloaded files are renamed to include a timestamp, and a random fraction of the DataFrame is sliced if file is CSV.
    This allows to test SCD1 functionality as part of preprocessing workflow.
    This class supports both local and Databricks environments.

    This class requiires Kaggle API credentials:
    KAGGLE_USERNAME and KAGGLE_KEY ENV variables must be set for authentication

    Attributes:
        kaggle_dataset (str): The name of the Kaggle dataset to download.
        dataset_files (List[str]): List of files to download from the Kaggle dataset.
        catalog (str): The catalog name for the data storage.
        schema (str): The schema name for the data storage.
        volume (str): The volume name for the data storage.
        databricks (bool): Flag to indicate if running in a Databricks environment.

    Example usage:
     downloader = KaggleDownloader(
         kaggle_dataset="titanic",
         dataset_files=["train.csv", "test.csv"],
         catalog="mycatalog",
         schema="myschema",
         volume="myvolume",
         databricks=False,  # Set True for Databricks, False for local
    )
    downloader.download()

    """

    def __init__(
        self,
        kaggle_dataset: str,
        dataset_files: list[str],
        catalog: str,
        schema: str,
        volume: str,
        databricks: bool,
    ) -> None:
        """Initialize the KaggleDownloader with dataset details and storage configuration.

        :param kaggle_dataset: The name of the Kaggle dataset to download.
        :param dataset_files: List of files to download from the Kaggle dataset.
        :param catalog: The catalog name for the data storage.
        :param schema: The schema name for the data storage.
        :param volume: The volume name for the data storage.
        :param databricks: Flag to indicate if running in a Databricks environment.
        """
        self.kaggle_dataset = kaggle_dataset
        self.dataset_files = dataset_files
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.databricks = databricks

        # KAGGLE_USERNAME and KAGGLE_KEY must be set in the environment
        if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
            raise OSError("KAGGLE_USERNAME and KAGGLE_KEY must be set in the environment.")

        if self.databricks:
            self.download_path = f"/Volumes/{catalog}/{schema}/{volume}"
            # Check if the Databricks path exists
            if not os.path.exists(self.download_path):
                raise FileNotFoundError(f"Databricks path does not exist: {self.download_path}")
        else:
            DOWNLOAD_DIR = PROJECT_DIR / "data" / catalog / schema / volume
            DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            self.download_path = DOWNLOAD_DIR.as_posix()

    def rename_to_timestamped(self, file_name: str) -> str:
        """Rename the file to include a timestamp.

        :param file_name: The original file name.
        :return: The new file name with a timestamp.
        """
        import datetime

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(file_name)
        return f"{base}_{timestamp}{ext}"

    def slice_random(self, df: pd.DataFrame, fraction: float = 0.1) -> pd.DataFrame:
        """Slice a random fraction of the DataFrame.

        :param df: The DataFrame to slice.
        :param fraction: The fraction of the DataFrame to return.
        :return: A sliced DataFrame containing the specified fraction of rows.
        """
        return df.sample(frac=fraction, random_state=42)

    def download(self) -> None:
        """Download files from a Kaggle dataset and save them to the specified directory.

        This method authenticates with the Kaggle API, downloads the specified files,
        renames them to include a timestamp, and slices a random fraction of the DataFrame if applicable.
        """
        k = kaggle.KaggleApi()
        k.authenticate()

        for file in self.dataset_files:
            k.competition_download_file(
                competition=self.kaggle_dataset,
                file_name=file,
                path=self.download_path,
            )

            # Rename the file to include a timestamp
            new_file_name = self.rename_to_timestamped(file)
            old_file_path = os.path.join(self.download_path, file)
            new_file_path = os.path.join(self.download_path, new_file_name)
            os.rename(old_file_path, new_file_path)

            # Slice a random fraction of the DataFrame if needed
            if file.endswith(".csv"):
                df = pd.read_csv(new_file_path)
                sliced_df = self.slice_random(df, fraction=0.1)
                sliced_df.to_csv(new_file_path, index=False)

        print(f"Downloaded files to: {self.download_path}")
