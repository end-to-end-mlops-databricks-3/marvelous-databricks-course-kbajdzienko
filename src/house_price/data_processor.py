"""Data preprocessing module for house price prediction project."""

import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from sklearn.model_selection import train_test_split

from house_price.config import ProjectConfig

class DataProcessor:
    """ A class for preprocessing and managing DataFrame operations.
    
    This class handles data preprocessing, splitting and saving to Databricks Delta tables.
    """

    def __init__(self, pandas_df: pd.DataFrame, spark: SparkSession):
        """Initialize the DataProcessor with a pandas DataFrame and a Spark session.
        
        Args:
            pandas_df (pd.DataFrame): The input pandas DataFrame to be processed.
            spark (SparkSession): The Spark session for DataFrame operations.
        """
        self.pandas_df = pandas_df
        self.spark = spark
        self.config = config

    def preprocess(self) -> None:
        """ Preprocess the DataFrame stored in self.df.
        
        This method performs the following operations:
            - Fills missing values in the DataFrame.
            - Casts correct datatypes
        """

        # Handle missing values and convert data types as needed
        self.df["LotFrontage"] = pd.to_numeric(self.df["LotFrontage"], errors="coerce")

        self.df["GarageYrBlt"] = pd.to_numeric(self.df["GarageYrBlt"], errors="coerce")
        median_year = self.df["GarageYrBlt"].median()
        self.df["GarageYrBlt"].fillna(median_year, inplace=True)
        current_year = datetime.datetime.now().year

        self.df["GarageAge"] = current_year - self.df["GarageYrBlt"]
        self.df.drop(columns=["GarageYrBlt"], inplace=True)

        # Handle numeric features
        num_features = self.config.num_features
        for col in num_features:
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        # Fill missing values with mean or default values
        self.df.fillna(
            {
                "LotFrontage": self.df["LotFrontage"].mean(),
                "MasVnrType": "None",
                "MasVnrArea": 0,
            },
            inplace=True,
        )

        # Convert categorical features to the appropriate type
        cat_features = self.config.cat_features
        for cat_col in cat_features:
            self.df[cat_col] = self.df[cat_col].astype("category")

        # Extract target and relevant features
        target = self.config.target
        relevant_columns = cat_features + num_features + [target] + ["Id"]
        self.df = self.df[relevant_columns]
        self.df["Id"] = self.df["Id"].astype("str")

    def split_data(self, test_size: float = 0.2, random_state: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split the DataFrame into training and testing sets.
        
        :param test_size: The proportion of the dataset to include in the test split.
        :param random_state: Controls the shuffling applied to the data before applying the split. Answer to everyone's question.
        :return: A tuple containing the training and testing DataFrames.
        """

        train_set, test_set = train_test_split(self.df, test_size=test_size, random_state=random_state)
        return train_set, test_set
    
    def save_to_catalog(self, train_set: pd.DataFrame, test_set: pd.DataFrame) -> None:
        """ Save the training and testing sets to Databricks Delta tables.
        
        :param train_set: The training DataFrame to be saved.
        :param test_set: The testing DataFrame to be saved.
        """
        # Convert pandas DataFrames to Spark DataFrames
        train_set_spark = self.spark.createDataFrame(train_set)
        test_set_spark = self.spark.createDataFrame(test_set)

        # Add timestamp columns
        train_set_spark = train_set_spark.withColumn("created_at", current_timestamp())
        test_set_spark = test_set_spark.withColumn("created_at", current_timestamp())

        # Enable Change Data Capture (CDC) for Delta tables
        self.spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

        # Save to Delta tables
        catalog = self.config.catalog_name
        schema = self.config.schema_name
        train_set_spark.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.train_set")
        test_set_spark.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.test_set")