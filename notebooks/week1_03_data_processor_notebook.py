# Databricks notebook source


# COMMAND ----------
import os
os.environ["UV_PROJECT_ENVIRONMENT"] = os.environ["VIRTUAL_ENV"]

# COMMAND ----------
%sh uv sync --locked --no-editable


# COMMAND ----------
from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent / 'src'))

# COMMAND ----------
from loguru import logger
import yaml
import sys
from pyspark.sql import SparkSession
import pandas as pd

from house_price.config import ProjectConfig
from house_price.data_processor import DataProcessor
from marvelous.logging import setup_logging
from marvelous.timer import Timer

config = ProjectConfig.from_yaml(config_path="../project_config.yml", env="dev")

setup_logging(log_file="logs/marvelous-1.log")

logger.info("Configuration loaded:")
logger.info(yaml.dump(config, default_flow_style=False))

# COMMAND ----------

# Load the house prices dataset
spark = SparkSession.builder.getOrCreate()

# Load the data
df = spark.read.table(config.catalog_name + "." + config.schema_name + ".bz_house_prices").toPandas()


# COMMAND ----------
# Load the house prices dataset
with Timer() as preprocess_timer:
    # Initialize DataProcessor
    data_processor = DataProcessor(df, spark, config)

    # Preprocess the data
    data_processor.preprocess()

logger.info(f"Data preprocessing: {preprocess_timer}")

# COMMAND ----------

# Split the data
X_train, X_test = data_processor.split_data()
logger.info("Training set shape: %s", X_train.shape)
logger.info("Test set shape: %s", X_test.shape)

# COMMAND ----------
# Save to catalog
logger.info("Saving data to catalog")
data_processor.save_to_catalog(X_train, X_test)


