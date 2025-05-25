# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data from Kaggle to Delta
# MAGIC
# MAGIC This notebooks is an excercise to practice loading data from external API's to Delta. The motivation for using delta tables is to exploring the lineage and integration between data engineering and ML workflows.
# MAGIC This notebook will require `kaggle` package to be installed in the cluster.

# COMMAND ----------
#%pip install uv

# COMMAND ----------
import os
os.environ["UV_PROJECT_ENVIRONMENT"] = os.environ["VIRTUAL_ENV"]

# COMMAND ----------
%sh uv sync --locked --no-editable

# COMMAND ----------
#%restart_python

# COMMAND ----------
from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent / 'src'))


# COMMAND ----------
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from house_price.kaggle_downloader import KaggleDownloader
from house_price.utils import utils

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------
# Test if volume exists
downloader = KaggleDownloader(
    kaggle_dataset=dbutils.widgets.get("kaggle_dataset"),
    dataset_files=dbutils.widgets.get("dataset_file_names").split(","),
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    volume=dbutils.widgets.get("volume"),
    databricks=utils.is_databricks()
)

downloader.download()

