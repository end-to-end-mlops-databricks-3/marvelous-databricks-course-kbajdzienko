# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data from Kaggle to Delta
# MAGIC
# MAGIC This notebooks is an excercise to practice loading data from external API's to Delta. The motivation for using delta tables is to exploring the lineage and integration between data engineering and ML workflows.
# MAGIC This notebook will require `kaggle` package to be installed in the cluster.



# COMMAND ----------
import kaggle
from os import mkdir

# COMMAND ----------
dataset = dbutils.widgets.get("dataset")
dataset_files = ["train.csv", "test.csv"]
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
download_path = f"/Volumes/{catalog}/{schema}/raw_kaggle/{dataset}"

# COMMAND ----------
# Create the download path if it does not exist
try:
    mkdir(download_path)
except FileExistsError:
    print(f"Directory {download_path} already exists.")
except OSError:
    print(f"Creation of the directory {download_path} failed.")
else:
    print(f"Successfully created the directory {download_path}.")

# COMMAND ----------
for file in dataset_files:
    k = kaggle.KaggleApi()
    k.authenticate()
    k.competition_download_file(competition=dataset, file_name=file, path=download_path)

