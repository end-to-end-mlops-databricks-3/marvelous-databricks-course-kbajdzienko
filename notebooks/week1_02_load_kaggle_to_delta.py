# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data from Kaggle to Delta
# MAGIC


# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dataset = dbutils.widgets.get("dataset")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tbl_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
file_path = f"/Volumes/{catalog}/{schema}/raw_kaggle/{dataset}"

# COMMAND ----------
df = spark.read.csv(f"{file_path}", header=True, inferSchema=True)

if not spark.catalog.tableExists(f"{catalog}.{schema}.{tbl_name}"):
    df.write.format("delta").saveAsTable(f"{catalog}.{schema}.{tbl_name}")

else:
    source = df
    target = DeltaTable.forName(spark, f"{catalog}.{schema}.{tbl_name}")

    pk_string = f"target.{primary_key} = source.{primary_key}"

    (target.alias("target").merge(source.alias("source"), pk_string).whenNotMatchedInsertAll().execute())
