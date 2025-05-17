# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data from Kaggle to Delta
# MAGIC


# COMMAND ----------
from pyspark.sql.functions import col, rank
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------
dataset = dbutils.widgets.get("dataset")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tbl_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
file_path = f"/Volumes/{catalog}/{schema}/{tbl_name}/{dataset}"

# COMMAND ----------
df = spark.read.csv(f"{file_path}", header=True, inferSchema=True)

if not spark.catalog.tableExists(f"{catalog}.{schema}.{tbl_name}"):
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tbl_name}")

else:
    source = df
    target = DeltaTable.forName(spark, f"{catalog}.{schema}.{tbl_name}")

    (
    target.alias("target")
        .merge(source.alias("source"), f"{primary_key}")
        .whenNotMatchedInsertAll()
        .execute()
    )
