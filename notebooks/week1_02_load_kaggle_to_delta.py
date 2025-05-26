# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data from Kaggle to Delta
# MAGIC


# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume")
tbl_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
file_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------
# Read all CSV files. 
# We don't stream here, we simulate data arriving in batches. 
# We are not expecting updates and deletes, only inserts. The CDF logic would be the same for all cases.
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{file_path}")
# Kaggle Downloader creates random slices of the data, so we need to ensure that the primary key is unique
df = df.distinct()


if not spark.catalog.tableExists(f"{catalog}.{schema}.{tbl_name}"):  
    # Enable Change Data Capture (CDC) for Delta tables
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    # Initiatlize Delta table
    df.write.format("delta").saveAsTable(f"{catalog}.{schema}.{tbl_name}")

else:

    # Only merge new data into the existing Delta table
    source = df
    target = DeltaTable.forName(spark, f"{catalog}.{schema}.{tbl_name}")

    pk_string = f"target.{primary_key} = source.{primary_key}"

    (target
     .alias("target")
     .merge(source.alias("source"), pk_string)
     .withSchemaEvolution()
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )


# COMMAND ----------
# Stream Change Data Feed to a History Table
# This table will allow to extract a particular version of the data at any point in time
history_table = f"{tbl_name}_history"

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")

(
    spark
    .readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table(f"{catalog}.{schema}.{tbl_name}")
    .writeStream
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/checkpoints/{history_table}")
    .toTable(f"{catalog}.{schema}.{history_table}")
)
