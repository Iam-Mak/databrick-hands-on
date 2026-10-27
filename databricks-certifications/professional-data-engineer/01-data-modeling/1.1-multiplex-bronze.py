# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ## Load Dataset
# MAGIC Run the shared setup notebook to initialize the bookstore environment and download the source data.

# COMMAND ----------

# MAGIC %run ../00-setup/copy-datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{bookstore.dataset_path}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(
    f"{bookstore.dataset_path}/kafka-raw"
)
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def process_bronze():
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(schema)
            .load(f"{bookstore.dataset_path}/kafka-raw")
            .withColumn(
                "timestamp",
                (F.col("timestamp") / 1000).cast("timestamp")
            )
            .withColumn(
                "year_month",
                F.date_format("timestamp", "yyyy-MM")
            )
            .writeStream
            .option("checkpointLocation", f"{bookstore.checkpoint_path}/bronze")
            .option("mergeSchema", True)
            .partitionBy("topic", "year_month")
            .trigger(availableNow=True)
            .table("bronze")
    )

    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze
# MAGIC GROUP BY topic;

# COMMAND ----------

# Load the next available JSON file from kafka-streaming into kafka-raw.
bookstore.load_new_data()

# COMMAND ----------

# Ingest available JSON files from kafka-raw into the Bronze Delta table.
process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze
# MAGIC GROUP BY topic;