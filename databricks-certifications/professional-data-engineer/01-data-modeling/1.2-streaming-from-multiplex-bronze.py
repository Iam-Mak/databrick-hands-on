# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>
# MAGIC
# MAGIC ## Load Dataset
# MAGIC Run the shared setup notebook to initialize the bookstore environment and download the source data.

# COMMAND ----------

# MAGIC %run ../00-setup/copy-datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT cast(key AS STRING), cast(value as STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC     SELECT from_json(
# MAGIC         CAST(value AS STRING),
# MAGIC         "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC     ) AS v
# MAGIC     FROM bronze
# MAGIC     WHERE topic = 'orders'
# MAGIC ) AS t

# COMMAND ----------

(
    spark.readStream
         .table("bronze")
         .createOrReplaceTempView("bronze_tmp")
)

# COMMAND ----------

orders_silver_df = spark.sql("""
    SELECT v.*
    FROM (
        SELECT from_json(
            CAST(value AS STRING),
            "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
        ) AS v
        FROM bronze_tmp
        WHERE topic = 'orders'
    ) AS t
""")

# COMMAND ----------

display(orders_silver_df, checkpointLocation = f"{bookstore.checkpoint_path}/tmp/orders_silver_{time.time()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver