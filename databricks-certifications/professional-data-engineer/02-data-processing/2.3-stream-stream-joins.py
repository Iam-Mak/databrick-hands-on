# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers_orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00-setup/copy-datasets

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

def batch_upsert(microBatchDF, batchId):
    window  = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())

    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank=1")
        .drop("rank", "_change_type", "_commit_version")
        .withColumnRenamed("_commit_timestamp", "processed_timestamp")
        .createOrReplaceTempView("ranked_updates"))


    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
                THEN UPDATE SET *
            WHEN NOT MATCHED 
                THEN INSERT *
    
    """

    microBatchDF.sparkSession.sql(query)


# COMMAND ----------

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, 
# MAGIC order_timestamp Timestamp, 
# MAGIC customer_id STRING, 
# MAGIC quantity BIGINT, 
# MAGIC total BIGINT, 
# MAGIC books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, 
# MAGIC email STRING, 
# MAGIC first_name STRING, 
# MAGIC last_name STRING, 
# MAGIC gender STRING, 
# MAGIC street STRING, 
# MAGIC city STRING, 
# MAGIC country STRING, 
# MAGIC row_time TIMESTAMP, 
# MAGIC processed_timestamp TIMESTAMP)

# COMMAND ----------

def process_customers_orders():

    orders_df = spark.readStream.table("orders_silver")

    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                        )
    
    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation", f"{bookstore.checkpoint_path}/customers_orders")
                    .trigger(availableNow=True)
                    .start()
             )
    
    query.awaitTermination()

# COMMAND ----------

process_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

process_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customers_orders