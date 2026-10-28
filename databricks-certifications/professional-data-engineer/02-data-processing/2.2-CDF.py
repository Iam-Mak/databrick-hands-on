# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/CDF.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00-setup/copy-datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE customers_silver
# MAGIC SET TBLPROPERTIES(delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED customers_silver 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("customers_silver", 2)

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 2)
               .table("customers_silver"))

display(cdf_df, checkpointLocation = f"{bookstore.checkpoint_path}/tmp/cdf_{time.time()}")