# Databricks notebook source
import os
import shutil


key_path = "/Volumes/project_lakehouse/bronze_layer/credentials/gcp-key.json"

try:
  with open(key_path, "r") as f:
    key_content = f.read()
    print("Successfully read the key content")

    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("google.cloud.auth.service.account.json.key", key_content)
    print("Spark configured via Direct Injection")
 
except Exception as e:
  print(f"Error: {e}")

# COMMAND ----------

bronze_path = "gs://data-lakehouse-bronze/raw/*/*/*/*.json"

df_bronze = spark.read.json(bronze_path)

print(f"Total records: {df_bronze.count()}")
df_bronze.show()

# COMMAND ----------

table_name = "project_lakehouse.bronze_layer.btc_raw"

df_bronze.write.format('delta').mode('overwrite').saveAsTable(table_name)

print(f"Success raw data is now a permanent Delta table: {table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Fix the Bronze Raw Table
# MAGIC UPDATE project_lakehouse.bronze_layer.btc_raw
# MAGIC SET source_system = 'databricks_automated_job'
# MAGIC WHERE source_system = 'databricks_autoated_job';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM project_lakehouse.bronze_layer.btc_raw

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df_bronze_raw = spark.read.table("project_lakehouse.bronze_layer.btc_raw")
 
df_silver = df_bronze_raw.select(col('bitcoin.usd').alias("price_usd"),to_timestamp(col("extraction_timestamp")).alias("event_timestamp"),col("source_system"))

display(df_silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists project_lakehouse.silver_layer;

# COMMAND ----------

from delta.tables import DeltaTable

target_table_name = "project_lakehouse.silver_layer.btc_prices_clean"

if not spark.catalog.tableExists(target_table_name):
  df_silver.write.format("delta").saveAsTable(target_table_name)
  print("Created Silver table for the first time")
else:
    target_table = DeltaTable.forName(spark, target_table_name)
    
    target_table.alias("target")\
        .merge(df_silver.alias("source"),
        "target.event_timestamp = source.event_timestamp"
        )\
        .whenNotMatchedInsertAll()\
        .execute()
    print("Merge completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists project_lakehouse.gold_layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table project_lakehouse.gold_layer.btc_price_trends as 
# MAGIC
# MAGIC with price_lagged as (
# MAGIC   select event_timestamp, price_usd,
# MAGIC     lag(price_usd) over (order by event_timestamp) as previous_price
# MAGIC   from project_lakehouse.silver_layer.btc_prices_clean
# MAGIC )
# MAGIC
# MAGIC select event_timestamp, price_usd, previous_price, (price_usd - previous_price) as price_difference,
# MAGIC round(((price_usd - previous_price)/previous_price) * 100, 4) as percentage_change
# MAGIC from price_lagged;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_lakehouse.gold_layer.btc_price_trends;