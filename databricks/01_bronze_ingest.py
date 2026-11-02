# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Incrementally loads the parquet files that Azure Data Factory lands in `bronze/raw/<table>`
# MAGIC into Delta tables using Auto Loader. Bronze keeps every version of every row (append only).

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("catalog", "spotify")

storage_account = dbutils.widgets.get("storage_account")
catalog = dbutils.widgets.get("catalog")
assert storage_account, "storage_account widget is required"

TABLES = ["dim_artist", "dim_track", "dim_user", "dim_date", "fact_stream"]
ROOT = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

# COMMAND ----------

def ingest(table: str) -> None:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{ROOT}/_schemas/{table}")
        .load(f"{ROOT}/raw/{table}")
    )
    (
        stream.writeStream
        .option("checkpointLocation", f"{ROOT}/_checkpoints/{table}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{table}")
        .awaitTermination()
    )


for t in TABLES:
    ingest(t)
    print(f"{t}: {spark.table(f'{catalog}.bronze.{t}').count()} rows in bronze")
