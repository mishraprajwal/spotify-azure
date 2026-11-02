# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver transformations
# MAGIC Cleans bronze data and upserts it into silver Delta tables (latest version per key wins).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("catalog", "spotify")

storage_account = dbutils.widgets.get("storage_account")
catalog = dbutils.widgets.get("catalog")
assert storage_account, "storage_account widget is required"

CHECKPOINTS = f"abfss://silver@{storage_account}.dfs.core.windows.net/_checkpoints"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

# COMMAND ----------

# table -> (primary key, ordering column used to pick the latest version, cleaning function)

def clean_artist(df: DataFrame) -> DataFrame:
    return (df.withColumn("artist_name", F.trim("artist_name"))
              .withColumn("genre", F.initcap(F.trim("genre")))
              .withColumn("country", F.upper(F.trim("country"))))


def clean_track(df: DataFrame) -> DataFrame:
    return (df.withColumn("track_name", F.trim("track_name"))
              .withColumn("album_name", F.trim("album_name"))
              .filter(F.col("duration_sec") > 0)
              .withColumn("duration_min", F.round(F.col("duration_sec") / 60, 2)))


def clean_user(df: DataFrame) -> DataFrame:
    return (df.withColumn("user_name", F.trim("user_name"))
              .withColumn("country", F.upper(F.trim("country")))
              .withColumn("subscription_tier", F.lower(F.trim("subscription_tier"))))


def clean_date(df: DataFrame) -> DataFrame:
    return df.withColumn("is_weekend", F.col("is_weekend").cast("boolean"))


def clean_stream(df: DataFrame) -> DataFrame:
    return (df.filter(F.col("ms_played") > 0)
              .withColumn("stream_date", F.to_date("stream_timestamp"))
              .withColumn("device_type", F.lower(F.trim("device_type"))))


SPECS = {
    "dim_artist": ("artist_id", "updated_at", clean_artist),
    "dim_track": ("track_id", "updated_at", clean_track),
    "dim_user": ("user_id", "updated_at", clean_user),
    "dim_date": ("date_value", "date_value", clean_date),
    "fact_stream": ("stream_id", "stream_timestamp", clean_stream),
}

# COMMAND ----------

def make_upsert(target: str, key: str, order_col: str, clean):
    def upsert(batch_df: DataFrame, _batch_id: int) -> None:
        latest = (
            batch_df.withColumn(
                "_rn", F.row_number().over(Window.partitionBy(key).orderBy(F.col(order_col).desc()))
            )
            .filter("_rn = 1")
            .drop("_rn")
        )
        latest = clean(latest)

        if not spark.catalog.tableExists(target):
            latest.write.format("delta").saveAsTable(target)
            return

        (
            DeltaTable.forName(spark, target).alias("t")
            .merge(latest.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition=f"s.{order_col} >= t.{order_col}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    return upsert


for table, (key, order_col, clean) in SPECS.items():
    (
        spark.readStream.table(f"{catalog}.bronze.{table}")
        .writeStream
        .foreachBatch(make_upsert(f"{catalog}.silver.{table}", key, order_col, clean))
        .option("checkpointLocation", f"{CHECKPOINTS}/{table}")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    print(f"{table}: {spark.table(f'{catalog}.silver.{table}').count()} rows in silver")
