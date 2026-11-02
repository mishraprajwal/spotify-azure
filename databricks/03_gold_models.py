# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold models
# MAGIC * `dim_user_history`  - slowly changing dimension (type 2) built from every bronze version of a user
# MAGIC * `fact_stream_enriched` - streams joined to the user version that was valid at stream time
# MAGIC * `daily_track_streams`, `top_artists` - reporting aggregates

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "spotify")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

# Type 2 history: one row per change, with validity window and current flag.
w = Window.partitionBy("user_id").orderBy("updated_at")

versions = (
    spark.table(f"{catalog}.bronze.dim_user")
    .dropDuplicates(["user_id", "updated_at"])
    .withColumn("user_name", F.trim("user_name"))
    .withColumn("country", F.upper(F.trim("country")))
    .withColumn("subscription_tier", F.lower(F.trim("subscription_tier")))
)

dim_user_history = (
    versions
    .withColumn("_prev_tier", F.lag("subscription_tier").over(w))
    .filter("_prev_tier IS NULL OR _prev_tier <> subscription_tier")  # keep real tier changes only
    .withColumn("_next", F.lead("updated_at").over(w))
    .withColumn("valid_from", F.when(F.col("_prev_tier").isNull(), F.lit("1900-01-01").cast("timestamp"))
                               .otherwise(F.col("updated_at")))
    .withColumn("valid_to", F.coalesce("_next", F.lit("9999-12-31").cast("timestamp")))
    .withColumn("is_current", F.col("_next").isNull())
    .select("user_id", "user_name", "country", "subscription_tier", "valid_from", "valid_to", "is_current")
)

dim_user_history.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.dim_user_history")

# COMMAND ----------

streams = spark.table(f"{catalog}.silver.fact_stream").alias("f")
users = spark.table(f"{catalog}.gold.dim_user_history").alias("u")
tracks = spark.table(f"{catalog}.silver.dim_track").alias("t")
artists = spark.table(f"{catalog}.silver.dim_artist").alias("a")
dates = spark.table(f"{catalog}.silver.dim_date").alias("d")

fact_stream_enriched = (
    streams
    .join(users, (F.col("f.user_id") == F.col("u.user_id"))
                 & (F.col("f.stream_timestamp") >= F.col("u.valid_from"))
                 & (F.col("f.stream_timestamp") < F.col("u.valid_to")), "left")
    .join(tracks, F.col("f.track_id") == F.col("t.track_id"), "left")
    .join(artists, F.col("t.artist_id") == F.col("a.artist_id"), "left")
    .join(dates, F.col("f.stream_date") == F.col("d.date_value"), "left")
    .select(
        "f.stream_id", "f.stream_timestamp", "f.stream_date", "f.ms_played", "f.device_type",
        "f.user_id", F.col("u.country").alias("user_country"),
        F.col("u.subscription_tier").alias("tier_at_stream"),
        "t.track_id", "t.track_name", "t.album_name",
        F.col("a.artist_id").alias("artist_id"), "a.artist_name", "a.genre",
        "d.weekday_name", "d.is_weekend",
    )
)

fact_stream_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.fact_stream_enriched")

# COMMAND ----------

enriched = spark.table(f"{catalog}.gold.fact_stream_enriched")

(
    enriched.groupBy("stream_date", "track_id", "track_name", "artist_name")
    .agg(F.count("*").alias("stream_count"),
         F.countDistinct("user_id").alias("unique_listeners"),
         F.round(F.sum("ms_played") / 60000, 2).alias("minutes_played"))
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.daily_track_streams")
)

(
    enriched.groupBy("artist_id", "artist_name", "genre")
    .agg(F.count("*").alias("stream_count"),
         F.countDistinct("user_id").alias("unique_listeners"))
    .withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("stream_count"))))
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.top_artists")
)

display(spark.table(f"{catalog}.gold.top_artists").orderBy("rank").limit(10))
