from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NYCTaxiAnalytics") \
    .master("local[*]") \
    .getOrCreate()

df_silver = spark.read.parquet("data/silver/nyc_taxi")

df_silver = df_silver.withColumn(
    "pickup_date", F.to_date("tpep_pickup_datetime")
)

df_silver_jan = df_silver.filter(
    (F.col("tpep_pickup_datetime") >= "2025-01-01") &
    (F.col("tpep_pickup_datetime") < "2025-02-01")
)

daily_metrics = df_silver_jan.groupBy("pickup_date").agg(
    F.count("*").alias("total_trips"),
    F.round(F.sum("total_amount"), 2).alias("total_amount_sum"),
    F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
    F.round(
        F.avg(
            F.when(F.col("is_valid_distance"), F.col("trip_distance"))
        ), 2
    ).alias("avg_trip_distance"),
    F.count(
        F.when(F.col("is_valid_duration"), 1)
    ).alias("valid_trips")
)

daily_metrics.write \
    .mode("overwrite") \
    .parquet("data/gold/daily_metrics")

print("Daily metrics successfully generated.")

spark.stop()