from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NYCTaxiAnalytics") \
    .master("local[*]") \
    .getOrCreate()

df_taxi = spark.read.parquet(
    "data/bronze/yellow_tripdata_2025-01.parquet"
)

df_silver = df_taxi.withColumns({
    "is_valid_duration":
        F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"),

    "is_valid_distance":
        F.col("trip_distance") > 0,

    "has_missing_passengers":
        F.col("passenger_count").isNull(),

    "has_non_positive_passengers":
        F.coalesce(F.col("passenger_count") <= 0, F.lit(False)),

    "is_disputed":
        F.col("payment_type") == 4,

    "has_negative_fare":
        F.col("fare_amount") < 0
})

df_silver.write \
    .mode("overwrite") \
    .parquet("data/silver/nyc_taxi")

spark.stop()