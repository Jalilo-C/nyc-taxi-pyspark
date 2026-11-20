from pyspark.sql import functions as F

def test_number_of_trips(spark):
    location_analytics = spark.read.parquet("data/gold/location_analytics")

    silver_jan = spark.read.parquet("data/silver/nyc_taxi").filter(
        (F.col("tpep_pickup_datetime") >= "2025-01-01") &
        (F.col("tpep_pickup_datetime") < "2025-02-01")
    )

    silver_jan_trips = silver_jan.count()
    pickup_trips = location_analytics.agg(F.sum("pickup_trips")).first()[0]
    dropoff_trips = location_analytics.agg(
        F.sum("dropoff_trips")
    ).first()[0]

    assert pickup_trips == silver_jan_trips
    assert dropoff_trips == silver_jan_trips