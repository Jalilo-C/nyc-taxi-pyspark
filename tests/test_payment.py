from pyspark.sql import functions as F
import pytest

def test_number_of_trips(spark):
    payment_analytics = spark.read.parquet("data/gold/payment_analytics")

    silver_jan = spark.read.parquet("data/silver/nyc_taxi").filter(
        (F.col("tpep_pickup_datetime") >= "2025-01-01") &
        (F.col("tpep_pickup_datetime") < "2025-02-01")
    )

    silver_jan_trips = silver_jan.count()
    payment_trips = payment_analytics.agg(F.sum("total_trips")).first()[0]
    
    assert payment_trips == silver_jan_trips

def test_percentage(spark):
    payment_analytics = spark.read.parquet("data/gold/payment_analytics")

    total_percentage = payment_analytics.agg(
        F.sum("payment_percentage")
    ).first()[0]

    assert total_percentage == pytest.approx(100, abs=0.02)

    