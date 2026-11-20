from pyspark.sql import functions as F

def test_number_of_days(spark):
    daily_metrics = spark.read.parquet("data/gold/daily_metrics")

    assert daily_metrics.count() == 31


def test_presence_of_pickup_date(spark):
    daily_metrics = spark.read.parquet("data/gold/daily_metrics")

    assert "pickup_date" in daily_metrics.columns


def test_total_trips(spark):
    silver_jan = spark.read.parquet("data/silver/nyc_taxi").filter(
        (F.col("tpep_pickup_datetime") >= "2025-01-01") &
        (F.col("tpep_pickup_datetime") < "2025-02-01")
    )

    daily_metrics = spark.read.parquet("data/gold/daily_metrics")

    silver_jan_trips = silver_jan.count()
    daily_metrics_trips = daily_metrics.agg(
        F.sum("total_trips")
    ).collect()[0][0]

    assert daily_metrics_trips == silver_jan_trips

def test_no_duplicate_date(spark):
    daily_metrics = spark.read.parquet("data/gold/daily_metrics")

    total_rows = daily_metrics.count()
    unique_dates = daily_metrics.select("pickup_date").distinct().count()

    assert total_rows == unique_dates
