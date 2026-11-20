from pyspark.sql import functions as F


def build_gold(spark):
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

    pickup_metrics = df_silver_jan.groupBy("PULocationID").agg(
        F.count("*").alias("pickup_trips"),
        F.round(F.sum("total_amount"), 2).alias("pickup_amount_sum")
    ).withColumnRenamed(
        "PULocationID", "location_id"
    )

    dropoff_metrics = df_silver_jan.groupBy("DOLocationID").agg(
        F.count("*").alias("dropoff_trips"),
    ).withColumnRenamed(
        "DOLocationID", "location_id"
    )

    location_metrics = pickup_metrics.join(
        dropoff_metrics,
        on="location_id",
        how="fullouter"
    )

    location_metrics = location_metrics.fillna({
        "pickup_trips": 0,
        "pickup_amount_sum": 0.0,
        "dropoff_trips": 0
    })

    location_metrics.write \
        .mode("overwrite") \
        .parquet("data/gold/location_analytics")

    print("Location metrics successfully generated.")

    total_jan_trips = df_silver_jan.count()

    payment_analytics = df_silver_jan.groupBy("payment_type").agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_amount_sum"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount")
    ).withColumn(
        "payment_percentage", F.round(100 * F.col("total_trips") / F.lit(total_jan_trips), 2)
    )

    payment_analytics.write.mode("overwrite").parquet("data/gold/payment_analytics")

    print("Payment metrics successfully generated.")