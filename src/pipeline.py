from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NYCTaxiAnalytics") \
    .master("local[*]") \
    .getOrCreate()

df_taxi = spark.read.parquet("data/bronze/yellow_tripdata_2025-01.parquet")

df_taxi.printSchema()

df_taxi.show(10, truncate=False)

nb_trips = df_taxi.count()

print(f"Total trips : {nb_trips}")

spark.stop()