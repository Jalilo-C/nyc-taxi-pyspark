from pyspark.sql import SparkSession
from src.pipeline import build_silver
from src.gold import build_gold


def main():

    spark = SparkSession.builder \
        .appName("NYCTaxiAnalytics") \
        .master("local[*]") \
        .getOrCreate()

    try:
        build_silver(spark)
        build_gold(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()