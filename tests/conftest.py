import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder \
        .appName("NYCTaxiTests") \
        .master("local[*]") \
        .getOrCreate()
    
    yield session

    session.stop()
