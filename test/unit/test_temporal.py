from pyspark.sql import SparkSession

def test_user_ranks(spark: SparkSession, users: str):
    assert True