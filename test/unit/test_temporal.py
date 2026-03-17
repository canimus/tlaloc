from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from tlaloc.temporal import anomaly as A


def test_ewma_anomalies(spark):
    """Injects a clear spike and asserts at least one flag is True"""
    raw = [(0, [10.0] * 49 + [999.0])]  # last point is a clear anomaly

    schema = T.StructType([
        T.StructField("group_id", T.IntegerType()),
        T.StructField("values", T.ArrayType(T.DoubleType()))
    ])

    df = spark.createDataFrame(raw, schema=schema)

    result = A.ewma(df, col="values", span=5, pct_tolerance=0.1, output_col="y_hat")
    result = result.withColumn("flag", F.explode("y_hat_flags"))

    anomaly_count = result.filter(F.col("flag") == True).count()

    assert anomaly_count > 0, "Expected at least one anomaly for spiked data"



def test_ewma_no_anomalies(spark):
    
    import math

    # Generate smooth sine wave data — EWMA tracks this well
    raw = [
        (group_id, [10.0 + math.sin(i * 0.1) for i in range(50)])
        for group_id in range(2)  # 2 groups x 50 points = 100 rows logically
    ]

    schema = T.StructType([
        T.StructField("group_id", T.IntegerType()),
        T.StructField("values", T.ArrayType(T.DoubleType()))
    ])

    df = spark.createDataFrame(raw, schema=schema)

    # Run EWMA with a wide span and generous tolerance
    result = A.ewma(
        dataframe=df,
        col="values",
        span=5,
        pct_tolerance=0.1,
        output_col="y_hat"
    )

    # Explode flags to assert point by point
    result = result.withColumn("flag", F.explode("y_hat_flags"))

    anomaly_count = result.filter(F.col("flag") == True).count()

    assert anomaly_count == 0, (
        f"Expected 0 anomalies on smooth data, but found {anomaly_count}"
    )

