import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame, Column

def threshold(dataframe: DataFrame, col: str, low: float, high: float, output_column: str = "y_hat") -> DataFrame:
    """Given a high and low threshold detects anomalies in a DataFrame"""
    predicate = ~F.col(col).between(low, high)
    return dataframe.withColumn(output_column, predicate)


def quantile(dataframe: DataFrame, col: str, low: float, high: float, output_column: str = "y_hat", precision=0.01) -> DataFrame:
    """Given a high and low quantile detects anomalies in a DataFrame"""
    return (
        dataframe
        .select(
            col, 
            F.lit(dataframe.select(col).approxQuantile(col, [low, high], precision)).alias("quantiles")
        )
        .select(
            F.col(col),
            (~F.col(col).between(F.col("quantiles").getItem(0), F.col("quantiles").getItem(1))).alias(output_column)
        )
    )
