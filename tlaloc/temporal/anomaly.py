import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame, Column
import pyspark.sql.types as T

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

def ewma(
    dataframe: DataFrame,
    col: str,
    span: int,
    pct_tolerance: float = 0.05,
    output_col: str = "y_hat"
) -> DataFrame:
    """
    Computes EWMA over an array column and flags anomalies where the actual
    value deviates from the EWMA prediction by more than pct_tolerance (e.g. 0.1 = 10%).
    
    Expects `col` to be an ArrayType(DoubleType()) column.
    Returns the original dataframe with added columns:
        - `output_col`         : array of ewma predicted values
        - `{output_col}_flags` : array of booleans (True = anomalous point)
        - `{output_col}_score` : array of relative deviations per point
    """
    alpha = 2 / (span + 1)

    # Step 1: compute EWMA array
    df = dataframe.withColumn(
        output_col,
        F.aggregate(
            F.col(col),
            F.struct(
                F.array().cast(T.ArrayType(T.DoubleType())).alias("ewma_list"),
                F.lit(None).cast(T.DoubleType()).alias("last_ewma")
            ),
            lambda acc, y_t: F.struct(
                F.concat(
                    acc["ewma_list"],
                    F.array(
                        F.when(acc["last_ewma"].isNull(), y_t.cast(T.DoubleType()))
                         .otherwise(y_t * alpha + acc["last_ewma"] * (1 - alpha))
                    )
                ).alias("ewma_list"),
                F.when(acc["last_ewma"].isNull(), y_t.cast(T.DoubleType()))
                 .otherwise(y_t * alpha + acc["last_ewma"] * (1 - alpha)).alias("last_ewma")
            ),
            lambda acc: acc["ewma_list"]
        )
    )

    # Step 2: compute per-point relative deviation score: |actual - predicted| / |predicted|
    df = df.withColumn(
        f"{output_col}_score",
        F.zip_with(
            F.col(col).cast(T.ArrayType(T.DoubleType())),
            F.col(output_col),
            lambda actual, predicted: F.try_divide(F.abs(actual - predicted), F.abs(predicted))
        )
    )

    # Step 3: flag anomalies — null scores (zero prediction) are treated as False
    df = df.withColumn(
        f"{output_col}_flags",
        F.transform(
            F.col(f"{output_col}_score"),
            lambda score: F.coalesce(score > F.lit(pct_tolerance), F.lit(False))
        )
    )

    return df
