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

def ewma(dataframe: DataFrame, col: str, span: int, pct_tolerance: float = 0.1, output_col: str = "y_hat") -> DataFrame:
    """Uses exponential weighted moving average and 10% default tolerance as detector"""
    
    alpha = 2 / (span + 1) # 0.4

    return dataframe.withColumn(
    "ewma_array",
    F.aggregate(
        F.col(col),         
        F.struct(            
            F.array_repeat(F.col(col)[0].cast(T.DoubleType()), 1).alias("ewma_list"),
            F.col(col)[0].cast(T.DoubleType()).alias("last_ewma")                     
        ),        
        lambda acc, y_t: F.struct(
            F.concat(
                acc["ewma_list"],
                F.array(
                    y_t * alpha + acc["last_ewma"] * (1 - alpha)
                )
            ),
            y_t * alpha + acc["last_ewma"] * (1 - alpha)
        ),
        lambda acc: acc["ewma_list"]
    )
)