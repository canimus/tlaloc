import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.dataframe import DataFrame

METRICS = {
    # Time Based
    "first_event": F.min("date"),
    "last_event": F.max("date"),
    "tenure": F.count_distinct("date"),
    "events": F.count("*"),
    "age": F.date_diff("last_event", "first_event") + 1,
    "inactive_days": F.date_diff(F.current_date(), F.col("last_event")),
    "engagement": F.round(F.try_divide("tenure", "age"), 2),
}

def rank(dataframe: DataFrame, date: str = "date", metric: str = "value") -> DataFrame:
    """Given a time series with date column rank a dimension daily"""
    return (
        dataframe
        .withColumn("rank", F.dense_rank().over(W.partitionBy(date).sortBy(metric.desc())))
    )

def engagement(dataframe: DataFrame, date: str = "date", dimension: str = "user_id") -> DataFrame:
    """Returns the associated engagement metrics"""
    return (
        dataframe
        .groupby(dimension)
        .agg(
            *[
                v.alias(k) for k,v in METRICS.items()
            ]
        )
    )
    
def slope(dataframe: DataFrame, date: str = "date", dimension: str = "user_id", metric: str = "value") -> DataFrame:
    """Returns the slope and r2 of a metric"""
    time_partition = W.partitionBy(dimension).orderBy(date)
    full_partition = time_partition.rowsBetween(W.unboundedPreceding, W.unboundedFollowing)
    return (
        dataframe
        .withColumn("row_number", F.row_number().over(time_partition))
        .withColumn("slope", F.regr_slope(metric, "row_number").over(full_partition))
        .withColumn("r2", F.regr_r2(metric, "row_number").over(full_partition))
    )
