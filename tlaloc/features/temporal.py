import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from typing import Union,List, Callable
from operator import methodcaller as mc


def week_of_year(dataframe: DataFrame, date: str = "date", week: str = "week") -> DataFrame:
    """Returns a datataframe with the ISO week of year"""
 
    return (
        dataframe
        .withColumn(week, F.concat(
                # If week number is 1 but we're in December, it's actually next year
                F.when(
                    (F.weekofyear(F.col(date)) == 1) & (F.month(F.col(date)) == 12),
                    F.year(F.col(date)) + 1
                )
                # If week number is 52/53 but we're in January, it's actually last year
                .when(
                    (F.weekofyear(F.col(date)) >= 52) & (F.month(F.col(date)) == 1),
                    F.year(F.col(date)) - 1
                )
                .otherwise(F.year(F.col(date)))
                .cast("string"),
                F.lit("-W"),
                F.lpad(F.weekofyear(F.col(date)).cast("string"), 2, "0")
            )
        )
    )


def weeklyfy(dataframe: DataFrame, pk: Union[None|str|List[str]] = None, agg: Union[Column, Callable, List[Callable]] = [F.count("date")], fill: bool = True) -> DataFrame:
    """Pivots weeks and counters for key"""
    pivot = (
        dataframe
        .transform(week_of_year)
        .groupby(pk)
        .pivot("week")
    )
    if not isinstance(agg, (tuple,list)):
        agg = [agg]

    pivot = mc("agg", *agg)(pivot)

    # Impute strategy
    if fill:
        pivot = pivot.fillna(0)

    return pivot