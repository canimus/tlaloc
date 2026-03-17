from functools import reduce
from operator import and_
from typing import List

from pyspark.ml import Estimator, Model
from pyspark.ml.param.shared import HasInputCols, HasOutputCol, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param import Param, TypeConverters
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W


class HasPrimaryKey(Params):
    pk = Param(
        Params._dummy(),
        "pk",
        "Primary key columns",
        typeConverter=TypeConverters.toListString,
    )

    def getPk(self):
        return self.getOrDefault(self.pk)


class HasDateCol(Params):
    dateCol = Param(
        Params._dummy(),
        "dateCol",
        "Date column name",
        typeConverter=TypeConverters.toString,
    )

    def getDateCol(self):
        return self.getOrDefault(self.dateCol)


class UserEngagementEstimator(
    Estimator,
    HasPrimaryKey,
    HasDateCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    def __init__(
        self,
        pk: List[str] = ["organization_id", "user_id"],
        dateCol: str = "date",
    ):
        super().__init__()
        self._set(pk=pk, dateCol=dateCol)

    def _fit(self, dataframe: DataFrame) -> "UserEngagementModel":
        pk = self.getPk()
        date = self.getDateCol()

        # Compute aggregations and ranking — these are data-dependent
        org_events_window = W.partitionBy("organization_id").orderBy(F.col("events").desc())
        org_tenure_window = W.partitionBy("organization_id").orderBy(F.col("user_tenure").desc())

        aggregated = (
            dataframe
            .filter(reduce(and_, [F.isnotnull(c) for c in pk + [date]]))
            .withColumn("month", F.date_trunc("month", date).cast("date"))
            .groupby(pk)
            .agg(
                F.min(date).alias("first_login"),
                F.max(date).alias("last_login"),
                F.count_distinct(date).alias("user_tenure"),
                F.array_sort(F.collect_set("month")).alias("active_months"),
                F.count("*").alias("events"),
            )
            .withColumn("user_months", F.size("active_months"))
            .withColumn(
                "month_age",
                F.months_between(
                    F.date_trunc("month", "last_login"),
                    F.date_trunc("month", "first_login"),
                ) + 1,
            )
            .withColumn(
                "user_consistency",
                F.zeroifnull(F.try_divide(F.col("user_months"), F.col("month_age"))),
            )
            .withColumn(
                "user_age",
                F.greatest(
                    F.datediff(F.col("last_login"), F.col("first_login")),
                    F.col("user_tenure"),
                ),
            )
            .withColumn("user_engagement", F.try_divide("user_tenure", "user_age"))
            .withColumn("user_staleness", F.date_diff(F.current_date(), F.col("last_login")))
            # Ranking — data dependent, computed once during fit
            .withColumn("user_quintile", F.ntile(5).over(org_events_window))
            .withColumn("event_rank", F.dense_rank().over(org_events_window))
            .withColumn("tenure_rank", F.dense_rank().over(org_tenure_window))
            .withColumn(
                "is_champion",
                (F.col("event_rank") == 1) & (F.col("tenure_rank") == 1),
            )
        )

        return UserEngagementModel(
            pk=pk,
            dateCol=date,
            precomputedDF=aggregated,
        )


class UserEngagementModel(
    Model,
    HasPrimaryKey,
    HasDateCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    # Thresholds as class-level constants
    STALENESS_THRESHOLDS = {
        "L5_active": 30,
        "L4_at_risk": 40,
        "L3_disengaged": 50,
        "L2_leaving": 90,
        "L1_churn": 100,
    }
    TENURE_THRESHOLDS = {
        "L1_new_1_month": 30,
        "L2_growing_3_months": 90,
        "L3_seasoned_6_months": 180,
        "L4_mature_12_months": 365,
        "L5_veteran_1_year_plus": 366,
    }
    ENGAGEMENT_THRESHOLDS = {
        "L0_tried": 7,
        "L1_rare": 0.3,
        "L2_occasional": 0.5,
        "L3_regular": 0.6,
        "L4_highly_active": 0.7,
        "L5_power_user": 0.8,
    }
    CONSISTENCY_THRESHOLDS = {
        "L1_sporadic": 0.1,
        "L2_inconsistent": 0.5,
        "L3_periodic": 0.8,
        "L4_consistent": 0.9,
        "L5_established": 0.95,
    }

    def __init__(self, pk: List[str], dateCol: str, precomputedDF: DataFrame):
        super().__init__()
        self._set(pk=pk, dateCol=dateCol)
        self._precomputedDF = precomputedDF  # carries fitted rankings

    def _transform(self, _: DataFrame) -> DataFrame:
        """
        Joins incoming dataframe against the precomputed aggregations
        and applies all cohort labels deterministically.
        """
        S = self.STALENESS_THRESHOLDS
        T = self.TENURE_THRESHOLDS
        E = self.ENGAGEMENT_THRESHOLDS
        C = self.CONSISTENCY_THRESHOLDS

        pk = self.getPk()


        return (
            self._precomputedDF
            # LABEL: user_state_cohort
            .withColumn(
                "user_state_cohort",
                F.when(F.col("user_staleness") <= S["L5_active"], "L5_active")
                 .when(F.col("user_staleness") <= S["L4_at_risk"], "L4_at_risk")
                 .when(F.col("user_staleness") <= S["L3_disengaged"], "L3_disengaged")
                 .when(F.col("user_staleness") <= S["L2_leaving"], "L2_leaving")
                 .otherwise("L1_churned"),
            )
            # LABEL: user_tenure_cohort
            .withColumn(
                "user_tenure_cohort",
                F.when(F.col("user_tenure") <= T["L1_new_1_month"], "L1_new_1_month")
                 .when(F.col("user_tenure") <= T["L2_growing_3_months"], "L2_growing_3_months")
                 .when(F.col("user_tenure") <= T["L3_seasoned_6_months"], "L3_seasoned_6_months")
                 .when(F.col("user_tenure") <= T["L4_mature_12_months"], "L4_mature_12_months")
                 .otherwise("L5_veteran_1_year_plus"),
            )
            # LABEL: user_engagement_cohort
            .withColumn(
                "user_engagement_cohort",
                F.when(F.col("user_tenure") <= E["L0_tried"], "L0_tried")
                 .when(F.col("user_engagement") <= E["L1_rare"], "L1_rare")
                 .when(F.col("user_engagement") <= E["L2_occasional"], "L2_occasional")
                 .when(F.col("user_engagement") <= E["L3_regular"], "L3_regular")
                 .when(F.col("user_engagement") <= E["L4_highly_active"], "L4_highly_active")
                 .otherwise("L5_power_user"),
            )
            # LABEL: user_consistency_cohort
            .withColumn(
                "user_consistency_cohort",
                F.when(F.col("user_consistency") <= C["L1_sporadic"], "L1_sporadic")
                 .when(F.col("user_consistency") <= C["L2_inconsistent"], "L2_inconsistent")
                 .when(F.col("user_consistency") <= C["L3_periodic"], "L3_periodic")
                 .when(F.col("user_consistency") <= C["L4_consistent"], "L4_consistent")
                 .otherwise("L5_established"),
            )
            # LABEL: user_rank_cohort
            .withColumn(
                "user_rank_cohort",
                F.when(F.col("user_quintile") == 1, "L5_top")
                 .when(F.col("user_quintile") == 2, "L4_high")
                 .when(F.col("user_quintile") == 3, "L3_mid")
                 .when(F.col("user_quintile") == 4, "L2_lower")
                 .otherwise("L1_bottom"),
            )
        )