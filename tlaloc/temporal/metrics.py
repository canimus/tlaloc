import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.dataframe import DataFrame, Column
from functools import reduce
from operator import and_
from typing import List


def rank(dataframe: DataFrame, date: str = "date", metric: str = "value") -> DataFrame:
    """Given a time series with date column rank a dimension daily"""
    return (
        dataframe
        .withColumn("rank", F.dense_rank().over(W.partitionBy(date).sortBy(metric.desc())))
    )

# FN: user_engagement
def user_engagement(
    dataframe: DataFrame,
    pk: List[str | Column] = ["organization_id", "user_id"],
    date: str | Column = "date",
) -> DataFrame:
    """Provide labels from L1-L5 for user engagement"""

    # RULE: staleness [30..100]
    STALENESS_THRESHOLDS = {
        "L5_active": 30,
        "L4_at_risk": 40,
        "L3_disengaged": 50,
        "L2_leaving": 90,
        "L1_churn": 100,
    }

    # RULE: tenure [30..366]
    TENURE_THRESHOLDS = {
        "L1_new_1_month": 30,
        "L2_growing_3_months": 90,
        "L3_seasoned_6_months": 180,
        "L4_mature_12_months": 365,
        "L5_veteran_1_year_plus": 366,
    }

    # RULE: engagement [0..1]
    ENGAGEMENT_THRESHOLDS = {
        "L0_tried": 7,  # user_tenure < 7d
        "L1_rare": 0.3,  # user_engagement
        "L2_occasional": 0.5,
        "L3_regular": 0.6,
        "L4_highly_active": 0.7,
        "L5_power_user": 0.8,
    }

    # RULE: consistency [0..1]
    CONSISTENCY_THRESHOLDS = {
        "L1_sporadic": 0.1,
        "L2_inconsistent": 0.5,
        "L3_periodic": 0.8,
        "L4_consistent": 0.9,
        "L5_established": 0.95,
    }

    # Define window specification once
    org_events_window = W.partitionBy("organization_id").orderBy(F.col("events").desc())
    org_tenure_window = W.partitionBy("organization_id").orderBy(
        F.col("user_tenure").desc()
    )

    return (
        dataframe.filter(reduce(and_, [F.isnotnull(c) for c in pk + [date]]))
        .withColumn("month", F.date_trunc("month", date).cast("date"))
        .groupby(pk)
        .agg(
            # AGG: first_login: date
            F.min(date).alias("first_login"),
            # AGG: last_login: date
            F.max(date).alias("last_login"),
            # AGG: user_tenure: int <days>
            F.count_distinct(date).alias("user_tenure"),
            # AGG: active_months: set<date>
            F.array_sort(F.collect_set("month")).alias("active_months"),
            # AGG: events: int[rows]
            F.count("*").alias("events"),
        )
        # METRIC: user_months: int [months]
        .withColumn("user_months", F.size("active_months"))
        # METRIC: month_age: int [months]
        .withColumn(
            "month_age",
            F.months_between(
                F.date_trunc("month", "last_login"),
                F.date_trunc("month", "first_login"),
            )
            + 1,
        )
        # METRIC: user_consistency: float [0..1]
        .withColumn(
            "user_consistency",
            F.zeroifnull(F.try_divide(F.col("user_months"), F.col("month_age"))),
        )
        # METRIC: user_age: int [days]
        .withColumn(
            "user_age",
            F.greatest(
                F.datediff(F.col("last_login"), F.col("first_login")),
                F.col("user_tenure"),
            ),
        )
        # METRIC: user_engagement: float [0..1]
        .withColumn("user_engagement", F.try_divide("user_tenure", "user_age"))
        # METRIC: user_staleness: int [days]
        .withColumn(
            "user_staleness", F.date_diff(F.current_date(), F.col("last_login"))
        )
        # Ranking metrics
        # METRIC: user_quintile: int [1..5]
        .withColumn("user_quintile", F.ntile(5).over(org_events_window))
        # METRIC: event_rank: int [1..n]
        .withColumn("event_rank", F.dense_rank().over(org_events_window))
        # METRIC: tenure_rank: int [1..n]
        .withColumn("tenure_rank", F.dense_rank().over(org_tenure_window))
        # METRIC: is_champion: bool
        .withColumn(
            "is_champion", (F.col("event_rank") == 1) & (F.col("tenure_rank") == 1)
        )
        # LABEL: user_state_cohort: str [active,at_risk,disengaged,leaving,churned]
        .withColumn(
            "user_state_cohort",
            F.when(
                F.col("user_staleness") <= STALENESS_THRESHOLDS["L5_active"],
                "L5_active",
            )
            .when(
                F.col("user_staleness") <= STALENESS_THRESHOLDS["L4_at_risk"],
                "L4_at_risk",
            )
            .when(
                F.col("user_staleness") <= STALENESS_THRESHOLDS["L3_disengaged"],
                "L3_disengaged",
            )
            .when(
                F.col("user_staleness") <= STALENESS_THRESHOLDS["L2_leaving"],
                "L2_leaving",
            )
            .otherwise("L1_churned"),
        )
        # LABEL: user_tenure_cohort: str [new,growing,seasoned,mature,veteran]
        .withColumn(
            "user_tenure_cohort",
            F.when(
                F.col("user_tenure") <= TENURE_THRESHOLDS["L1_new_1_month"],
                "L1_new_1_month",
            )
            .when(
                F.col("user_tenure") <= TENURE_THRESHOLDS["L2_growing_3_months"],
                "L2_growing_3_months",
            )
            .when(
                F.col("user_tenure") <= TENURE_THRESHOLDS["L3_seasoned_6_months"],
                "L3_seasoned_6_months",
            )
            .when(
                F.col("user_tenure") <= TENURE_THRESHOLDS["L4_mature_12_months"],
                "L4_mature_12_months",
            )
            .otherwise("L5_veteran_1_year_plus"),
        )
        # LABEL: user_engagement_cohort: str [tried,rare,occasional,regular,highly_active,power_user]
        .withColumn(
            "user_engagement_cohort",
            F.when(
                F.col("user_tenure") <= ENGAGEMENT_THRESHOLDS["L0_tried"], "L0_tried"
            )
            .when(
                F.col("user_engagement") <= ENGAGEMENT_THRESHOLDS["L1_rare"], "L1_rare"
            )
            .when(
                F.col("user_engagement") <= ENGAGEMENT_THRESHOLDS["L2_occasional"],
                "L2_occasional",
            )
            .when(
                F.col("user_engagement") <= ENGAGEMENT_THRESHOLDS["L3_regular"],
                "L3_regular",
            )
            .when(
                F.col("user_engagement") <= ENGAGEMENT_THRESHOLDS["L4_highly_active"],
                "L4_highly_active",
            )
            .otherwise("L5_power_user"),
        )
        # LABEL: user_consistency_cohort: str [sporadic,inconsistent,periodic,consistent,established]
        .withColumn(
            "user_consistency_cohort",
            F.when(
                F.col("user_consistency") <= CONSISTENCY_THRESHOLDS["L1_sporadic"],
                "L1_sporadic",
            )
            .when(
                F.col("user_consistency") <= CONSISTENCY_THRESHOLDS["L2_inconsistent"],
                "L2_inconsistent",
            )
            .when(
                F.col("user_consistency") <= CONSISTENCY_THRESHOLDS["L3_periodic"],
                "L3_periodic",
            )
            .when(
                F.col("user_consistency") <= CONSISTENCY_THRESHOLDS["L4_consistent"],
                "L4_consistent",
            )
            .otherwise("L5_established"),
        )
        # Cohort labels - rank
        # LABEL: user_rank_cohort: str [champion,contender,mid_rank,low_rank,bottom_rank]
        .withColumn(
            "user_rank_cohort",
            F.when(F.col("user_quintile") == 1, "L5_top")
            .when(F.col("user_quintile") == 2, "L4_high")
            .when(F.col("user_quintile") == 3, "L3_mid")
            .when(F.col("user_quintile") == 4, "L2_lower")
            .otherwise("L1_bottom"),
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
