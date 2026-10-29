from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "labor_stats"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUARTERS = ("Q01", "Q02", "Q03", "Q04")


def silver(table_name):
    return spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")


def not_null_expectations(*columns):
    return {f"{column}_not_null": f"{column} IS NOT NULL" for column in columns}


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.population_statistics_2013_2018",
    comment="Average and population standard deviation for US population, 2013-2018.",
)
@dp.expect_all(not_null_expectations("label", "average_pop", "stddev_pop"))
def population_statistics_2013_2018():
    return (
        silver("population")
        .filter(F.col("year").between(2013, 2018))
        .agg(
            F.avg("population").alias("average_pop"),
            F.stddev_pop("population").alias("stddev_pop"),
        )
        .withColumn("label", F.lit("US population 2013-2018"))
        .select("label", "average_pop", "stddev_pop")
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.series_best_year",
    comment="Best year per BLS series using available quarterly values or an annual-only Q05 value.",
)
@dp.expect_all(
    not_null_expectations(
        "series_id",
        "series_label",
        "best_year",
        "value_type",
        "value",
    )
)
def series_best_year():
    rank = Window.partitionBy("series_id").orderBy(
        F.desc("value"),
        F.desc("year"),
    )

    observations = silver("pr_data_1_alldata")
    quarterly_series = (
        observations.where(F.col("period").isin(*QUARTERS))
        .select("series_id")
        .distinct()
    )

    quarterly_years = (
        observations
        .where(
            F.col("period").isin(*QUARTERS)
            & F.col("value").isNotNull()
        )
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("quarter_value_sum"))
        .select(
            "series_id",
            "year",
            F.lit("quarter_value_sum").alias("value_type"),
            F.col("quarter_value_sum").alias("value"),
        )
    )

    annual_only_years = (
        observations.where(
            (F.col("period") == "Q05")
            & F.col("value").isNotNull()
        )
        .join(quarterly_series, "series_id", "left_anti")
        .select(
            "series_id",
            "year",
            F.lit("annual_average").alias("value_type"),
            "value",
        )
    )

    best_year = (
        quarterly_years.unionByName(annual_only_years)
        .withColumn("rnk", F.row_number().over(rank))
        .where("rnk = 1")
    )

    labels = (
        silver("pr_series")
        .join(silver("pr_sector"), "sector_code", "left")
        .join(silver("pr_class"), "class_code", "left")
        .join(silver("pr_measure"), "measure_code", "left")
        .join(silver("pr_duration"), "duration_code", "left")
        .select(
            "series_id",
            F.concat_ws(
                " | ",
                "sector_name",
                "class_text",
                "measure_text",
                "duration_text",
            ).alias("series_label"),
        )
    )

    return (
        labels.join(best_year, "series_id", "inner")
        .select(
            "series_id",
            "series_label",
            F.col("year").alias("best_year"),
            "value_type",
            "value",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.prs30006032_q01_population",
    comment="Q01 observations for PRS30006032 joined to US population by year.",
)
@dp.expect_all(
    not_null_expectations(
        "value",
        "series_id",
        "period",
        "year",
    )
)
def prs30006032_q01_population():
    observations = silver("pr_data_1_alldata").alias("observations")
    population = silver("population").alias("population")

    return (
        observations.join(
            population,
            F.col("observations.year") == F.col("population.year"),
            "left",
        )
        .filter(
            (F.col("observations.period") == "Q01")
            & (F.col("observations.series_id") == "PRS30006032")
        )
        .select(
            F.col("observations.value").alias("value"),
            F.col("observations.series_id").alias("series_id"),
            F.col("observations.period").alias("period"),
            F.col("observations.year").alias("year"),
            F.col("population.population").alias("population"),
        )
    )
