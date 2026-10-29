from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

BRONZE_CATALOG = "labor_stats"
BRONZE_SCHEMA = "bronze"
SILVER_CATALOG = "labor_stats"
SILVER_SCHEMA = "silver"

# Volume paths for source file discovery (must match bronze_bls.py)
CATALOG = "labor_stats"
RAW_SCHEMA = "raw"
LANDING_VOLUME = "landing_zone"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}/raw"
BLS_TIMESERIES = "bls/pr/"
POPULATION_SOURCE_FILE = "population.json"
NON_TABULAR_FILES = {"pr.txt", "pr.contacts"}

POPULATION_DATA_SCHEMA = ArrayType(
    StructType(
        [
            StructField("Nation ID", StringType()),
            StructField("Nation", StringType()),
            StructField("Year", StringType()),
            StructField("Population", StringType()),
        ]
    )
)
POPULATION_COLUMNS = {
    "Nation ID": "nation_id",
    "Nation": "nation",
    "Year": "year",
    "Population": "population",
}
SOURCE_METADATA_COLUMNS = [
    "_volume_path",
    "_content_hash",
    "_file_modified_at",
    "_ingested_at",
]

# Bronze intentionally preserves CSV and JSON values as strings. Silver casts
# fields with stable business meanings while keeping identifiers as strings.
SILVER_COLUMN_TYPES = {
    "year": "INT",
    "id year": "INT",
    "begin_year": "INT",
    "end_year": "INT",
    "base_year": "INT",
    "display_level": "INT",
    "sort_sequence": "INT",
    "selectable": "BOOLEAN",
    "value": "DECIMAL(38, 10)",
    "population": "BIGINT",
}

# Typed silver columns consumed by gold. apply_silver_types uses try_cast so bad
# source values become NULL; silver expectations fail when any of those NULLs
# reach a published silver table.
GOLD_CAST_COLUMNS = {
    "pr.data.1.AllData": ("year", "value"),
    POPULATION_SOURCE_FILE: ("year", "population"),
}

# Business keys that must be present on every Silver row.
KEY_COLUMNS = {
    "pr.class": ("class_code",),
    "pr.data.0.Current": ("series_id", "year", "period"),
    "pr.data.1.AllData": ("series_id", "year", "period"),
    "pr.duration": ("duration_code",),
    "pr.footnote": ("footnote_code",),
    "pr.measure": ("measure_code",),
    "pr.period": ("period",),
    "pr.seasonal": ("seasonal_code",),
    "pr.sector": ("sector_code",),
    "pr.series": ("series_id",),
    POPULATION_SOURCE_FILE: ("nation_id", "year"),
}


def quote_identifier(name: str) -> str:
    """Quote a Spark column name, including names such as 'ID Year'."""
    return f"`{name.replace('`', '``')}`"


def apply_silver_types(df):
    """Trim strings and apply known types without guessing identifier types."""
    for field in df.schema.fields:
        column_name = field.name
        target_type = SILVER_COLUMN_TYPES.get(column_name.casefold())
        quoted_name = quote_identifier(column_name)
        if target_type:
            source_expression = f"trim(cast({quoted_name} AS STRING))"
            if column_name.casefold() == "population":
                source_expression = f"try_cast({source_expression} AS DOUBLE)"
            df = df.withColumn(
                column_name,
                F.expr(f"try_cast({source_expression} AS {target_type})"),
            )
        elif isinstance(field.dataType, StringType):
            df = df.withColumn(column_name, F.trim(F.col(quoted_name)))
    return df


def parse_population_data(df):
    """Explode the Data USA `data` JSON array into one row per year."""
    records = df.select(
        F.explode(F.from_json(F.col("data"), POPULATION_DATA_SCHEMA)).alias("record"),
        *SOURCE_METADATA_COLUMNS,
    )
    return records.select(
        *[
            F.col("record").getField(source_name).alias(target_name)
            for source_name, target_name in POPULATION_COLUMNS.items()
        ],
        *SOURCE_METADATA_COLUMNS,
    )


def latest_file_rows(table_name: str):
    """Keep only rows from the file(s) with the most recent _file_modified_at."""
    src = spark.read.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    latest = src.agg(F.max("_file_modified_at").alias("_latest_file_modified_at"))
    return (
        src.join(latest, src["_file_modified_at"] == latest["_latest_file_modified_at"])
        .drop("_latest_file_modified_at")
    )


def discover_source_files():
    """Discover source file directories in the UC volume."""
    source_dir = f"{VOLUME_ROOT}/{BLS_TIMESERIES}"
    items = dbutils.fs.ls(source_dir)
    bls_files = sorted(
        item.name.rstrip("/")
        for item in items
        if item.name.endswith("/") and item.name.rstrip("/") not in NON_TABULAR_FILES
    )
    return [*bls_files, POPULATION_SOURCE_FILE]


def bronze_table_name(source_file):
    """pr.data.0.Current -> pr_data_0_current"""
    return source_file.removesuffix(".json").replace(".", "_").lower()


def silver_row_expectations(source_file):
    expectations = {
        "ingestion_timestamp_present": "_ingested_at IS NOT NULL",
    }
    expectations.update(
        {
            f"{column}_key_not_null": f"{quote_identifier(column)} IS NOT NULL"
            for column in KEY_COLUMNS.get(source_file, ())
        }
    )
    expectations.update(
        {
            f"{column}_cast_valid": f"{quote_identifier(column)} IS NOT NULL"
            for column in GOLD_CAST_COLUMNS.get(source_file, ())
        }
    )
    return expectations


# One silver materialized view per discovered source file, keeping only rows
# from the latest file version in the corresponding bronze streaming table.
for source_file in discover_source_files():
    table_name = bronze_table_name(source_file)

    @dp.materialized_view(
        name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{table_name}",
        comment=(
            f"Silver layer: only rows from the most recently modified file(s) "
            f"in {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}."
        ),
    )
    @dp.expect_all_or_fail(silver_row_expectations(source_file))
    def build(t=table_name, sf=source_file):
        latest = latest_file_rows(t)
        if sf == POPULATION_SOURCE_FILE:
            latest = parse_population_data(latest)
        return apply_silver_types(latest)
