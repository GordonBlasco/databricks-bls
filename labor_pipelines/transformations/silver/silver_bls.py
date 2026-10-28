from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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
            df = df.withColumn(
                column_name,
                F.expr(
                    f"try_cast(trim(cast({quoted_name} AS STRING)) AS {target_type})"
                ),
            )
        elif isinstance(field.dataType, StringType):
            df = df.withColumn(column_name, F.trim(F.col(quoted_name)))
    return df


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
    def build(t=table_name):
        return apply_silver_types(latest_file_rows(t))
