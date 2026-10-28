from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_CATALOG = "labor_stats"
BRONZE_SCHEMA = "bls_bronze"
SILVER_CATALOG = "labor_stats"
SILVER_SCHEMA = "bls_silver"

# Volume paths for source file discovery (must match bronze_bls.py)
CATALOG = "labor_stats"
SCHEMA = "bls"
VOLUME = "datausa"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw"
BLS_TIMESERIES = "bls/pr/"
NON_TABULAR_FILES = {"pr.txt", "pr.contacts"}


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
    return sorted(
        item.name.rstrip("/")
        for item in items
        if item.name.endswith("/") and item.name.rstrip("/") not in NON_TABULAR_FILES
    )


def bronze_table_name(source_file):
    """pr.data.0.Current -> pr_data_0_current"""
    return source_file.replace(".", "_").lower()


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
        return latest_file_rows(t)
