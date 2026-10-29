from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "labor_stats"
RAW_SCHEMA = "raw"
LANDING_VOLUME = "landing_zone"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}/raw"
BLS_TIMESERIES = "bls/pr/"
DATAUSA_SOURCE = "datausa"
POPULATION_SOURCE_FILE = "population.json"

BRONZE_CATALOG = "labor_stats"
BRONZE_SCHEMA = "bronze"

# documentation, not tab-delimited tables
NON_TABULAR_FILES = {"pr.txt", "pr.contacts"}


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


def source_config(source_file):
    """Return the Auto Loader path and options for a discovered source."""
    if source_file == POPULATION_SOURCE_FILE:
        return (
            f"{VOLUME_ROOT}/{DATAUSA_SOURCE}/{source_file}/",
            {"cloudFiles.format": "json", "multiLine": "true"},
        )

    return (
        f"{VOLUME_ROOT}/{BLS_TIMESERIES}{source_file}/",
        {"cloudFiles.format": "csv", "sep": "\t", "header": "true"},
    )


# One bronze streaming table per source file. Auto Loader picks up new
# content-addressed .txt and .json versions incrementally; SDP manages
# checkpoints and schema location automatically.
for source_file in discover_source_files():
    table_name = bronze_table_name(source_file)

    @dp.table(
        name=f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}",
        comment=f"Bronze layer: raw ingestion of {source_file} via Auto Loader.",
    )
    def bronze(sf=source_file):
        source_path, options = source_config(sf)
        reader = spark.readStream.format("cloudFiles")
        for key, value in options.items():
            reader = reader.option(key, value)
        raw = reader.load(source_path)

        # BLS pads header names with whitespace ("value   "); data values are left as-is.
        # Bronze preserves the raw API/CSV shape; parsing and typing happen in silver.
        data_columns = [column for column in raw.columns if column != "_metadata"]
        return raw.select(
            *[F.col(f"`{c}`").alias(c.strip()) for c in data_columns],
            F.col("_metadata.file_path").alias("_volume_path"),
            F.regexp_replace(F.col("_metadata.file_name"), r"\.(txt|json)$", "").alias("_content_hash"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
