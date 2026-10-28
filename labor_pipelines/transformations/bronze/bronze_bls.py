from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "labor_stats"
RAW_SCHEMA = "raw"
LANDING_VOLUME = "landing_zone"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}/raw"
BLS_TIMESERIES = "bls/pr/"

BRONZE_CATALOG = "labor_stats"
BRONZE_SCHEMA = "bronze"

# documentation, not tab-delimited tables
NON_TABULAR_FILES = {"pr.txt", "pr.contacts"}


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


# One bronze streaming table per source file. Auto Loader picks up new file
# versions (content-addressed .txt files) incrementally; SDP manages
# checkpoints and schema location automatically.
for source_file in discover_source_files():
    table_name = bronze_table_name(source_file)

    @dp.table(
        name=f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}",
        comment=f"Bronze layer: raw ingestion of {source_file} from BLS via Auto Loader.",
    )
    def bronze(sf=source_file):
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("sep", "\t")
            .option("header", "true")
            .load(f"{VOLUME_ROOT}/{BLS_TIMESERIES}{sf}/")
        )

        # BLS pads header names with whitespace ("value   "); data values are left as-is.
        # All source columns stay strings; trimming and typing happen in silver.
        return raw.select(
            *[F.col(f"`{c}`").alias(c.strip()) for c in raw.columns],
            F.col("_metadata.file_path").alias("_volume_path"),
            F.regexp_replace(F.col("_metadata.file_name"), r"\.txt$", "").alias("_content_hash"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
