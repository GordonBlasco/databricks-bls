# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Notebook
# MAGIC
# MAGIC Loads every raw file version from the volume into one bronze Delta table per source file.
# MAGIC
# MAGIC - Raw files are content-addressed (`raw/pr/<source_file>/<sha256>.txt`) and never overwritten,
# MAGIC   so every new version is a new path and Auto Loader picks up only files it hasn't seen.
# MAGIC - Bronze is append-only: each version BLS publishes is a full snapshot, so tables hold every
# MAGIC   snapshot stacked together. Pick the latest snapshot per table in silver.
# MAGIC - All columns stay strings; trimming and typing happen in silver.

# COMMAND ----------

from pathlib import Path

from pyspark.sql import functions as F


CATALOG = "labor_stats"
SCHEMA = "bls"
VOLUME = "datausa"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw"
BLS_TIMESERIES = "bls/pr/"

BRONZE_SCHEMA = "bls_bronze"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/_checkpoints/bronze"

# documentation, not tab-delimited tables
NON_TABULAR_FILES = {"pr.txt", "pr.contacts"}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# each source file is a directory of hash-named versions; stray flat files are ignored
source_dir = Path(VOLUME_ROOT) / BLS_TIMESERIES
source_files = sorted(
    p.name
    for p in source_dir.iterdir()
    if p.is_dir() and p.name not in NON_TABULAR_FILES
)
source_files

# COMMAND ----------

def bronze_table_name(source_file):
    # pr.data.0.Current -> labor_stats.bls_bronze.pr_data_0_current
    return f"{CATALOG}.{BRONZE_SCHEMA}.{source_file.replace('.', '_').lower()}"


def ingest_to_bronze(source_file):
    checkpoint = f"{CHECKPOINT_ROOT}/{source_file}"

    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("sep", "\t")
        .option("header", "true")
        .load(f"{VOLUME_ROOT}/{BLS_TIMESERIES}{source_file}/")
    )

    # BLS pads header names with whitespace ("value   "); data values are left as-is
    bronze = raw.select(
        *[F.col(f"`{c}`").alias(c.strip()) for c in raw.columns],
        F.col("_metadata.file_path").alias("_volume_path"),
        F.regexp_replace(F.col("_metadata.file_name"), r"\.txt$", "").alias("_content_hash"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )

    query = (
        bronze.writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # process new files, then stop
        .toTable(bronze_table_name(source_file))
    )
    query.awaitTermination()
    # availableNow can split a backlog into several micro-batches
    # progress entries can report None, e.g. when there was nothing new to read
    return sum(p["numInputRows"] or 0 for p in query.recentProgress)

# COMMAND ----------

for source_file in source_files:
    rows = ingest_to_bronze(source_file)
    print(f"{source_file}: {rows:,} new rows -> {bronze_table_name(source_file)}")

# COMMAND ----------

# MAGIC %md
# MAGIC Snapshot versions per bronze table. Each `_content_hash` is one full file version from BLS.

# COMMAND ----------

versions = None
for source_file in source_files:
    df = (
        spark.table(bronze_table_name(source_file))
        .groupBy("_content_hash", "_file_modified_at", "_ingested_at")
        .agg(F.count("*").alias("row_count"))
        .withColumn("source_file", F.lit(source_file))
    )
    versions = df if versions is None else versions.unionByName(df)

display(versions.orderBy("source_file", F.col("_file_modified_at").desc()))