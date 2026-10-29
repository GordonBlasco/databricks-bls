# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace setup
# MAGIC
# MAGIC Creates the Unity Catalog objects required by the ingestion job and
# MAGIC Spark Declarative Pipeline. Every statement is idempotent so this task
# MAGIC is safe to run before each ingestion.
# MAGIC
# MAGIC The job identity must be allowed to create catalogs, schemas, and
# MAGIC managed Volumes, and the workspace metastore must have managed storage.

# COMMAND ----------

CATALOG = "labor_stats"
SCHEMAS = ("raw", "bronze", "silver", "gold")
RAW_SCHEMA = "raw"
LANDING_VOLUME = "landing_zone"


spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"`{CATALOG}`.`{RAW_SCHEMA}`.`{LANDING_VOLUME}`"
)

print(f"Catalog ready: {CATALOG}")
print(f"Schemas ready: {', '.join(SCHEMAS)}")
print(f"Volume ready: /Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}")
