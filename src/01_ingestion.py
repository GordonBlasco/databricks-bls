# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Notebook
# MAGIC
# MAGIC goals for this are to keep ingestions
# MAGIC
# MAGIC Some assumptions:
# MAGIC - Data is updated very infrequently (quartly?)
# MAGIC - pr.data.1.AllData is a historical super set of pr.data.0.Current (YTD?)
# MAGIC
# MAGIC Decisions:
# MAGIC - 

# COMMAND ----------

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import uuid

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)


BLS_HOME_DIR = "https://download.bls.gov"
BLS_PARENT_DIR = "/pub/time.series/"
BLS_TIMESERIES = "pr/"
BLS_DIRECTORY = f"{BLS_HOME_DIR}{BLS_PARENT_DIR}{BLS_TIMESERIES}"
BLS_SOURCE = f"bls/{BLS_TIMESERIES.rstrip('/')}"

POPULATION_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)

CATALOG = "labor_stats"
RAW_SCHEMA = "raw"
LANDING_VOLUME = "landing_zone"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{LANDING_VOLUME}/raw"
MANIFEST_TABLE = f"{CATALOG}.{RAW_SCHEMA}.ingestion_manifest"


CONTACT_EMAIL = "gordonblasco@gmail.com"
RUN_TS = datetime.now(timezone.utc)
RUN_ID = (RUN_TS.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8])

print(f"Run ID: {RUN_ID}")
print(f"Raw Volume path: {VOLUME_ROOT}")

# COMMAND ----------

retry_policy = Retry(
    total=3,                       # Up to 3 retries after the first attempt
    backoff_factor=1,              # Increasing delays between retries
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True,
    raise_on_status=False,
)

# BLS blocks requests without a User-Agent that includes contact info
session = requests.Session()
session.headers.update({
    "User-Agent": f"productivity-ingestion/1.0 ({CONTACT_EMAIL})"
})
session.mount("https://", HTTPAdapter(max_retries=retry_policy))


def get_response(url, *, params=None, stream=False):
    response = session.get(
        url,
        params=params,
        stream=stream,
        timeout=(10, 120),
    )
    response.raise_for_status()
    return response

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
    run_id STRING,
    run_timestamp TIMESTAMP,
    source STRING,
    source_file STRING,
    source_url STRING,
    content_hash STRING,
    volume_path STRING,
    status STRING
)
USING DELTA
""")

manifest_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("source", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("volume_path", StringType(), True),
    StructField("status", StringType(), False),
])

# COMMAND ----------

response = get_response(BLS_DIRECTORY)

# easy way to pull all links on a page
# keep only files inside this timeseries dir (skips the parent dir link)
soup = BeautifulSoup(response.text, "html.parser")
links = [a["href"]
         for a in soup.find_all("a", href=True)
         if a["href"].startswith(f"{BLS_PARENT_DIR}{BLS_TIMESERIES}")
         and not a["href"].endswith("/")
         ]

# (source, source_file, url) - list of touples
sources = [(BLS_SOURCE, 
            link.rsplit("/", 1)[-1], 
            f"{BLS_HOME_DIR}{link}") 
           for link in links]

sources.append(("datausa", 
                "population.json", 
                POPULATION_URL))
sources

# COMMAND ----------

# MAGIC %md
# MAGIC basics of the loop. for each dataset/link
# MAGIC
# MAGIC 1. pull the data
# MAGIC 2. save the file no matter what
# MAGIC 3. hash the file
# MAGIC 4. update the manifest
# MAGIC

# COMMAND ----------

results = []
for source, file_name, url in sources:
    row = {
        "run_id": RUN_ID,
        "run_timestamp": RUN_TS,
        "source": source,
        "source_file": file_name,
        "source_url": url,
        "content_hash": None,
        "volume_path": None,
        "status": "failed",
    }

    try:
        content = get_response(url).content
    except requests.RequestException as e:
        print(f"{file_name}: failed ({e})")
        results.append(row)
        continue

    # files are content-addressed, so an existing path means this exact content is already saved
    content_hash = sha256(content).hexdigest()
    extension = ".json" if file_name.endswith(".json") else ".txt"
    file_path = Path(VOLUME_ROOT) / source / file_name / f"{content_hash}{extension}"

    if file_path.exists():
        status = "unchanged"
    else:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # "xb" = exclusive create; raises instead of ever overwriting
        with open(file_path, "xb") as f:
            f.write(content)
        status = "new"

    row.update(content_hash=content_hash, volume_path=str(file_path), status=status)
    results.append(row)
    print(f"{file_name}: {status} -> {file_path}")

# COMMAND ----------

# one row per file per run, including unchanged and failed, so the table is a full run history
(
    spark.createDataFrame(results, schema=manifest_schema)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")  # lets new manifest columns land without a manual ALTER
    .saveAsTable(MANIFEST_TABLE)
)

display(spark.table(MANIFEST_TABLE).where(F.col("run_id") == RUN_ID))

failed_sources = sorted(
    row["source_file"] for row in results if row["status"] == "failed"
)
if failed_sources:
    raise RuntimeError(
        "Ingestion manifest was saved, but one or more downloads failed: "
        + ", ".join(failed_sources)
    )
