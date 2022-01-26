# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import sys

code_location = spark.conf.get("mypipeline.code_location")
sys.path.append(f"/Workspace{code_location}")

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

@dlt.create_table(comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets.")
def clickstream_raw():
  return lib_clickstream_raw(json_path)

# COMMAND ----------

@dlt.table(comment="Wikipedia clickstream data cleaned and prepared for analysis.")
@dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
def clickstream_prepared():
  return lib_clickstream_prepared(dlt.read("clickstream_raw"))

# COMMAND ----------

  @dlt.table(comment="A table containing the top pages linking to the Apache Spark page.")
  def top_spark_referrers():
    return lib_top_spark_referrers(dlt.read("clickstream_prepared"))
