from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.getActiveSession()

def lib_clickstream_raw(json_path):          
  return (
    spark.read.json(json_path).limit(100000)
  )

def lib_clickstream_prepared(df):
  return (
    df
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_title", "click_count", "previous_page_title")
  )

def lib_top_spark_referrers(df):
  return (
    df
      .filter(expr("current_page_title == 'Apache_Spark'"))
      .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("click_count"))
      .select("referrer", "click_count")
      .limit(10)
  )