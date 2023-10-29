# Databricks notebook source
import sys

from pyspark.sql.types import StructType, StructField, FloatType, DateType, StringType
import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append("/Workspace/Repos/lendingclub_pipeline/lending-club-analytics/az_databricks/")

from az_databricks.utils.common_funcs import add_ingestion_date, add_surrogate_key, overwrite_table
from az_databricks.utils.project_conf import lending_analytics_dl_bronze_path, lending_analytics_dl_silver_path, lending_analytics_dl_gold_path

# COMMAND ----------

file_path = f"{lending_analytics_dl_bronze_path}/loan_details.csv"

# COMMAND ----------

loan_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .load())

# COMMAND ----------

loan_df.printSchema()

# COMMAND ----------

loan_df.show(vertical=True)
