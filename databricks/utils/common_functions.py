# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import DataFrame

# COMMAND ----------

def add_ingestion_date(df: DataFrame) -> DataFrame:
    return df.withColumn("ingestion_date", F.cast(TimestampType(), F.current_timestamp()))

# COMMAND ----------

def show_null_counts(df: DataFrame):
    display(df.select([F.count(F.when(F.isnotnull(c), F.col(c))).alias(c) for c in df.columns]))
