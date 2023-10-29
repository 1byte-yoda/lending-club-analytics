# Databricks notebook source
dbutils.widgets.text("env", "prod")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType

# COMMAND ----------

parent_path = [str(Path(p).parent) for p in sys.path if "processor" in p]
sys.path.extend(parent_path)

from conf import Config
from helper import add_ingestion_date, add_surrogate_key, overwrite_table

# COMMAND ----------

config = Config(env=ENV)

# COMMAND ----------

file_path = f"{config.lending_analytics_dl_bronze_path}/loan_defaulters.csv"

# COMMAND ----------

defaulter_schema = StructType(
    [
        StructField("loan_id", StringType()),
        StructField("mem_id", StringType()),
        StructField("def_id", StringType()),
        StructField("delinq_2yrs", IntegerType()),
        StructField("delinq_amnt", IntegerType()),
        StructField("pub_rec", IntegerType()),
        StructField("pub_rec_bankruptcies", IntegerType()),
        StructField("inq_last_6mths", IntegerType()),
        StructField("total_rec_late_fee", IntegerType()),
        StructField("hardship_flag", StringType()),
        StructField("hardship_type", StringType()),
        StructField("hardship_length", IntegerType()),
        StructField("hardship_amount", FloatType()),
    ]
)

# COMMAND ----------

defaulter_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(defaulter_schema)
              .load())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename columns

# COMMAND ----------

defaulter_renamed_cols_df = (defaulter_df
                                .withColumnRenamed("mem_id", "member_id")
                                .withColumnRenamed("def_id", "defaulter_id")
                                .withColumnRenamed("delinq_2yrs", "delinquency_2yrs")
                                .withColumnRenamed("delinq_amnt", "delinquency_amount")
                                .withColumnRenamed("pub_rec", "public_records")
                                .withColumnRenamed("pub_rec_bankruptcies", "public_record_bankruptcies")
                                .withColumnRenamed("inq_last_6mths", "inquiry_last_6months")
                                .withColumnRenamed("total_rec_late_fee", "total_received_late_fee"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Replace null string with null value

# COMMAND ----------

defaulter_without_null_df = defaulter_renamed_cols_df.replace("null", None)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Loading metadata

# COMMAND ----------

defaulter_with_ingest_date_df = add_ingestion_date(df=defaulter_without_null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add loan_defaulter_key field (Surrogate Key) – SHA-2 (member_id, loan_id, def_id)

# COMMAND ----------

defaulter_with_sk_df = add_surrogate_key("member_id", "loan_id", "defaulter_id", df=defaulter_with_ingest_date_df, key_name="loan_defaulter_key")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Re-arrange Columns

# COMMAND ----------

final_defaulter_df = defaulter_with_sk_df.select("loan_defaulter_key", *[c for c in defaulter_with_sk_df.columns if c != "loan_defaulter_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to processed container as delta format

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_silver_path}/dim_loan_defaulter"

# COMMAND ----------

overwrite_table(df=final_defaulter_df, save_path=save_path, partition_fields=[])
