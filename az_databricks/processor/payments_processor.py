# Databricks notebook source
import sys

from pyspark.sql.types import (
    StructType, StructField, FloatType, DateType, StringType, IntegerType
)
import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append("/Workspace/Repos/lendingclub_pipeline/lending-club-analytics/az_databricks")
sys.path.append("/live")

from utils.common_functions import add_ingestion_date, add_surrogate_key, overwrite_table
from utils.project_config import lending_analytics_dl_bronze_path, lending_analytics_dl_silver_path

# COMMAND ----------

file_path = f"{lending_analytics_dl_bronze_path}/loan_payment.csv"

# COMMAND ----------

payment_schema = StructType(
    [
        StructField("loan_id", StringType()),
        StructField("mem_id", StringType()),
        StructField("latest_transaction_id", StringType()),
        StructField("funded_amnt_inv", IntegerType()),
        StructField("total_pymnt_rec", FloatType()),
        StructField("installment", FloatType()),
        StructField("last_pymnt_amnt", FloatType()),
        StructField("last_pymnt_d", DateType()),
        StructField("next_pymnt_d", DateType()),
    ]
)

# COMMAND ----------

payment_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(payment_schema)
              .load())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Processing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

payment_renamed_cols_df = (payment_df.withColumnRenamed("mem_id", "member_id")
                .withColumnRenamed("funded_amnt_inv", "funded_amount_investor")
                .withColumnRenamed("total_pymnt_rec", "total_payment_received")
                .withColumnRenamed("last_pymnt_amnt", "last_payment_amount")
                .withColumnRenamed("last_pymnt_d", "last_payment_date")
                .withColumnRenamed("next_pymnt_d", "next_payment_d"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Loading metadata
# MAGIC

# COMMAND ----------

payment_with_ingestion_date_df = add_ingestion_date(df=payment_renamed_cols_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add payment_key field (surrogate key) 
# MAGIC Use SHA-2 for the dataset's composite key (member_id, loan_id, latest_transaction_id)
# MAGIC

# COMMAND ----------

payment_with_sk_df = add_surrogate_key("member_id", "loan_id", "latest_transaction_id", df=payment_with_ingestion_date_df, key_name="payment_key")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Re-arrange columns

# COMMAND ----------

final_payment_df = payment_with_sk_df.select("payment_key", *[c for c in payment_with_sk_df.columns if c != "payment_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Partition Column year_month

# COMMAND ----------

final_payment_df = final_payment_df.withColumn("year_month", F.coalesce(F.date_format("last_payment_date", "yyyy-MM"), F.lit("2022-04")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to silver container as delta format

# COMMAND ----------

save_path = f"{lending_analytics_dl_silver_path}/dim_payment"

# COMMAND ----------

overwrite_table(df=final_payment_df, save_path=save_path, partition_fields=["year_month"])
