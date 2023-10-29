# Databricks notebook source
dbutils.widgets.text("env", "prod")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql.types import StructType, StructField, FloatType, DateType, StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

parent_path = [str(Path(p).parent) for p in sys.path if "processor" in p]
sys.path.extend(parent_path)

from conf import Config
from helper import add_ingestion_date, add_surrogate_key, overwrite_table

# COMMAND ----------

config = Config(env=ENV)

# COMMAND ----------

file_path = f"{config.lending_analytics_dl_bronze_path}/loan_details.csv"

# COMMAND ----------

loan_schema = StructType(
    [
        StructField("loan_id", StringType()),
        StructField("mem_id", StringType()),
        StructField("acc_id", StringType()),
        StructField("loan_amt", IntegerType()),
        StructField("fnd_amt", IntegerType()),
        StructField("term", StringType()),
        StructField("interest", StringType()),
        StructField("installment", FloatType()),
        StructField("issue_date", DateType()),
        StructField("loan_status", StringType()),
        StructField("purpose", StringType()),
        StructField("title", StringType()),
        StructField("disbursement_method", StringType())
    ]
)

# COMMAND ----------

loan_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(loan_schema)
              .load())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename columns

# COMMAND ----------

loan_column_renamed_df = (loan_df.withColumnRenamed("mem_id", "member_id")
                          .withColumnRenamed("acc_id", "account_id")
                          .withColumnRenamed("loan_amt", "loan_amount")
                          .withColumnRenamed("fnd_amt", "funded_amount")
                          .withColumnRenamed("term", "term_months"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Replace null string with null value
# MAGIC

# COMMAND ----------

loan_without_null_df = loan_column_renamed_df.replace("null", None)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Process term field 
# MAGIC Remove months suffix

# COMMAND ----------

loan_term_processed_df = loan_without_null_df.withColumn("term_months", F.regexp_replace("term_months", "[^0-9]", "").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Process interest field
# MAGIC Remove % character

# COMMAND ----------

loan_interest_processed_df = loan_term_processed_df.withColumn("interest", F.regexp_replace("interest", "%", "").cast(FloatType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Load metadata
# MAGIC

# COMMAND ----------

loan_with_ingest_date_df = add_ingestion_date(df=loan_interest_processed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add loan_key (Surrogate Key)
# MAGIC SHA-2 (member_id, loan_id, loan_amount)

# COMMAND ----------

loan_with_sk_df = add_surrogate_key("member_id", "loan_id", "loan_amount", df=loan_with_ingest_date_df, key_name="loan_key")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Add dimension keys
# MAGIC investor_loan_key, payment_key, loan_defaulter_key, account_key

# COMMAND ----------

dim_investor_df = spark.read.format("delta").load(f"{lending_analytics_dl_silver_path}/dim_investor_loan")

dim_payment_df = spark.read.format("delta").load(f"{lending_analytics_dl_silver_path}/dim_payment")

dim_defaulter_df = spark.read.format("delta").load(f"{lending_analytics_dl_silver_path}/dim_loan_defaulter")

dim_account_df = spark.read.format("delta").load(f"{lending_analytics_dl_silver_path}/dim_account")

dim_customer_df = spark.read.format("delta").load(f"{lending_analytics_dl_silver_path}/dim_customer")


# COMMAND ----------

loan_with_dim_keys_df = loan_with_sk_df.join(
    dim_investor_df, dim_investor_df["loan_id"] == loan_with_sk_df["loan_id"], "inner"
).join(
    dim_payment_df,
    (dim_payment_df["loan_id"] == loan_with_sk_df["loan_id"]) & (dim_payment_df["member_id"] == loan_with_sk_df["member_id"]),
    "inner"
).join(
    dim_defaulter_df,
    (dim_defaulter_df["loan_id"] == loan_with_sk_df["loan_id"]) & (dim_defaulter_df["member_id"] == loan_with_sk_df["member_id"]),
    "inner"
).join(
    dim_account_df,
    (dim_account_df["loan_id"] == loan_with_sk_df["loan_id"]) & (dim_account_df["member_id"] == loan_with_sk_df["member_id"]) & (dim_account_df["account_id"] == loan_with_sk_df["account_id"]),
    "inner"
).join(
    dim_customer_df,
    (dim_customer_df["member_id"] == loan_with_sk_df["member_id"]),
    "inner"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Select Necessary Fields Only
# MAGIC member_id, loan_id

# COMMAND ----------

final_fact_loan_df = loan_with_dim_keys_df.select(
    F.col("investor_loan_key"),
    F.col("payment_key"),
    F.col("loan_defaulter_key"),
    F.col("account_key"),
    F.col("loan_amount"),
    F.col("funded_amount"),
    F.col("term_months"),
    F.col("interest"),
    loan_with_sk_df["installment"],
    F.col("issue_date"),
    F.col("loan_status"),
    F.col("purpose"),
    F.col("title"),
    F.col("disbursement_method"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to processed container as delta format

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_silver_path}/fact_loan"

# COMMAND ----------

overwrite_table(df=final_fact_loan_df, save_path=save_path, partition_fields=[])
