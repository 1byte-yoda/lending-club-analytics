# Databricks notebook source
import sys

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

sys.path.append("/Workspace/Repos/lendingclub_pipeline/lending-club-analytics/az_databricks/")

from az_databricks.utils.common_functions import add_ingestion_date, add_surrogate_key, overwrite_table
from az_databricks.utils.project_config import lending_analytics_dl_bronze_path, lending_analytics_dl_silver_path

# COMMAND ----------

file_path = f"{lending_analytics_dl_bronze_path}/account_details.csv"

# COMMAND ----------

account_schema = StructType(
    [
        StructField("acc_id", StringType()),
        StructField("mem_id", StringType()),
        StructField("loan_id", StringType()),
        StructField("grade", StringType()),
        StructField("sub_grade", StringType()),
        StructField("emp_title", StringType()),
        StructField("emp_length", StringType()),
        StructField("home_ownership", StringType()),
        StructField("annual_inc", IntegerType()),
        StructField("verification_status", StringType()),
        StructField("tot_hi_cred_lim", IntegerType()),
        StructField("application_type", StringType()),
        StructField("annual_income_joint", IntegerType()),
        StructField("verification_status_joint", StringType())
    ]
)

# COMMAND ----------

account_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(account_schema)
              .load())

# COMMAND ----------

account_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Processing

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename columns

# COMMAND ----------

account_col_renamed_df = (account_df
                            .withColumnRenamed("acc_id", "account_id")
                            .withColumnRenamed("mem_id", "member_id")
                            .withColumnRenamed("emp_title", "employment_title")
                            .withColumnRenamed("emp_length", "employment_length")
                            .withColumnRenamed("annual_inc", "annual_income")
                            .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace 'null' with SQL NULL

# COMMAND ----------

account_no_null_df = account_col_renamed_df.replace('null', None)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Process emp_length
# MAGIC - Replace "n/a" with null value
# MAGIC - Replace "< 1 year" with 1 and "10+ years" with 10
# MAGIC - Remove "years" or "year" suffix
# MAGIC - Convert to Integer
# MAGIC

# COMMAND ----------

account_no_null_df = account_no_null_df.withColumn("employment_length",  F.regexp_replace("employment_length", "[^0-9]", "").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Loading metadata
# MAGIC

# COMMAND ----------

account_with_ingestion_date_df = add_ingestion_date(df=account_no_null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add account_key field (Surrogate Key)
# MAGIC Use SHA-2 algorithm with member_id, account_id, loan_id fields

# COMMAND ----------

composite_key_fields = ["member_id", "account_id", "loan_id"]
account_with_sk_df = add_surrogate_key(
    *composite_key_fields,
    df=account_with_ingestion_date_df,
    key_name="account_key"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Re-arrange Columns

# COMMAND ----------

final_account_df = account_with_sk_df.select("account_key", *[c for c in account_with_sk_df.columns if c != "account_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to silver container as delta format

# COMMAND ----------

save_path = f"{lending_analytics_dl_silver_path}/dim_account"

# COMMAND ----------

overwrite_table(df=final_account_df, save_path=save_path, partition_fields=[])
