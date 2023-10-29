# Databricks notebook source
dbutils.widgets.text("env", "prod")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

parent_path = [str(Path(p).parent) for p in sys.path if "processor" in p]
sys.path.extend(parent_path)

from conf import Config
from helper import add_ingestion_date, add_surrogate_key, overwrite_table

# COMMAND ----------

config = Config(env=ENV)

# COMMAND ----------

file_path = f"{config.lending_analytics_dl_bronze_path}/loan_investors.csv"

# COMMAND ----------

investor_schema = StructType(
    [
        StructField("investor_loan_id", StringType()),
        StructField("loan_id", StringType()),
        StructField("investor_id", StringType()),
        StructField("loan_funded_amt", IntegerType()),
        StructField("investor_type", StringType()),
        StructField("age", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
    ]
)

# COMMAND ----------

investor_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(investor_schema)
              .load())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename columns

# COMMAND ----------

investor_renamed_cols_df = investor_df.withColumnRenamed("loan_funded_amt", "loan_funded_amount")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for incremental loading metadata

# COMMAND ----------

investor_with_ingest_date_df = add_ingestion_date(df=investor_renamed_cols_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add investor_loan_key field (surrogate key)
# MAGIC SHA-2 (investor_loan_id, loan_id, investor_id)

# COMMAND ----------

investor_with_sk_df = add_surrogate_key("investor_loan_id", "loan_id", "investor_id", df=investor_with_ingest_date_df, key_name="investor_loan_key")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Re-arrange columns

# COMMAND ----------

final_investor_df = investor_with_sk_df.select("investor_loan_key", *[c for c in investor_with_sk_df.columns if c != "investor_loan_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to processed container as delta format

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_silver_path}/dim_investor_loan"

# COMMAND ----------

overwrite_table(df=final_investor_df, save_path=save_path, partition_fields=["country", "state"])
