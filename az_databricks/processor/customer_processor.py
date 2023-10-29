# Databricks notebook source
import sys

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

sys.path.append("/Workspace/Repos/lendingclub_pipeline/lending-club-analytics/az_databricks/")

from az_databricks.utils.common_functions import add_ingestion_date, add_surrogate_key, overwrite_table
from az_databricks.utils.project_config import lending_analytics_dl_bronze_path, lending_analytics_dl_silver_path

# COMMAND ----------

file_path = f"{lending_analytics_dl_bronze_path}/loan_customer_data.csv"

# COMMAND ----------

customer_schema = StructType(
    [
        StructField("cust_id", StringType()),
        StructField("mem_id", StringType()),
        StructField("fst_name", StringType()),
        StructField("lst_name", StringType()),
        StructField("prm_status", StringType()),
        StructField("age", IntegerType()),
        StructField("state", StringType()),
        StructField("country", StringType())
    ]
)

# COMMAND ----------

customer_df = (spark.read
              .format("csv")
              .option("path", file_path)
              .option("header", True)
              .schema(customer_schema)
              .load())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename Columns

# COMMAND ----------

customer_renamed_cols_df = (customer_df.withColumnRenamed("fst_name", "first_name")
                                .withColumnRenamed("lst_name", "last_name")
                                .withColumnRenamed("prm_status", "premium_status")
                                .withColumnRenamed("mem_id", "member_id")
                                .withColumnRenamed("cust_id", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Loading metadata

# COMMAND ----------

customer_with_ingest_date_df = add_ingestion_date(df=customer_renamed_cols_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add customer_key field (Surrogate Key)
# MAGIC SHA-2 (customer_id, member_id, age, state)
# MAGIC

# COMMAND ----------

customer_with_sk_df = add_surrogate_key("customer_id", "member_id", "age", "state", df=customer_with_ingest_date_df, key_name="customer_key")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Re-arrange Columns

# COMMAND ----------

final_customer_df = customer_with_sk_df.select("customer_key", *[c for c in customer_with_sk_df.columns if c != "customer_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to processed container as delta format

# COMMAND ----------

save_path = f"{lending_analytics_dl_silver_path}/dim_customer"

# COMMAND ----------

overwrite_table(df=final_customer_df, save_path=save_path, partition_fields=["country", "state"])
