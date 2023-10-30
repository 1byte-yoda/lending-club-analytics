# Databricks notebook source
dbutils.widgets.text("env", "prod")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

# COMMAND ----------

from utils.conf import Config
from utils.helper import add_ingestion_date, add_surrogate_key, overwrite_table

# COMMAND ----------

config = Config(env=ENV)

# COMMAND ----------

file_path = f"{config.lending_analytics_dl_bronze_path}/loan_customer_data.csv"

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
# MAGIC ### Add age_bracket Field
# MAGIC - Youngsters = >= 18 & <= 25
# MAGIC - Working class = > 25 & <= 45
# MAGIC - Middle Age = > 45 & <= 59
# MAGIC - Senior Citizen > 60

# COMMAND ----------

customer_with_age_bracket_df = customer_renamed_cols_df.withColumn(
    "age_bracket", 
    F.when(F.col("age").between(F.lit("18"), F.lit("25")), "Youngsters")
    .when(F.col("age").between(F.lit("25"), F.lit("45")), "Working class")
    .when(F.col("age").between(F.lit("45"), F.lit("59")), "Middle age")
    .when(F.col("age") > F.lit("60"), "Senior citizen")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add loan_score Field
# MAGIC Loan score will help identify borrowers with lesser risk. It will be calculated using the following logic:
# MAGIC
# MAGIC loan_score = (payment_history_pts * .20) + (defaulters_history_pts * .45) + (financial_health_pts * .35)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add ingestion_date field for Incremental Loading metadata

# COMMAND ----------

customer_with_ingest_date_df = add_ingestion_date(df=customer_with_age_bracket_df)

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

save_path = f"{config.lending_analytics_dl_silver_path}/dim_customer"

# COMMAND ----------

overwrite_table(df=final_customer_df, save_path=save_path, partition_fields=["country", "state"])
