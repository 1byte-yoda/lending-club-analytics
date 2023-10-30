# Databricks notebook source
dbutils.widgets.text("env", "prod")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

from utils.conf import Config
from utils.helper import overwrite_table

# COMMAND ----------

config = Config(env=ENV)

# COMMAND ----------

dim_investor_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/dim_investor_loan")

dim_payment_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/dim_payment")

dim_defaulter_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/dim_loan_defaulter")

dim_account_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/dim_account")

dim_customer_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/dim_customer")

fact_loan_df = spark.read.format("delta").load(f"{config.lending_analytics_dl_silver_path}/fact_loan")

# COMMAND ----------

dim_investor_df.createOrReplaceTempView("dim_investor")
dim_payment_df.createOrReplaceTempView("dim_payment")
dim_defaulter_df.createOrReplaceTempView("dim_defaulter")
dim_account_df.createOrReplaceTempView("dim_account")
dim_customer_df.createOrReplaceTempView("dim_customer")
fact_loan_df.createOrReplaceTempView("fact_loan")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Premium to Non-Premium Users Ratio per Month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC       ,COUNT(IF(dc.premium_status = 'TRUE', 1, NULL)) AS total_premium_customers
# MAGIC       ,COUNT(1) AS total_customers
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')
# MAGIC ORDER BY `month`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Total Premium Customer per Month, Age Bracket and State

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,dc.age_bracket
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC       ,COUNT(IF(dc.premium_status = 'TRUE', 1, NULL)) AS total_premium_customers
# MAGIC       ,COUNT(1) AS total_customers
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM'), dc.age_bracket
# MAGIC ORDER BY `month`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Average Premium Customer Age per State and Country

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,ROUND(AVG(dc.age), 2) AS average_age
# MAGIC FROM dim_customer AS dc
# MAGIC WHERE dc.premium_status = 'TRUE'
# MAGIC GROUP BY dc.state

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to Gold Layer

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_gold_path}/lc_borrower_loan_score"

# COMMAND ----------

overwrite_table(df=final_loan_score, save_path=save_path, partition_fields=[])
