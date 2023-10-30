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
# MAGIC ### Late Loans Ratio Per State and Month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,COUNT(IF(dd.delinquency_2yrs > 0, 1, NULL)) AS total_late_loans
# MAGIC       ,COUNT(1) AS total_loans
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')
# MAGIC ORDER BY `month`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Debt to Income Ratio per State and Month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dc.state AS state
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS year_month
# MAGIC       ,ROUND(SUM(da.annual_income / 12), 2) AS total_income
# MAGIC       ,ROUND(SUM(fl.installment), 2) AS total_debt
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_customer dc ON dc.customer_key = fl.customer_key
# MAGIC JOIN dim_account da ON da.account_key = fl.account_key
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Employment Length Distribution for Each State
# MAGIC Which state has the most stable employees?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(da.member_id) AS total_customers
# MAGIC       ,dc.state
# MAGIC       ,da.employment_length
# MAGIC FROM dim_customer dc
# MAGIC JOIN dim_account da ON da.member_id = dc.member_id 
# MAGIC WHERE employment_length IS NOT NULL 
# MAGIC GROUP BY da.employment_length, dc.state

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Home Owner Borrowers per Month and State

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dc.state AS state
# MAGIC       ,COUNT(IF(da.home_ownership = 'OWN', 1, NULL)) AS total_home_owners
# MAGIC       ,COUNT(1) AS total_customers
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_customer dc ON dc.customer_key = fl.customer_key
# MAGIC JOIN dim_account da ON da.account_key = fl.account_key
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delinquency Incident per month and state

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,ROUND(SUM(dd.delinquency_2yrs) / 24, 2) AS total_delinquency
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')
# MAGIC ORDER BY `month`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Monthly Derogatory Borrowers per State

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,COUNT(IF(dd.public_records > 0 OR dd.public_record_bankruptcies > 0, 1, NULL)) AS total_derogatory_borrowers
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')
# MAGIC ORDER BY `month`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Monthly Late Received Fees

# COMMAND ----------

display(dim_defaulter_df.where(F.expr("hardship_length > 0")))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dc.state
# MAGIC       ,SUM(dd.total_received_late_fee) AS total_received_late_fee
# MAGIC       ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS month
# MAGIC FROM fact_loan AS fl
# MAGIC JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
# MAGIC JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
# MAGIC WHERE fl.issue_date IS NOT NULL
# MAGIC GROUP BY dc.state, DATE_FORMAT(fl.issue_date, 'yyyyMM')
# MAGIC ORDER BY `total_received_late_fee` DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Unified Table for Borrower Campaign

# COMMAND ----------

final_df = spark.sql("""
    SELECT dc.state
        ,da.employment_length
        ,dc.age_bracket
        ,DATE_FORMAT(fl.issue_date, 'yyyyMM') AS year_month
        ,ROUND(AVG(dc.age), 2) AS average_age
        ,COUNT(IF(dc.premium_status = 'TRUE', 1, NULL)) AS total_premium_borrowers
        ,COUNT(1) AS total_borrowers
        ,COUNT(IF(dd.delinquency_2yrs > 0, 1, NULL)) AS total_late_borrowers
        ,COUNT(IF(da.home_ownership = 'OWN', 1, NULL)) AS total_home_owners
        ,ROUND(SUM(dd.delinquency_2yrs) / 24, 2) AS total_delinquency
        ,COUNT(IF(dd.public_records > 0 OR dd.public_record_bankruptcies > 0, 1, NULL)) AS total_derogatory_borrowers
        ,SUM(dd.total_received_late_fee) AS total_received_late_fee
        ,ROUND(SUM(da.annual_income / 12), 2) AS total_income
        ,ROUND(SUM(fl.installment), 2) AS total_debt
    FROM fact_loan AS fl
    JOIN dim_defaulter AS dd ON dd.loan_defaulter_key = fl.loan_defaulter_key
    JOIN dim_customer AS dc ON dc.customer_key = fl.customer_key
    JOIN dim_account AS da ON da.account_key = fl.account_key
    WHERE fl.issue_date IS NOT NULL
    GROUP BY dc.state, da.employment_length, DATE_FORMAT(fl.issue_date, 'yyyyMM'), dc.age_bracket
    ORDER BY year_month
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to Gold Layer

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_gold_path}/lc_borrower_campaign"

# COMMAND ----------

overwrite_table(df=final_df, save_path=save_path, partition_fields=["year_month", "state"])
