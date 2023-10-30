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
# MAGIC ### Scoring Mechanics

# COMMAND ----------

unacceptable = 0
very_bad = 100
bad = 250
good = 500
very_good = 650
excellent = 800

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grading Mechanincs

# COMMAND ----------

unacceptable_grade = "F"
very_bad_grade = "E"
bad_grade = "D"
good_grade = "C"
very_good_grade = "B"
excellent_grade = "A"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Calculating Payment Points
# MAGIC - last_payment_pts
# MAGIC   - very_bad = last_payment_amount < installment * 0.5
# MAGIC   - bad = last_payment_amount BETWEEN installment * 0.5 and installment
# MAGIC   - good = last_payment_amount = installment
# MAGIC   - very_good = last_payment_amount BETWEEN installment and installment * 1.5
# MAGIC   - excellent = last_payment_amount > installment * 1.5
# MAGIC - total_payment_pts
# MAGIC   - very_good = total_payment_recorded >= funded_amount_investor * 0.5
# MAGIC   - good = total_payment_recorded > 0 and last_payment_amount < funded_amount_investor * 0.5
# MAGIC   - unacceptable = total_payment_recorded = 0 or total_payment_recorded IS NULL

# COMMAND ----------

payment_points_df = spark.sql(f"""
    SELECT dc.member_id
        ,dc.first_name
        ,dc.last_name
        ,dc.state
        ,dc.country
        ,CASE
            WHEN dp.last_payment_amount < fl.installment * 0.5 THEN {very_bad}
            WHEN dp.last_payment_amount >= (fl.installment * 0.5) AND dp.last_payment_amount < fl.installment THEN {bad}
            WHEN dp.last_payment_amount = fl.installment THEN {good}
            WHEN dp.last_payment_amount > (fl.installment) AND dp.last_payment_amount <= (fl.installment * 1.50) THEN {very_good}
            WHEN dp.last_payment_amount > (fl.installment * 1.50) THEN {excellent}
            ELSE {unacceptable}
        END AS last_payment_pts
        ,CASE
            WHEN dp.total_payment_received >= (dp.funded_amount_investor * 0.50) THEN {very_bad}
            WHEN dp.total_payment_received < (dp.funded_amount_investor * 0.50) AND dp.total_payment_received > 0 then {good}
            WHEN dp.total_payment_received = 0 OR dp.total_payment_received IS NULL THEN {unacceptable}
        END AS total_payment_pts
    FROM fact_loan fl
    JOIN dim_customer dc ON dc.customer_key = fl.customer_key
    JOIN dim_payment dp ON dp.payment_key = fl.payment_key                              
""")
payment_points_df.createOrReplaceTempView("payment_points")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Calculating Defaulter History Points
# MAGIC
# MAGIC - delinquency_pts
# MAGIC   - excellent = defaulters_2yrs = 0
# MAGIC   - bad = defaulters_2yrs BETWEEN 1 & 2
# MAGIC   - very_bad = defaulters_2yrs BETWEEN 3 & 5
# MAGIC   - unacceptable = defaulters_2yrs > 5 or defaulters_2yrs IS NULL
# MAGIC - public_records_pts
# MAGIC   - excellent = public_records = 0
# MAGIC   - bad = public_records BETWEEN 1 & 2
# MAGIC   - very_bad = public_records BETWEEN 3 & 5
# MAGIC   - unacceptable = public_records > 5 or public_records IS NULL
# MAGIC - public_records_bankruptcies_pts
# MAGIC   - excellent = public_records_bankruptcies = 0
# MAGIC   - bad = public_records_bankruptcies BETWEEN 1 & 2
# MAGIC   - very_bad = public_records_bankruptcies BETWEEN 3 & 5
# MAGIC   - unacceptable = public_records_bankruptcies > 5 or public_records_bankruptcies IS NULL
# MAGIC - enquiries_6months_pts
# MAGIC   - excellent = enquiries_6months = 0
# MAGIC   - bad = enquiries_6months BETWEEN 1 & 2
# MAGIC   - very_bad = enquiries_6months BETWEEN 3 & 5
# MAGIC   - unacceptable = enquiries_6months > 5 or enquiries_6months IS NULL
# MAGIC - hardship_flag_pts
# MAGIC   - very_good = hardship_flag = 'N'
# MAGIC   - bad = hardship_flag = 'Y'

# COMMAND ----------

loan_default_points_df = spark.sql(f"""
    SELECT p.*
        ,dd.loan_defaulter_key
        ,CASE
            WHEN dd.delinquency_2yrs = 0 THEN {excellent}
            WHEN dd.delinquency_2yrs BETWEEN 1 AND 2 THEN {bad}
            WHEN dd.delinquency_2yrs BETWEEN 3 AND 5 THEN {very_bad}
            WHEN dd.delinquency_2yrs > 5 OR dd.delinquency_2yrs IS NULL THEN {unacceptable}
         END AS delinquency_pts
        ,CASE
            WHEN dd.public_records = 0 THEN {excellent}
            WHEN dd.public_records BETWEEN 1 AND 2 THEN {bad}
            WHEN dd.public_records BETWEEN 3 AND 5 THEN {very_bad}
            WHEN dd.public_records > 5 OR dd.public_records IS NULL THEN {unacceptable}
         END AS public_records_pts
        ,CASE
            WHEN dd.public_record_bankruptcies = 0 THEN {excellent}
            WHEN dd.public_record_bankruptcies BETWEEN 1 AND 2 THEN {bad}
            WHEN dd.public_record_bankruptcies BETWEEN 3 AND 5 THEN {very_bad}
            WHEN dd.public_record_bankruptcies > 5 OR dd.public_record_bankruptcies IS NULL THEN {unacceptable}
         END AS public_bankruptcies_pts
        ,CASE
            WHEN dd.inquiry_last_6months = 0 THEN {excellent}
            WHEN dd.inquiry_last_6months BETWEEN 1 AND 2 THEN {bad}
            WHEN dd.inquiry_last_6months BETWEEN 3 AND 5 THEN {very_bad}
            WHEN dd.inquiry_last_6months > 5 OR dd.inquiry_last_6months IS NULL THEN {unacceptable}
         END AS inquiries_6months_pts
        ,CASE
            WHEN dd.hardship_flag = 'N' THEN {very_good}
            WHEN dd.hardship_flag = 'Y' OR dd.hardship_flag IS NULL THEN {bad}
        END AS hardship_pts
    FROM dim_defaulter AS dd
    LEFT JOIN payment_points AS p ON p.member_id = dd.member_id
""")
loan_default_points_df.createOrReplaceTempView("loan_default_points")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Calculating Financial Health Points

# COMMAND ----------

financial_health_points_df = spark.sql(f"""
    SELECT ld.*
        ,CASE 
            WHEN LOWER(fl.loan_status) LIKE '%fully paid%' THEN {excellent}
            WHEN LOWER(fl.loan_status) LIKE '%current%' THEN {good}
            WHEN LOWER(fl.loan_status) LIKE '%in grace period%' THEN {bad}
            WHEN LOWER(fl.loan_status) LIKE '%late (16-30 days)%' OR LOWER(fl.loan_status) LIKE '%late (31-120 days)%' THEN {very_bad}
            WHEN LOWER(fl.loan_status) LIKE '%charged off%' THEN {unacceptable}
        END AS loan_status_pts
        ,CASE 
            WHEN LOWER(da.home_ownership) LIKE '%own%' THEN {excellent} 
            WHEN LOWER(da.home_ownership) LIKE '%rent%' THEN {good} 
            WHEN LOWER(da.home_ownership) LIKE '%mortgage%' THEN {bad} 
            WHEN LOWER(da.home_ownership) LIKE '%any%' OR LOWER(da.home_ownership) IS NULL THEN {very_bad} 
        END AS home_pts
        ,CASE 
            WHEN fl.funded_amount <= (da.total_high_credit_limit * 0.10) THEN {excellent} 
            WHEN fl.funded_amount > (da.total_high_credit_limit * 0.10) AND fl.funded_amount <= (da.total_high_credit_limit * 0.20)  THEN {very_good} 
            WHEN fl.funded_amount > (da.total_high_credit_limit * 0.20) AND fl.funded_amount <= (da.total_high_credit_limit * 0.30)  THEN {good} 
            WHEN fl.funded_amount > (da.total_high_credit_limit * 0.30) AND fl.funded_amount <= (da.total_high_credit_limit * 0.50)  THEN {bad} 
            WHEN fl.funded_amount > (da.total_high_credit_limit * 0.50) AND fl.funded_amount <= (da.total_high_credit_limit * 0.70)  THEN {very_bad} 
            WHEN fl.funded_amount > (da.total_high_credit_limit * 0.70) THEN {unacceptable} 
        END AS credit_limit_pts
        ,CASE 
            WHEN (da.grade) ='A' and (da.sub_grade)='A1' THEN {excellent} 
            WHEN (da.grade) ='A' and (da.sub_grade)='A2' THEN ({excellent}* 0.80) 
            WHEN (da.grade) ='A' and (da.sub_grade)='A3' THEN ({excellent}* 0.60) 
            WHEN (da.grade) ='A' and (da.sub_grade)='A4' THEN ({excellent}* 0.40) 
            WHEN (da.grade) ='A' and (da.sub_grade)='A5' THEN ({excellent}* 0.20) 
            WHEN (da.grade) ='B' and (da.sub_grade)='B1' THEN ({very_good}) 
            WHEN (da.grade) ='B' and (da.sub_grade)='B2' THEN ({very_good}* 0.80) 
            WHEN (da.grade) ='B' and (da.sub_grade)='B3' THEN ({very_good}* 0.60) 
            WHEN (da.grade) ='B' and (da.sub_grade)='B4' THEN ({very_good}* 0.40) 
            WHEN (da.grade) ='B' and (da.sub_grade)='B5' THEN ({very_good}* 0.20) 
            WHEN (da.grade) ='C' and (da.sub_grade)='C1' THEN ({good}) 
            WHEN (da.grade) ='C' and (da.sub_grade)='C2' THEN ({good}* 0.80) 
            WHEN (da.grade) ='C' and (da.sub_grade)='C3' THEN ({good}* 0.60) 
            WHEN (da.grade) ='C' and (da.sub_grade)='C4' THEN ({good}* 0.40) 
            WHEN (da.grade) ='C' and (da.sub_grade)='C5' THEN ({good}* 0.20) 
            WHEN (da.grade) ='D' and (da.sub_grade)='D1' THEN ({bad}) 
            WHEN (da.grade) ='D' and (da.sub_grade)='D2' THEN ({bad}*0.80) 
            WHEN (da.grade) ='D' and (da.sub_grade)='D3' THEN ({bad}*0.60) 
            WHEN (da.grade) ='D' and (da.sub_grade)='D4' THEN ({bad}*0.40) 
            WHEN (da.grade) ='D' and (da.sub_grade)='D5' THEN ({bad}*0.20) 
            WHEN (da.grade) ='E' and (da.sub_grade)='E1' THEN ({very_bad}) 
            WHEN (da.grade) ='E' and (da.sub_grade)='E2' THEN ({very_bad}*0.80) 
            WHEN (da.grade) ='E' and (da.sub_grade)='E3' THEN ({very_bad}*0.60) 
            WHEN (da.grade) ='E' and (da.sub_grade)='E4' THEN ({very_bad}*0.40) 
            WHEN (da.grade) ='E' and (da.sub_grade)='E5' THEN ({very_bad}*0.20) 
            WHEN (da.grade) in ('F','G') and (da.sub_grade) in ('F1','G1') THEN ({unacceptable}) 
            WHEN (da.grade) in ('F','G') and (da.sub_grade) in ('F2','G2') THEN ({unacceptable}*0.80)
            WHEN (da.grade) in ('F','G') and (da.sub_grade) in ('F3','G3') THEN ({unacceptable}*0.60)
            WHEN (da.grade) in ('F','G') and (da.sub_grade) in ('F4','G4') THEN ({unacceptable}*0.40)
            WHEN (da.grade) in ('F','G') and (da.sub_grade) in ('F5','G5') THEN ({unacceptable}*0.20)
        END AS grade_pts 
    FROM loan_default_points AS ld
    LEFT JOIN fact_loan AS fl ON ld.loan_defaulter_key = fl.loan_defaulter_key 
    LEFT JOIN dim_account AS da ON da.member_id = ld.member_id
 """)
financial_health_points_df.createOrReplaceTempView("loan_score_details")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Calculating Weighted Loan Score
# MAGIC
# MAGIC - payment_history_pts = 20%
# MAGIC - defaulters_history_pts = 45%
# MAGIC - financial_health_pts = 35%

# COMMAND ----------

loan_score_df = spark.sql("""
    WITH tmp_loan_score AS (
        SELECT member_id
            ,first_name
            ,last_name
            ,state
            ,country
            ,(last_payment_pts + total_payment_pts) * 0.2 AS payment_history_pts
            ,(delinquency_pts + public_records_pts + public_bankruptcies_pts + inquiries_6months_pts + hardship_pts) * 0.45 AS defaulters_history_pts
            ,(loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35 AS financial_health_pts
        FROM loan_score_details
    )
    SELECT t.member_id
         ,t.first_name
         ,t.last_name
         ,t.state
         ,t.country
         ,t.payment_history_pts + t.defaulters_history_pts + t.financial_health_pts AS loan_score
    FROM tmp_loan_score AS t
""")
loan_score_df.createOrReplaceTempView("loan_score")

# COMMAND ----------

final_loan_score = spark.sql(f"""
    SELECT ls.*,
        CASE
            WHEN loan_score > {very_good} THEN '{excellent_grade}'
            WHEN loan_score <= {very_good} AND loan_score > {good} THEN '{very_good_grade}'
            WHEN loan_score <= {good} AND loan_score > {bad} THEN '{good_grade}'
            WHEN loan_score <= {bad} AND loan_score > {very_bad} THEN '{bad_grade}'
            WHEN loan_score <= {very_bad} AND loan_score > {unacceptable} THEN '{very_bad_grade}'
            WHEN loan_score <= {unacceptable} THEN '{unacceptable_grade}'
        END AS loan_final_grade
    FROM loan_score AS ls
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dump to Gold Layer

# COMMAND ----------

save_path = f"{config.lending_analytics_dl_gold_path}/lc_borrower_loan_score"

# COMMAND ----------

overwrite_table(df=final_loan_score, save_path=save_path, partition_fields=[])
