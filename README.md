### Problem Statement
- Data Engineering team from pee-to-peer lending platform called Lending Club who are interested in getting insights from their peer-to-peer platform
customers data, calculating credit score and customers loan availing patterns which helps them to knock out the blockers hindering their business and adding more trusted customers into their
platform to grow the business.
- Credit score and loan defaulters history becomes critical for investors to decide on the righteousness of a borrower
  availing for a personal loan through the platform who can pay back the borrowed money on time. Analyzing the data belonging to lenders and borrowers
  can bring in high value to the platform on ways to improvise their existing business.
- The huge volume of data generated on a daily basis is converted into a form which is readily consumed by analyst
  and data scientists for deepend analysis and processing by making use of big data stack to ingest, clean, and transform the data.

### Lending Club Background
- Financial Services Firm HeadQuartered in San Francisco, California
- A peer-to-peer lending firm for personal loans
- It connects the borrowers having good credit score, payment records and established financial histories with
  potential lenders.
- They have a grading system for every borrower based on their credit score and income data which will help out the investors to make up
  the decision to lend their money or not.
- Typical Interest Rates can range from 6% to 36% which can spread across a repayment schedule of 36-60 months

### Architecture
- The data source will be scraped from [lending club's website](https://www.lendingclub.com/info/download-data.action) using Python Azure Function and then stage the data to ADLS Gen2 Bronze Layer.
- The data will be processed / cleansed using Azure Databricks and will be stored under the ADLS Gen2 silver layer
- After the data has been processed and cleansed, it will be transformed to answer business related questions. The transformed data will be stored in ADLS Gen2 Gold Layer
- The gold data will be validated in-transit (separate pipeline job).
- Synapse analytics will serve as the serving layer and have a connection to the ADLS Gen2 Gold Layer and will create managed tables corresponding to each transformed file.
- The created table in Synapse Analytics will be presented using Power BI.
- The whole process will be orchestrated in Azure Data Factory and will execute on a daily basis.
- The pipeline will be monitored using Logs Analytics and Azure Monitor.

### Data Loading Strategy
- The data will be loaded incrementally to the pipeline.
- The data in the silver and gold layer will be stored in delta file format to support schema evolution, and for a simpler incremental data loading implementation.
- The data will be partitioned using the year_month and customer_state fields for optimal aggregation (as per the business questions / requirements)

### CI/CD
- Databricks Transformations and Processing will be tested using Pytest and will run as part of Azure Devops pipeline.
- Pyspark scripts will undergo a flake8 linter and will be auto-formatted using black 

### ER Diagram

### Data Source Understanding
The data can be downloaded / scraped directly from [Lending Club](https://www.lendingclub.com/info/download-data.action).
In this project, I downloaded the data from [data.world](https://data.world/jaypeedevlin/lending-club-loan-data-2007-11), and there were 2 files – data dictionary and the loan data itself.
After downloading the file, I manually inspected the data to see possible logical groupings and ended up having 6 logical subgroups.
There are 6 csv files corresponding to each table logical grouping with the field's description below.

#### Customer
- id: unique ID for the loan listing
- member_id: unique ID for the borrower member
- premium_status: whether borrower holds a premium account
- first_name: first name of borrower
- last_name: last name of borrower
- age: borrower age
- country: borrower country
- state: borrower state
- zip_code: borrower zip code
- addr_state: borrower address

#### Loan Details
- loan_id: id of the loan availed by the borrower
- member_id: id for the borrower member
- account_id: account id of the borrower
- loan_amt: loan amount
- funded_amt: amount that got actually funded
- term: the number of months to pay on the loan. Values are in months and can be either 36 or 60
- int_rate: interest rate on the loan
- installment: the monthly payment amount owned by the borrower if the loan originates
- issue_d: the date on which the loan was funded
- loan_status: current status of the loan
    - Charged Off - didn't pay it
    - Grace Period - ie. does not pay for 1 week
- purpose: a category provided by the borrower for the loan request
    - car
    - credit_card - to pay for an overdue CC
    - debt_consolidation - you want to pay someone you owe
    - home improvement
    - house
    - major_purchase
    - medical
    - moving - to a new place
    - renewable_energy
    - small_business
    - vacation
- title: the loan title provided by the borrower
- disbursement_method: the method by which the borrower receives their loan. Possible values are: CASH, DIRECT_PAY
    - Cash
    - Direct Pay - pay directly to the lender

#### Account
- account_id: account id of the borrower
- member_id: id for the borrower member
- loan_id: id of the loan availed by the borrower
- grade: assigned loan grade
- sub_grade: assigned loan sub grade
- emp_title: the job title supplied by the borrower when applying for the loan.
- emp_length: employment length in years. Possible values are between 0 and 10 where 0 mean less than one year and 10 means ten or more years
- home_ownership: the home ownership status provided by the borrower during registration or obtained from the credit report. Values are RENT, OWN,
  MORTGAGE (paying installment), OTHER.
- annual_inc - the self-reported annual income provided by the borrower during registration
- verification_status: indicates if income was verified, not verified, or if the income source was verified
- total_hi_cred_lim: total high credit or credit limit (ie. based on annual income)
- application_type: indicates whether the loan is an individual application or joint application with two co-borrowers
- annual_inc_joint: the combined self-reported annual income provided by the co-borrowers during registration.

#### Investor Loan
- investor_loan_id: investor id for a given loan
- loan_id: id of the loan availed by the borrower
- investor_id: id of the investor
- funded_amnt_inv: amount funded by the investor
- funded_full: whether full loan amount has been funded or not
- investor_type: type of investor
- investor_age: investor age
- investor_country: investor country
- investor_state: investor_state

#### Loan Defaulters (Shows the trust I can have to the borrower)
- loan_id: id of the loan availed by the borrower
- mem_id: id for the borrower member
- loan_default_id: id of the defaulter loan
- delinq_2yrs: (didn't pay at all) number of 30+ days past-due incidences of delinquency in the borrower's credit file for the past 2 years
- delinq_amnt: the past due amount owed for the accounts on which the borrower is now delinquent.
- pub_rec: number of derogatory public records (bad image)
- pub_rec_bankruptcies: number of public record bankruptcies
- inq_last_6mnths: credit inquiries in the last 6 months at time of application for the secondary applicant.
- total_rec_late_fee: late fees received to date
- pub_rec_bankruptcies: number of public record bankruptcies
- hardship_flag: flags whether or not the borrower is on a hardship plan (to be excused in paying the loan for specific period of time)
- hardship_type: describes the hardship plan offering
- hardship_length: the number of months the borrower will make smaller payments than normally obligated due to a hardship plan

#### Payments
- loan_id: id of the loan availed by the borrower
- mem_id: id for the borrower member
- transaction_id: id of the payment transaction
- funded_pymnt: payment funded by the investor
- total_pymnt: payments received to date for total amount funded
- instalment_amnt: how much do they pay monthly
- last_pymnt_d: last month payment was received
- next_pymnt_d: next scheduled payment date
- hardship_amount: the interest payment that the borrower has committed to make each month they are on a hardship plan
- pymnt_plan: indicates if a payment plan has been put in place for the loan
- pymnt_method: CHECKS, CREDIT CARDS, DEBIT CARDS, PAYPAL
- last_pymnt_amnt: last time payment was received

### Data Processing Strategy (Silver Layer)
#### Customer
- Rename Columns
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (member_id, age, state)
- Dump to processed container as delta format

#### Loan Details
- Rename columns
- Replace null string with null value
- term (remove months suffix)
- interest (remove % character)
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (member_id, loan_id, loan_amount)
- Dump to processed container as delta format

#### Account
- Rename columns
- Replace null string with null value
- emp_length (Replace  "n/a" with null value)
- emp_length (Replace "< 1 year" with 1 and "10+ years" with 10)
- emp_length (Remove "years" or "year" suffix)
- emp_length to integer
- Replace null string with null value
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (member_id, account_id, loan_id)
- Dump to processed container as delta format

#### Investor
- Rename columns
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (investor_loan_id, loan_id, investor_id)
- Dump to processed container as delta format

#### Loan Defaulters
- Rename columns
- Replace null string with null value
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (member_id, loan_id, def_id)
- Dump to processed container as delta format

#### Payment
- Rename columns
- Replace null string with null value
- ingestion_date (Incremental Load & Partitioning)
- surrogate_key (Identify a unique row) – SHA-2 (member_id, loan_id, latest_transaction_id)
- Dump to processed container as delta format

### Call Function Variables Config Notebook
- All file path variables are defined here and is ready for import
- Function to create a new ingestion_date column in a dataframe

### Business Questions (Gold Layer)
#### Targeted Customer Campaign
Here, I would like to identify which states has borrowers that incurs the minimum possible risk.
For the 2 most recent years I will derive below questions:
1. What is the top 10 state with the highest on-time payment ratio per state and month?
2. What is the top 10 state debt-to-income ratio per state and month?
3. What is the total loan score for each customer for the 2 most recent years?
4. How many customers do we have per employment length? (Employment Stability)
5. How many customers have a own home per month and state?

#### Premium Customer Conversion Campaign
Here, I want to start a campaign to increase the number of premium customers
For the 2 most recent years I will derive below questions:
1. What is the total premium account holder ration for the past 2 years?
2. How many Premium Customers do we have per different age brackets and country?
3. What is the percentage of premium users per state and country?
4. What is the average Premium Customer Age per state and country?

#### Investor Retention
I want to monitor the investor's performance and increase their retention to the platform.
For the 2 most recent years I will derive below questions:
1. What is the expected returns to funded amount ratio for each investor?
2. What is the projected loan score vs. the actual performance score per investor? (Investor's ROI)
3. How long does it take for each investor to fully fund a loan?
4. How consistent are payments received as scheduled?
5. What is the distribution of different types of investors?

#### Lessen Defaulters Campaign
I want to lessen the defaulters in the platform and covert them into a more trustworthy borrowers
For the 2 most recent years I will derive below questions:
1. What is the monthly default rate ratio?
2. How many late fees are there per month?
3. What is the hardship plan utilization per month?
4. What is the percentage of borrowers with derogatory public records per month?
5. How frequent delinquency incidents happen per month?

### Business Level Field / Formula Reference
#### Customer Data
##### age_brackets
  - Youngsters = >= 18 & <= 25
  - Working class = > 25 & <= 35
  - Middle Age = > 35 & <= 45
  - Senior Citizen > 45

#### Loan Score
##### Scoring Mechanics
  - unacceptable = 0
  - very_bad = 100
  - bad = 250
  - good = 500
  - very_good = 650
  - excellent = 800
  - 
##### Calculating Payment Points
- last_payment_pts
  - very_bad = last_payment_amount < installment * 0.5
  - bad = last_payment_amount BETWEEN installment * 0.5 and installment
  - good = last_payment_amount = installment
  - very_good = last_payment_amount BETWEEN installment and installment * 1.5
  - excellent = last_payment_amount > installment * 1.5
- total_payment_pts
  - very_good = total_payment_recorded >= funded_amount_investor * 0.5
  - good = total_payment_recorded > 0 and last_payment_amount < funded_amount_investor * 0.5
  - unacceptable = total_payment_recorded = 0 or total_payment_recorded IS NULL

##### Calculating Defaulters History
- delinquency_pts
  - excellent = defaulters_2yrs = 0
  - bad = defaulters_2yrs BETWEEN 1 & 2
  - very_bad = defaulters_2yrs BETWEEN 3 & 5
  - unacceptable = defaulters_2yrs > 5 or defaulters_2yrs IS NULL
- public_records_pts
  - excellent = public_records = 0
  - bad = public_records BETWEEN 1 & 2
  - very_bad = public_records BETWEEN 3 & 5
  - unacceptable = public_records > 5 or public_records IS NULL
- public_records_bankruptcies_pts
  - excellent = public_records_bankruptcies = 0
  - bad = public_records_bankruptcies BETWEEN 1 & 2
  - very_bad = public_records_bankruptcies BETWEEN 3 & 5
  - unacceptable = public_records_bankruptcies > 5 or public_records_bankruptcies IS NULL
- enquiries_6months_pts
  - excellent = enquiries_6months = 0
  - bad = enquiries_6months BETWEEN 1 & 2
  - very_bad = enquiries_6months BETWEEN 3 & 5
  - unacceptable = enquiries_6months > 5 or enquiries_6months IS NULL
- hardship_flag_pts
  - very_good = hardship_flag = 'N'
  - bad = hardship_flag = 'Y'

##### Calculating Financial Health Points (Things they are owning, loan status customers have currently)
- loan_status_pts
  - excellent = loan_status is 'fully paid'
  - good = loan_status is 'current'
  - bad = loan_status is 'in grace period'
  - very_bad = loan_status is 'late (16-30 days)' or loan_status is 'late (31-120 days)'
  - unacceptable = loan_status is 'charged off'
- home_pts
  - excellent = home_ownership is 'own'
  - good = home_ownership is 'rent'
  - bad = home_ownership is 'mortgage'
  - very_bad = home_ownership is 'any' or home_ownership is NULL
- credit_limit_pts
  - excellent = funded_amount <= total_high_credit_limit * 0.10
  - very_good = funded_amount > total_high_credit_limit * 0.10 AND funded_amount <= total_high_credit_limit * 0.20
  - good = funded_amount > total_high_credit_limit * 0.20 AND funded_amount <= total_high_credit_limit * 0.30
  - bad = funded_amount > total_high_credit_limit * 0.30 AND funded_amount <= total_high_credit_limit * 0.50
  - very_bad = - very_good = funded_amount > total_high_credit_limit * 0.50 AND funded_amount <= total_high_credit_limit * 0.70
  - unacceptable = - very_good = funded_amount > total_high_credit_limit * 0.7
- grade_pts
  - 100% excellent = a.grade = 'A' and a.sub_grade = 'A1'
  - 80% excellent = a.grade = 'A' and a.sub_grade = 'A2'
  - 60% excellent = a.grade = 'A' and a.sub_grade = 'A3'
  - 40% excellent = a.grade = 'A' and a.sub_grade = 'A4'
  - 20% excellent = a.grade = 'A' and a.sub_grade = 'A5'
  - 100% very good = a.grade = 'B' and a.sub_grade = 'B1'
  - 80% very good = a.grade = 'B' and a.sub_grade = 'B2'
  - 60% very good = a.grade = 'B' and a.sub_grade = 'B3'
  - 40% very good = a.grade = 'B' and a.sub_grade = 'B4'
  - 20% very good = a.grade = 'B' and a.sub_grade = 'B5'
  - 100% good = a.grade = 'C' and a.sub_grade = 'C1'
  - 80% good = a.grade = 'C' and a.sub_grade = 'C2'
  - 60% good = a.grade = 'C' and a.sub_grade = 'C3'
  - 40% good = a.grade = 'C' and a.sub_grade = 'C4'
  - 20% good = a.grade = 'C' and a.sub_grade = 'C5'
  - 100% bad = a.grade = 'D' and a.sub_grade = 'D1'
  - 80% bad = a.grade = 'D' and a.sub_grade = 'D2'
  - 60% bad = a.grade = 'D' and a.sub_grade = 'D3'
  - 40% bad = a.grade = 'D' and a.sub_grade = 'D4'
  - 20% bad = a.grade = 'D' and a.sub_grade = 'D5'
  - 100% very bad = a.grade = 'E' and a.sub_grade = 'E1'
  - 80% very bad = a.grade = 'E' and a.sub_grade = 'E2'
  - 60% very bad = a.grade = 'E' and a.sub_grade = 'E3'
  - 40% very bad = a.grade = 'E' and a.sub_grade = 'E4'
  - 20% very bad = a.grade = 'E' and a.sub_grade = 'E5'
  - 100% unacceptable = a.grade = 'F' and a.sub_grade = 'F1'
  - 80% unacceptable = a.grade = 'F' and a.sub_grade = 'F2'
  - 60% unacceptable = a.grade = 'F' and a.sub_grade = 'F3'
  - 40% unacceptable = a.grade = 'F' and a.sub_grade = 'F4'
  - 20% unacceptable = a.grade = 'F' and a.sub_grade = 'F5'

##### Calculating The Weighted Loan Score
- payment_history_pts = 20%
- defaulters_history_pts = 45%
- financial_health_pts = 35%

##### Calculating The Final Loan Score
- loan_score = payment_history_pts + defaulters_history_pts + financial_health_pts
- loan_final_grade = map using the categorical grades
  - unacceptable_grade = "F"
  - very_bad_grade = "E"
  - bad_grade = "D"
  - good_grade = "C"
  - very_good_grade = "B"
  - excellent_grade = "A"
