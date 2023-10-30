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

### Business Questions (Gold Layer)
#### Targeted Customer Campaign
Here, I would like to identify which states has borrowers that incurs the minimum possible risk.
For the 2 most recent years I will derive below questions:
1. What is the top 10 state with the highest on-time payment ratio per state and month?
2. What is the top 10 state debt-to-income ratio per state and month?
3. What is the total loan score for each customer for the 2 most recent years?
4. How many customers do we have per employment length? (Employment Stability)
5. How many customers have a own home per month and state?
6. How many customers has a low count of delinquency incidences per month and state?
7. What is the percentage of derogatory borrowers per month and state?
8. What is the total received late fees per month and state?

#### Premium Customer Conversion Campaign
Here, I want to start a campaign to increase the number of premium customers
For the 2 most recent years I will derive below questions:
1. What is the total premium account holder ration for the past 2 years?
2. How many Premium Customers do we have per different age brackets and state?
3. What is the percentage of premium users per state and country?
4. What is the average Premium Customer Age per state?

### Business Level Field / Formulae Reference
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