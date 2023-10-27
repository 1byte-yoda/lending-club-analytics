### Project Background
- Lending Club
    - Financial Services Firm HeadQuartered in San Francisco, California
    - A peer-to-peer lending firm for personal loans
    - It connects the borrowers having good credit score, payment records and established financial histories with
      potential lenders.
    - They have a grading system for every borrower based on their credit score and income data which will help out the investors to make up
      the decision to lend their money or not.
    - Typical Interest Rates can range from 6% to 36% which can spread across a repayment schedule of 36-60 months

### Problem Statement
- Data Engineering team from pee-to-peer lending platform called Lending Club who are interested in getting insights from their peer-to-peer platform
customers data, calculating credit score and customers loan availing patterns which helps them to knock out the blockers hindering their business and adding more trusted customers into their
platform to grow the business.
- Credit score and loan defaulters history becomes critical for investors to decide on the righteousness of a borrower
  availing for a personal loan through the platform who can pay back the borrowed money on time. Analyzing the data belonging to lenders and borrowers
  can bring in high value to the platform on ways to improvise their existing business.
- The huge volume of data generated on a daily basis is converted into a form which is readily consumed by analyst
  and data scientists for deepend analysis and processing by making use of big data stack to ingest, clean, and transform the data.

### Data Source Understanding
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

### ER Diagram


