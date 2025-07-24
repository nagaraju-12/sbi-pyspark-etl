# 🏦SBI Customer ETL Project using PySpark

This project processes 50,000 SBI bank customer records using PySpark. The data includes loan, EMI, utility bills, and balance information. It performs cleaning, transformations, summarization, and stores results in Parquet format.

## Features
- Null removal
- Age flagging
- Ranking customers
- Expense & surplus calculation
- Branch-level summarization

## File Structure
-  ' main_etl_script.py '  - PySpark code
-  ' data/sbi_customers.csv '  - Sample CSV file
-  ' README.md '  - Project documentation

 ##Requirements
- Apache Spark (Databricks or local)
- Python 3.8+
- PySpark

##Output
- Cleaned data:  ' cleaned_data_parquet ' 
- Summary data:  ' summarized_branch_data ' 
