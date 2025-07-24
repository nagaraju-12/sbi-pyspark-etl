# Databricks notebook source
dbutils.fs.ls("/Volumes/workspace/default/sbi_volume/sbi_customers.csv")

# COMMAND ----------

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date, rank, dense_rank, sum, avg, count
from pyspark.sql.window import Window
from datetime import datetime
import sys
import traceback

# COMMAND ----------

# Step 1: Start Spark Session
spark = SparkSession.builder.appName("SBI_Customer_Project").getOrCreate()

# Logging start
print(f" ETL Job Started at {datetime.now()}")

# COMMAND ----------

try:
    # Step 2: Load Raw CSV Data
    df = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("dbfs:/Volumes/workspace/default/sbi_volume/sbi_customers.csv")

    # Step 3: Remove nulls in critical columns
    df = df.filter(
        col("Balance").isNotNull() & 
        col("Monthly_EMI").isNotNull() & 
        col("Electricity_Bill").isNotNull()
    )

    # Step 4: Add flag for age
    df_age = df.withColumn("age_1", when(col("age") > 35, "Yes").otherwise("No"))

    # Step 5: Filter age > 35
    df_age = df_age.filter(col("age") > 35)

    # Step 6: Add current date
    df_age = df_age.withColumn("curr_date", current_date())

    # Step 7: Rank customers by branch
    df_age = df_age.withColumn("rank", rank().over(Window.orderBy("Branch"))) \
                   .withColumn("denseRank", dense_rank().over(Window.orderBy("Branch")))

    # Step 8: Add expense & surplus columns
    df_age = df_age.withColumn("Total_Expense", 
                    col("Monthly_EMI") + col("Electricity_Bill") + col("Water_Bill"))

    df_age = df_age.withColumn("Net_Surplus", col("Balance") - col("Total_Expense"))

    df_age = df_age.withColumn("Loss_Flag", 
                    when(col("Net_Surplus") < 0, "Yes").otherwise("No"))

    # Step 9: Save cleaned data
    df_age.write.mode("overwrite").parquet("dbfs:/Volumes/workspace/default/sbi_volume/cleaned_data_parquet")

    # Step 10: Load again for summarization
    df_clean = spark.read.parquet("dbfs:/Volumes/workspace/default/sbi_volume/cleaned_data_parquet")

    # Step 11: Group summary by branch
    df_summary = df_clean.groupBy("Branch").agg(
        sum("Balance").alias("Total_balance"),
        sum("Monthly_EMI").alias("Total_EMI"),
        sum("Loan_Amount").alias("Total_Loan_bill"),
        sum("Electricity_Bill").alias("Total_electricBill"),
        sum("Water_Bill").alias("Total_Waterbill"),
        avg("Balance").alias("Average_Balance"),
        avg("Monthly_EMI").alias("Average_EMI"),
        avg("Loan_Amount").alias("Average_Loan_Amount"),
        avg("Electricity_Bill").alias("Average_Electricity_Bill"),
        avg("Water_Bill").alias("Average_Water_Bill"),
        count("*").alias("customer_count")
    )

    # Step 12: Save summary data
    df_summary.write.mode("overwrite").parquet("dbfs:/Volumes/workspace/default/sbi_volume/summarized_branch_data")

    # Success log
    print(f" Job completed successfully at {datetime.now()}")
    print("Summary saved to: dbfs:/Volumes/workspace/default/sbi_volume/summarized_branch_data")

except Exception as e:
    # Print error details
    print("ERROR occurred during ETL job")
    traceback.print_exc(file=sys.stdout)
    dbutils.notebook.exit("ERROR: " + str(e))

# COMMAND ----------


