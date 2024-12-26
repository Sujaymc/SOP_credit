from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, lit
import os
import pandas as pd

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("Fraud Detection").config("spark.jars", "postgresql-42.2.20.jar").getOrCreate()
    print("Spark session created successfully.")

    # File path for CSV
    file_path = "D:\\fraudTest.csv"

    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file at path {file_path} does not exist.")

    # Read CSV with parsing dates
    df = spark.read.csv(file_path, header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss")
    print("CSV file loaded successfully.")

    # Ensure the data is sorted by date
    df = df.orderBy(col('trans_date_trans_time'))
    print("Data sorted by transaction date.")

    # Calculate the minimum date and maximum date within the first 100 days
    start_date = df.select(spark_min('trans_date_trans_time')).first()[0]
    end_date = start_date + pd.Timedelta(days=100)

    # Filter the data for the first 100 days
    filtered_data = df.filter((col('trans_date_trans_time') >= lit(start_date)) & 
                              (col('trans_date_trans_time') < lit(end_date)))
    print(f"Filtered data for the first 100 days: {filtered_data.count()} rows.")

    # Define database connection properties
    database_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
    db_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver"
    }

    # Write the filtered data to the database
    filtered_data.write.jdbc(url=database_url, table="sop_credit_transaction", mode="overwrite", properties=db_properties)
    print("Filtered data written to the database successfully.")

except FileNotFoundError as fnf_error:
    print(f"File not found: {fnf_error}")
except Exception as e:
    print(f"An error occurred: {e}")
