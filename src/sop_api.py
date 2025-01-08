import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MiniPrjPython") \
    .config("spark.master", "local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Fetch data from API
api_url = "https://global-electricity-production.p.rapidapi.com/country?country=Afghanistan"
headers = {
    "x-rapidapi-key": "48bce046c0mshee45259ba8a9955p1a871bjsn90680f9cecd6",
    "x-rapidapi-host": "global-electricity-production.p.rapidapi.com"
}
response = requests.get(api_url, headers=headers)
api_data = response.text

# Define the schema of the API response
schema = StructType([
    StructField("country", StringType(), True),
    StructField("code", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("coal", DoubleType(), True),
    StructField("gas", DoubleType(), True),
    StructField("hydro", DoubleType(), True),
    StructField("other_renewables", DoubleType(), True),
    StructField("solar", DoubleType(), True),
    StructField("oil", DoubleType(), True),
    StructField("wind", DoubleType(), True),
    StructField("nuclear", DoubleType(), True)
])

# Parse JSON and create a DataFrame
data = [json.loads(api_data)]
df = spark.createDataFrame(data, schema)

# Print schema and data
df.printSchema()
df.show(10)

# Add transformations or calculations
transformed_df = df \
    .withColumn("total_renewables", col("hydro") + col("other_renewables") + col("solar") + col("wind")) \
    .withColumn("renewable_percentage", expr("(total_renewables / (coal + gas + oil + nuclear + total_renewables)) * 100"))

transformed_df.show(10)

# Sort the DataFrame by year
sorted_df = transformed_df.orderBy("year")
sorted_df.show(10)

# Write data to PostgreSQL
jdbc_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
db_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}
sorted_df.write \
    .mode("overwrite") \
    .jdbc(jdbc_url, "public.sop_electricity_data", properties=db_properties)

print("Data loaded to PostgreSQL")