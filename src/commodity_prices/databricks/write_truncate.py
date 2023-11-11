from pyspark.sql import SparkSession
import yfinance as yf

# Initialize Spark session
spark = SparkSession.builder.appName("YFinance to BigQuery").getOrCreate()

# Set up the Yahoo Finance API to retrieve data
ticker = "AAPL"  # Example stock ticker
data = yf.download(ticker, start="2023-01-01", end="2023-11-01")

# Convert the data to a PySpark DataFrame
df = spark.createDataFrame(data.reset_index())

# Write the data to BigQuery
project_id = "your_project_id"
dataset_id = "your_dataset_id"
table_id = "your_table_id"

# Define the table schema based on the DataFrame schema
schema = ",".join([f"{field.name} {field.dataType}" for field in df.schema.fields])

# Write to BigQuery
df.write.format("bigquery") \
    .option("temporaryGcsBucket", "your_bucket_name") \
    .option("parentProject", project_id) \
    .option("table", f"{project_id}.{dataset_id}.{table_id}") \
    .option("schema", schema) \
    .mode("overwrite") \
    .save()
