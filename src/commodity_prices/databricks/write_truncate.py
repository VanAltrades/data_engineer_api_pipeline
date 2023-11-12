from pyspark.sql import SparkSession
import yfinance as yf
import datetime as dt

# Initialize Spark session
spark = SparkSession.builder.appName("YFinance to BigQuery").getOrCreate()

# Tickers list for data extraction from yahoo finance
tickers = ['BZ', 'EB', 'NG']

# Set start and end date ranges
today = dt.datetime.now()
start = dt.datetime(2022, 1, 1,)
end = dt.date(today.year, today.month, today.day)

# API call to download data from yahoo finance
d = yf.download(tickers=tickers, start=start, end=end, interval='1d',)

# format multi-column index dataframe to single column row
data = d.stack(level=1)
data = data.reset_index()

# format table
data.columns = ['Date','Ticker','Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume'] # rename columns to follow BigQuery specs
data = data.dropna(subset=['Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume'], how="all") # drop na records
data = data.sort_values(by="Date",ascending=False) # sort by date desc per preference
data = data[['Date','Ticker','Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume']]

# Convert the data to a PySpark DataFrame
df = spark.createDataFrame(data.reset_index())

# Write the data to BigQuery
PROJECT_ID="e-commerce-demo-v"
TABLE = "databricks.commodity_prices_wt_databricks"

BUCKET = "databricks-bq-2023"

# Define the table schema based on the DataFrame schema
schema = ",".join([f"{field.name} {field.dataType}" for field in df.schema.fields])

# Write to BigQuery
df.write.format("bigquery") \
    .option("temporaryGcsBucket", BUCKET) \
    .option("parentProject", PROJECT_ID) \
    .option("table", TABLE) \
    .option("schema", schema) \
    .mode("overwrite") \
    .save()
