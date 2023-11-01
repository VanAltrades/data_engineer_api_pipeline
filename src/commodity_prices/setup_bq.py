# $ cd "directory/above/data_engineer_api"
# $ python -m data_engineer_api_pipeline.src.commodity_prices.setup_bq

import yfinance as yf
from ..utils.gcp_utils import client, bigquery
from google.cloud.exceptions import NotFound


# Get the tickers data for AAPL and MSFT
tickers = ['BZ', 'EB', 'NG']
data = yf.download(tickers)

# format multi-column index dataframe to single column row
d = data.stack(level=1)
d = d.reset_index()

# format table
d.columns = ['Date','Ticker','Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume'] # rename columns to follow BigQuery specs
d = d.dropna(subset=['Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume'], how="all") # drop na records
d = d.sort_values(by="Date",ascending=False) # sort by date desc per preference

# remove recent day (to see if dag can backfill)
d = d.loc[d['Date']!="2023-10-31"]


# send backfill dataframe to a new BigQuery table
project_id = "e-commerce-demo-v"
dataset_id = "dag_examples"
table_id = "commodity_prices"

dataset_ref = client.dataset(dataset_id)

try:
    client.get_dataset(dataset_ref)
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)  # Create the dataset

# Date	Ticker	Adj_Close	Close	High	Low	Open	Volume

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Adj_Close", "FLOAT64"),
        bigquery.SchemaField("Close", "FLOAT64"),
        bigquery.SchemaField("High", "FLOAT64"),
        bigquery.SchemaField("Low", "FLOAT64"),
        bigquery.SchemaField("Open", "FLOAT64"),
        bigquery.SchemaField("Volume", "FLOAT64"),
    ],
)

table_ref = client.dataset(dataset_id).table(table_id)
job = client.load_table_from_dataframe(d, table_ref, job_config=job_config)
job.result()  # Wait for the job to complete
