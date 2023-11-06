from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator
import uuid
from datetime import timedelta
import datetime as dt
from airflow.utils.dates import days_ago
import fnmatch
from google.cloud import storage
from google.cloud import bigquery
import io

# add below dependencies to cloud composer's environment
# via ui or...
# $ gcloud composer environments update {demo-environment} --location us-central1 --update-pypi-package yfinance>=0.2.31
import yfinance as yf
import pandas as pd

PROJECT_ID="e-commerce-demo-v"
STAGING_DATASET = "dag_examples"
TABLE = "commodity_prices" # existing table we want to append to
LOCATION = "us-central1"

# Initialize the BigQuery client
client = bigquery.Client()


default_args = {
    'owner': 'VanAltrades',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

def extract_from_database():

    query_job=client.query("SELECT * FROM `e-commerce-demo-v.dag_examples.commodity_prices`") 
    results = query_job.result()
    df = results.to_dataframe()

    # Convert the data to CSV and encode 
    data = df.to_csv(index=False).encode()

    # Create a storage client
    storage_client = storage.Client()

    # Get a list of all buckets
    buckets = list(storage_client.list_buckets())

    # Filter the list of buckets to only include those with the desired prefix
    buckets_with_prefix = [bucket for bucket in buckets if fnmatch.fnmatch(bucket.name, 'tmp_commodity_*')]

    #Choose the matching buckets to upload the data to
    bucket = buckets_with_prefix[0]

    # Upload the data to the selected bucket
    blob = bucket.blob('commodity_data_existing_bq.csv')
    blob.upload_from_string(data)
    print(f"data sucessfully uploaded to {bucket}")

def extract_from_yfinance():
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
    data_y_finance = data[['Date','Ticker','Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume']]
 
    # Convert the data to CSV and encode 
    data = data_y_finance.to_csv(index=False).encode()

    # Create a storage client
    storage_client = storage.Client()

    # Get a list of all buckets
    buckets = list(storage_client.list_buckets())

    # Filter the list of buckets to only include those with the desired prefix
    buckets_with_prefix = [bucket for bucket in buckets if fnmatch.fnmatch(bucket.name, 'tmp_commodity_*')]

    #Choose the matching buckets to upload the data to
    bucket = buckets_with_prefix[0]

    # Upload the data to the selected bucket
    blob = bucket.blob('commodity_data_new_yfinance.csv')
    blob.upload_from_string(data)
    print(f"data sucessfully uploaded to {bucket}")


# Define a function to identify unique records between two DataFrames
def identify_unique_records(**kwargs): # task_instance, **kwargs
    
    gcs_file_path1 = 'commodity_data_existing_bq.csv'
    gcs_file_path2 = 'commodity_data_new_yfinance.csv'

    storage_client = storage.Client()

    # use uuid of created bucket in generate_uuid task
    ti = kwargs['ti']
    bucket_name = f"{ti.xcom_pull(task_ids='generate_uuid')}"
    
    # Fetch the data from Google Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)

    print(bucket)
    print(gcs_file_path1)
    print(gcs_file_path2)

    # file 1: exsiting bq
    blob1 = bucket.blob(gcs_file_path1)
    file1 = pd.read_csv(blob1.open())
    
    # file 2: new yfinance
    blob2 = bucket.blob(gcs_file_path2)
    file2 = pd.read_csv(blob2.open())

    # Identify unique records by performing set operations
    unique_records = pd.concat([file1, file2]).drop_duplicates(keep=False)

    # Save unique_records to a new CSV file
    unique_records = unique_records.to_csv(index=False, header=True).encode()

    # Upload the data to the selected bucket
    blob = bucket.blob('commodity_data_new.csv')
    blob.upload_from_string(unique_records)
    print(f"data sucessfully uploaded to {bucket}")

with DAG('commodity_write_append',
         start_date=days_ago(1), 
         schedule_interval="@once",
        #  schedule='5 4 * * *', # run daily at 7 pm
         catchup=False, 
         default_args=default_args, 
         tags=["gcs", "bq"]
) as dag:

    # 1. Generate Unique ID For Storage Bucket
    generate_uuid = PythonOperator(
            task_id="generate_uuid", 
            python_callable=lambda: "tmp_commodity_" + str(uuid.uuid4()),
            provide_context=True,
        )

    # 2. Create a Storage Bucket
    create_bucket = GCSCreateBucketOperator(
            task_id="create_bucket",
            bucket_name="{{ task_instance.xcom_pull('generate_uuid') }}", # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
            project_id=PROJECT_ID,
        )


    # 3. Extract data from an existing database to GCS
    extract_from_db_task = PythonOperator(
        task_id="extract_from_db_task",
        python_callable=extract_from_database,
    )

    # 4. Extract data from Yahoo Finance to GCS
    extract_from_yfinance_task = PythonOperator(
        task_id="extract_from_yfinance_task",
        python_callable=extract_from_yfinance,
    )

    # Define the task to identify unique records
    identify_unique = PythonOperator(
        task_id='identify_unique_records',
        python_callable=identify_unique_records,
        provide_context=True,
        dag=dag,
    )

    # 7. Load new records to GBQ
    load_to_bq = GCSToBigQueryOperator(
        task_id = 'load_to_bq',
        bucket = "{{ task_instance.xcom_pull('generate_uuid') }}",
        source_objects = ['commodity_data_new.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.{TABLE}',
        write_disposition='WRITE_APPEND', # We are appending to an existing database in this example
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'Date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'Ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Adj_Close', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'Close', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'High', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'Low', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'Open', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'Volume', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            ],
        )
    
    delete_bucket = GCSDeleteBucketOperator(
            task_id="delete_bucket",
            bucket_name="{{ task_instance.xcom_pull('generate_uuid') }}",
        )

    (
        generate_uuid
        >> create_bucket
        >> extract_from_db_task
        >> extract_from_yfinance_task
        >> identify_unique
        >> load_to_bq
        >> delete_bucket
    )