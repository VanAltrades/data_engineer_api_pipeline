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

# add below dependencies to cloud composer's environment
# via ui or...
# $ gcloud composer environments update {demo-environment} --location us-central1 --update-pypi-package yfinance>=0.2.31
import yfinance as yf
import pandas as pd

PROJECT_ID="e-commerce-demo-v"
STAGING_DATASET = "dag_examples"
TABLE = "commodity_prices" # existing table we want to append to
LOCATION = "us-central1"

default_args = {
    'owner': 'VanAltrades',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}


def extract_from_database():
    # Initialize the BigQuery client
    client = bigquery.Client()
    query_job=client.query("SELECT * FROM `e-commerce-demo-v.dag_examples.commodity_prices`") 
    results = query_job.result()
    # data_table = results.to_dataframe() # will cause TypeError: Object of type DataFrame is not JSON serializable

    # Convert Date column to strings to avoid AttributeError: 'datetime.date' object has no attribute '__module__'
    results['Date'] = results['Date'].dt.strftime('%Y-%m-%d')

    # Create a list to hold the data as dictionaries
    data_table_list = []
    
    for row in results:
        data_table_list.append(dict(row))
    
    return data_table_list

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
 
    data_y_finance['Date'] = data_y_finance['Date'].dt.strftime('%Y-%m-%d')
    
     # Return data_y_finance as a list of dictionaries
    data_y_finance_list = []
    for index, row in data_y_finance.iterrows():
        data_y_finance_list.append(row.to_dict())

    return data_y_finance_list

def identify_new_records(existing_table_records, yfinance_data_records):

    # convert data records back to dataframe for comparison
    existing_table = pd.DataFrame(existing_table_records)
    yfinance_data = pd.DataFrame(yfinance_data_records)

    # Merge the two DataFrames with the 'indicator' parameter
    merged = yfinance_data.merge(existing_table, how='left', indicator=True)

    # Filter only the rows that are in 'new_data' but not in 'existing_data'
    new_records = merged[merged['_merge'] == 'left_only']

    # Drop the '_merge' column if needed
    data_new = new_records.drop(columns=['_merge'])

    data_new_list = []
    for index, row in data_new.iterrows():
        data_new_list.append(row.to_dict())

    return data_new_list

def save_new_records_to_gcs(task_instance):
    # Retrieve the identified new records from XCom
    new_records_list = task_instance.xcom_pull(task_ids="identify_new_records_task")

    new_records = pd.DataFrame(new_records_list)

    # Convert the data to CSV and encode 
    data = new_records.to_csv(index=False).encode()

    # Create a storage client
    storage_client = storage.Client()

    # Get a list of all buckets
    buckets = list(storage_client.list_buckets())

    # Filter the list of buckets to only include those with the desired prefix
    buckets_with_prefix = [bucket for bucket in buckets if fnmatch.fnmatch(bucket.name, 'tmp_commodity_*')]

    #Choose the matching buckets to upload the data to
    bucket = buckets_with_prefix[0]

    # Upload the data to the selected bucket
    blob = bucket.blob('commodity_data_append.csv')
    blob.upload_from_string(data)
    print(f"data sucessfully uploaded to {bucket}")

with DAG('commodity_write_append',
         start_date=days_ago(1), 
         schedule_interval="@once",
        #  schedule='5 4 * * *', # run daily at 7 pm
         catchup=False, 
         default_args=default_args, 
         tags=["gcs", "bq"]
) as dag:

    # 1. Extract data from an existing database
    extract_from_db_task = PythonOperator(
        task_id="extract_from_db_task",
        python_callable=extract_from_database,
    )

    # 2. Extract data from Yahoo Finance
    extract_from_yfinance_task = PythonOperator(
        task_id="extract_from_yfinance_task",
        python_callable=extract_from_yfinance,
    )

    # 3. Identify new records
    identify_new_records_task = PythonOperator(
        task_id="identify_new_records_task",
        python_callable=identify_new_records,
        op_args=[  # Provide arguments for the comparison function
            "{{ task_instance.xcom_pull('extract_from_db_task') }}",  # Existing database data
            "{{ task_instance.xcom_pull('extract_from_yfinance_task') }}",  # Data from Yahoo Finance
        ],
    )

    # 4. Generate Unique ID For Storage Bucket
    generate_uuid = PythonOperator(
            task_id="generate_uuid", 
            python_callable=lambda: "tmp_commodity_" + str(uuid.uuid4()),
        )

    # 5. Create a Storage Bucket
    create_bucket = GCSCreateBucketOperator(
            task_id="create_bucket",
            bucket_name="{{ task_instance.xcom_pull('generate_uuid') }}", # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
            project_id=PROJECT_ID,
        )

    # 6. Save new records to GCS
    save_new_records_to_gcs_task  = PythonOperator(
        task_id = 'save_new_records_to_gcs_task',
        python_callable = save_new_records_to_gcs,
        )

    # 7. Load new records to GBQ
    load_to_bq = GCSToBigQueryOperator(
        task_id = 'load_to_bq',
        bucket = "{{ task_instance.xcom_pull('generate_uuid') }}",
        source_objects = ['commodity_data_append.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.{TABLE}',
        write_disposition='WRITE_APPEND',
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
        extract_from_db_task
        >> extract_from_yfinance_task
        >> identify_new_records_task
        >> generate_uuid
        >> create_bucket
        >> save_new_records_to_gcs_task
        >> load_to_bq
        >> delete_bucket
    )