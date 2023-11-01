import yfinance as yf
import pandas as pd
import requests,os
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'vanaltrades',
}


# covid_data_path = '/home/airflow/gcs/data/covid_data.csv'
# transform_data_output_path = '/home/airflow/gcs/data/transform_data.csv'
tickers = ["ZS","EB","NG"]

def get_yfinance_data(tickers):
    data = pd.DataFrame
    for ticker in tickers:
        df = yf.download(ticker, period='1y')
        df.reset_index(inplace=True)

        df['Date'] = pd.to_datetime(df.Date, format='%Y-%m-%d')
        df['Ticker'] = ticker

        data = data.append(df,ignore_index=True)
    return data

def get_commodity_db():
    return

def update_new_records():
    return

def load_to_bq():
    return

def get_covid19_report_today(covid_data_path):
    url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-line-lists'
    response = requests.get(url)
    decoded_content = response.content.decode('utf-8')
    with open(covid_data_path, 'w', encoding='utf-8') as f:
        f.write(decoded_content)
    print(f"Output to {covid_data_path}")

def transform_covid_data(covid_data_path, transform_data_output_path):
    df = pd.read_csv(covid_data_path)
    df = df.drop(['age_range', 'patient_type_map'], axis=1)

    patient_map = {'1.ผู้ป่วย PUI': 1, '2.สัมผัสผู้ติดเชื้อ': 2, '3.ต่างชาติมาจากต่างประเทศ': 3, '4.คนไทยมาจากต่างประเทศ': 4, '5.ลักลอบเข้าประเทศ': 5, '6.บุคลากรทางการแพทย์': 6,
                   '7.เฝ้าระวัง ARI/pneumonia': 7, '8.สำรวจกลุ่มเสี่ยง (survey)': 8, 9: '9.ขอตรวจหาเชื้อเอง', '10.อื่นๆ': 10, '11.เฝ้าระวังกลุ่มเสี่ยง (sentinel)': 11}
    df = df.replace({'patient_type': patient_map})
    df.to_csv(transform_data_output_path, encoding='utf-8',index=False)
    print(f"Output to {transform_data_output_path}")


with DAG(
    'covid_workflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['covid','workshop']
) as dag:
    dag.doc_md = "Sample Workflow to extract covid data and load to data big query (datawarehouse)"

    t1 = PythonOperator(
        task_id='get_data_from_api',
        # python_callable=get_covid19_report_today,
        python_callable=get_yfinance_data,
        op_kwargs={'covid_data_path': covid_data_path},
    )
    
    # t2 = PythonOperator(
    #     task_id='transform_data',
    #     python_callable=transform_covid_data,
    #     op_kwargs={'covid_data_path': covid_data_path,
    #             'transform_data_output_path': transform_data_output_path},
    # )
    t2 = BigQueryGetDataOperator(
        task_id='read_bigquery_data',
        sql='SELECT * FROM `your_project.your_dataset.your_table`',
        location='us-central1',  # Set the appropriate location
        dag=dag,
    )

    t3 = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='us-central1-testworkshop4-bd3b2d1d-bucket',
        source_objects=['data/transform_data.csv'],
        field_delimiter =',',
        destination_project_dataset_table='article_medium.covid_data',
        skip_leading_rows=1,
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'date',
                'type': 'DATE'
            },
            {
                'mode': 'NULLABLE',
                'name': 'ticker',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'price',
                'type': 'FLOAT'
            },
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    t4 = EmailOperator(
        task_id='send_email',
        to=['vanaltrades@gmail.com'],
        subject='Today\'s commodity price data loaded to BigQuery',
        html_content='{project}.{dataset}.{table} for more information'
    )

    t1 >> t2 >> t3 >> t4