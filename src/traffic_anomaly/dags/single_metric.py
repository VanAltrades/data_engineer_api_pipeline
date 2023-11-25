from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage
import os

from datetime import *
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import smtplib


logger = logging.getLogger("airflow.task")

ANOM_EMAIL_FROM = os.environ.get("ANOM_EMAIL_FROM")
ANOM_EMAIL_FROM_PW = os.environ.get("ANOM_EMAIL_FROM_PW")

# PRODUCTION
ANOM_EMAIL_TO = os.environ.get("ANOM_EMAIL_TO")

BUCKET_ANOMALY = 'anomaly_visualizations'

DATE_NAME = "date_key"
METRIC_NAME = "visits"
THRESHOLD_LOW = 0.25
THRESHOLD_HIGH = 0.75

CHART_TITLE = f"Google Analytics {METRIC_NAME.title()} Anomalies <br> Days Where {METRIC_NAME.title()} are Outside Day of Week's Quantile Threshold"
ANOMALY_IMAGE_NAME=f"ga_visits_{str(datetime.today().strftime('%Y-%m-%d'))}"
EMAIL_SUBJECT = "GA Visits Outside Day's Expected 0.25-0.75 Quantile Range"
IMG_PATH = f"storage.googleapis.com/{BUCKET_ANOMALY}/{ANOMALY_IMAGE_NAME}.png"

DAG_NAME = 'anomaly_ga_visits'
DAG_DESCRIPTION = 'Alert an email address if GA Visits fall outside 0.25-0.75 quantile.'

default_args = {
    'owner': 'Jack VanAltrades',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

SQL = """
      SELECT date_month, sum(visits) visits 
      FROM `e-commerce-demo-v.summary.s_ga_organic`
      WHERE date < "2017-12-31"
      GROUP BY 1,2
      ORDER BY 1 ASC
    """

with DAG(DAG_NAME,
         start_date=days_ago(1), 
         schedule_interval='0 15 * * *',  # Run at 9 am CST every day
         description=DAG_DESCRIPTION,
         tags=["bq", "gcs"],
         catchup=False, 
         max_active_runs=1,
         default_args=default_args         
) as dag:

  def get_data_from_bq(**kwargs):
      # Define the BigQuery connection ID
      bigquery_conn_id = 'bigquery_default'
      # Create a BigQuery hook
      bigquery_hook = BigQueryHook(bigquery_conn_id)
      # Execute the query and get the result as a Pandas DataFrame
      result = bigquery_hook.get_pandas_df(sql=SQL, dialect='standard')
      
      # Push the data to XCom
      kwargs['ti'].xcom_push(key='bq_data_key', value=result)

  t1 = PythonOperator(
      task_id='t1_get_data_from_bq',
      python_callable=get_data_from_bq,
      provide_context=True,
      dag=dag
  )

  def format_data(**kwargs):

    ti = kwargs['ti']
    bq_data = ti.xcom_pull(task_ids='t1_get_data_from_bq', key='bq_data_key')

    data = bq_data[[DATE_NAME,METRIC_NAME]].copy()
    data[DATE_NAME] = pd.to_datetime(data[DATE_NAME], errors='coerce')
    # add name of day column
    data.loc[:,'day_name'] = data[DATE_NAME].dt.day_name()
    data[METRIC_NAME] = data[METRIC_NAME].astype('int64')
    # set timestamp to index
    data.set_index(DATE_NAME, drop=True, inplace=True)
    kwargs['ti'].xcom_push(key='data_formated_key', value=data)

  t2 = PythonOperator(
      task_id='t2_format_data',
      python_callable=format_data,
      provide_context=True,
      dag=dag
  )

  def classify_anomaly(**kwargs):
    
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='t2_format_data', key='data_formated_key')

    # Set 15th Percentile threshold
    data['quantile_0.25'] = data.groupby('day_name')['promo_orders'].transform(lambda x: x.quantile(THRESHOLD_LOW))
    data['quantile_0.75'] = data.groupby('day_name')['promo_orders'].transform(lambda x: x.quantile(THRESHOLD_HIGH))
    
    # Define a function to set the 'anomaly' columns
    def set_anomaly_low(row):
        return (row[METRIC_NAME] < row['quantile_0.25'])
    data['anomaly_low'] = data.apply(set_anomaly_low, axis=1)
    
    def set_anomaly_high(row):
        return (row[METRIC_NAME] > row['quantile_0.75'])
    data['anomaly_high'] = data.apply(set_anomaly_high, axis=1)
    
    # Replace NaN metrics with 0
    data_anomaly = data.fillna(0)
    
    print(data_anomaly.head()) # LOG
    kwargs['ti'].xcom_push(key='data_anomaly_key', value=data_anomaly)

  t3 = PythonOperator(
      task_id='t3_classify_anomaly',        
      python_callable=classify_anomaly,
      provide_context=True,
      dag=dag
  )

  def day_is_anomaly(**kwargs):

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='t3_classify_anomaly', key='data_anomaly_key')

    if data[data.index == data.index.max()]["anomaly_low"][0] == True or data[data.index == data.index.max()]["anomaly_high"][0] == True:
      return True
    
  # ShortCircuitOperator to conditionally skip downstream tasks
  short_circuit_task = ShortCircuitOperator(
      task_id='short_circuit',
      python_callable=day_is_anomaly,
      provide_context=True,
      dag=dag,
  )

  def gcs_create_store_visualization(**kwargs):

    ti = kwargs['ti']
    data_anomaly = ti.xcom_pull(task_ids='t3_classify_anomaly', key='data_anomaly_key')

    fig = px.line(
            data_anomaly,
            x=data_anomaly.index,
            y=METRIC_NAME,
            title=CHART_TITLE,
            template = 'plotly_dark')
    
    # create list of outlier_dates below threshold
    outlier_dates_low = data_anomaly[data_anomaly['anomaly_low'] == True].index
    # obtain y metric of anomalies to plot
    y_metrics = [data_anomaly.loc[i][METRIC_NAME] for i in outlier_dates_low]
    fig.add_trace(
        go.Scatter(
            x=outlier_dates_low, 
            y=y_metrics, 
            mode = 'markers',
            name = 'anomaly',
            marker=dict(color='red',size=10)
            )
        )
    
    # create list of outlier_dates above threshold
    outlier_dates_high = data_anomaly[data_anomaly['anomaly_high'] == True].index
    # obtain y metric of anomalies to plot
    y_metrics = [data_anomaly.loc[i][METRIC_NAME] for i in outlier_dates_high]
    fig.add_trace(
        go.Scatter(
            x=outlier_dates_high, 
            y=y_metrics, 
            mode = 'markers',
            name = 'anomaly',
            marker=dict(color='green',size=10)
            )
        )
    
    fig.update_layout(title={'text': CHART_TITLE})

    # IMAGE
    img_bytes = fig.to_image(format="png")

    # HTML
    html_content = fig.to_html(full_html=False)

    storage_client = storage.Client()
    #Choose the matching buckets to upload the data to
    bucket = storage_client.get_bucket(BUCKET_ANOMALY)
    
    # Upload the data to the selected bucket
    blob = bucket.blob(f'{ANOMALY_IMAGE_NAME}.png')
    blob.upload_from_string(img_bytes,  content_type='image/png')
    blob = bucket.blob(f'{ANOMALY_IMAGE_NAME}.html')
    blob.upload_from_string(html_content, content_type='text/html')

  t5 = PythonOperator(
      task_id='t5_gcs_create_store_visualization',
      python_callable=gcs_create_store_visualization,
      provide_context=True,
      dag=dag
  )

  def send_alert():
    msg = MIMEMultipart('alternative')
    msg['From'] = ANOM_EMAIL_FROM
    msg['To'] = ANOM_EMAIL_TO
    msg['Subject'] = EMAIL_SUBJECT

    # Read directly from GCS and attach it to the email
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_ANOMALY)  # Replace with your GCS bucket name
    blob_html = bucket.blob(f'{ANOMALY_IMAGE_NAME}.html')
    blob_png = bucket.blob(f'{ANOMALY_IMAGE_NAME}.png')
    
    # NICE TO HAVE - creates a temporary public link for stakeholders to view data
    # signed_html_url = blob_html.generate_signed_url(expiration=3600)  # Expiration time in seconds (adjust as needed)
    # signed_png_url = blob_png.generate_signed_url(expiration=3600)
    
    # # Read the image content from GCS
    # html_content = blob_html.download_as_text()
    png_content = blob_png.download_as_bytes()

    # Create the body of the message (a plain-text and an HTML version).
    text = f"{EMAIL_SUBJECT}"
    html = f'''
      <html>
          <body>                              
                <img src="cid:{IMG_PATH}" alt="Embedded PNG link">
                <br/>
                <p>Path to Interactive Chart: 'gs://{BUCKET_ANOMALY}/{ANOMALY_IMAGE_NAME}.html'</p>
          </body>
      </html>    
    '''
    print(html[:100])


    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    msg.attach(part1)
    msg.attach(part2)

    img = MIMEImage(png_content)
    img.add_header('Content-ID', f'<{IMG_PATH}>')
    msg.attach(img)

    mailserver = smtplib.SMTP('smtp.gmail.com',587)
    # identify ourselves to smtp gmail client
    mailserver.ehlo()
    # secure our email with tls encryption
    mailserver.starttls()
    # re-identify ourselves as an encrypted connection
    mailserver.ehlo()
    mailserver.login(ANOM_EMAIL_FROM, ANOM_EMAIL_FROM_PW)

    mailserver.sendmail(ANOM_EMAIL_FROM,ANOM_EMAIL_TO,msg.as_string())
    # mailserver.sendmail(email_sender,email_receiver_personal,msg.as_string())

    mailserver.quit()

  t6 = PythonOperator(
      task_id='t6_send_alert',
      python_callable=send_alert,
      provide_context=True,
      dag=dag
  )


  # Dummy task to end the DAG if the condition is false
  end_dag_task = DummyOperator(
      task_id='end_dag',
      dag=dag,
  )

  ( 
    # Test
    # t1 >> t2 >> t3 >> t5 >> t6
    
    # Production
    t1 >> t2 >> t3 >> short_circuit_task 
    >> [t5, end_dag_task] # [recent day anomaly==True, recent day anomaly==False]
    >> t6
  )