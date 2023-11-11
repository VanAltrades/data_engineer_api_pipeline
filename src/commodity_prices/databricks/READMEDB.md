1. Google Cloud Platform (GCP) Service Account:
Create a Service Account: In the Google Cloud Console, navigate to the IAM & Admin section. Create a service account if you haven't already.

Assign Roles: Assign the necessary roles to the service account, such as BigQuery Data Editor or BigQuery Admin, depending on the level of access needed.

Create a JSON Key File: Generate a JSON key for the service account. This file contains the credentials needed to access BigQuery from your Databricks environment.

2. Databricks Configuration:
Upload JSON Key to Databricks: Upload the JSON key file containing your GCP service account credentials to Databricks. You can do this in the "Secrets" or "Data" tab of the Databricks workspace.

Create a Databricks Secret Scope: If you haven't done so, create a secret scope in Databricks. This is where you'll store the key or the path to the uploaded JSON key.

Set Up Databricks Secret Keys: In Databricks, use the Databricks CLI or the Databricks UI to set secret keys that reference the uploaded JSON key. For instance, you might name it gcp-key in your secret scope.

3. Accessing BigQuery in PySpark:
In your PySpark script, you can use the following code to read the credentials and access BigQuery:

```from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BigQueryAccess").getOrCreate()

project_id = "your_project_id"
dataset_id = "your_dataset_id"
table_id = "your_table_id"

spark.conf.set("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
spark.conf.set("gcpBillingProject", project_id)

df = spark.read.format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.{table_id}") \
    .load()
```

1. Create a Notebook:
Create a new notebook containing the PySpark code for accessing the Yahoo Finance API and loading the data into BigQuery.
2. Schedule the Notebook Execution:
Within Databricks, you can schedule the notebook to run daily by using the Jobs feature.