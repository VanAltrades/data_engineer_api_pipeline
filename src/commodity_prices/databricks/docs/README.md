# Build a Databricks API Pipeline to BigQuery on GCP
## All your data, analytics and AI on one Lakehouse platform

![flow](image-14.png)

1. Google Cloud Platform (GCP) Service Account:
Create a Service Account: In the Google Cloud Console, navigate to the IAM & Admin section. Create a service account if you haven't already.

Assign Roles: Assign the necessary roles to the service account, such as BigQuery Data Editor or BigQuery Admin, depending on the level of access needed.

Create a JSON Key File: Generate a JSON key for the service account. This file contains the credentials needed to access BigQuery from your Databricks environment.

2. Subscribe to Databricks through GCP Marketplace

Free tier 14 day trial offering:

Features:

* Managed Apache Spark: Yes
* Optimized Delta Lake: Yes
* Cluster Autopilot: Yes
* Jobs Scheduling & Workflow: Yes
* Notebooks & Collaboration: Yes
* Databricks Runtime for ML: Yes
* Optimized Runtime Engine: Yes
* Administration Console: Yes
* Single Sign-On (SSO): Yes
* Role-based Access Control: Yes
* Token Management API: Yes

![order request to databricks](image.png)

Sign up via the Databricks link and you will receive a GCP email with the following title: "Your Databricks trial has now started"

This email will link you towards documentation for [Databricks on GCP](https://docs.gcp.databricks.com/).

Click the Manage on Provider link where you specify your subscription plan.

Create a workspace in the Databricks UI
![create a workspace](image-1.png)

Note: As part of creating this workspace, two Google Cloud Storage buckets will be created in your GCP project. These buckets will host the data that is put in the external DBFS storage and internal DBFS storage, respectively. Please review the access controls on these buckets in the GCP console. Read more

![databricks gcs buckets](image-2.png)

Refresh and note that the workspace will take a moment to be provisioned.

Next, we need to grant an existing GCP service account access to Databricks so we can read and write to the BigQuery service.

https://cloud.google.com/bigquery/docs/connect-databricks

And then create a temporary storage cloud storage bucket - in this case `databricks-bq-2023`. 

![create gcs bucket](image-3.png)

Add Service Account to gcs under Permissions page

![sa grant access](image-4.png)

Add Databricks cluster with service account

![service account addition under advanced options](image-5.png)

Add yfinance python package to cluster library

![yfinance install](image-6.png)

Add new notebook under Workspace tab.

## Test GCP Connection on Existing Table

![test access by reading existing bq table](image-7.png)

## Import Required Packages and Initialize Spark Session

![import and init](image-8.png)

## Extract Data from yFinance API

![api data extract](image-9.png)

## Transform the Data

![data transform](image-10.png)

## Load the Data to BigQuery

![load to bq](image-11.png)

![confirmation of overwrite table load](image-12.png)

## Schedule the ETL Pipeline 

![schedule](image-13.png)


