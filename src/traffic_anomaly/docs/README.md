# alert dag

Analysts are the domain experts of their organization's information. With an expertise in areas of their business to monitor and report upon, analysts should combine concise reporting with timely notices.

For example, an e-commerce analyst may be tasked with monitoring traffic levels to their website. In an ideal world traffic follows an expected distribution of traffic that grows linearly or exponentially overtime. But the unexpected nature of the market can see some days see traffic declines or increases that an analyst should be aware of and be able to explain in a timely manner. In addition, perhaps an unexpected change in traffic attribution or a site change completely breaks analytics and repoting. In this case, an analyst should be able to identify an anomalous change from an expected value and pursue a root cause identification and solution.

With many areas of an organization to monitor, spending a brief amount of time to configure an daily script to detect unexpected swings in key performance indicators becomes an invaluable tool in an analyst's toolkit.

Enter Airflow for anomaly detection. With a short setup, analysts can leverage batch processing to save large amounts of time and accurately assess a swing in KPIs in perpetuity.

https://github.com/VanAltrades/data_engineer_api_pipeline/tree/main/src/traffic_anomaly

This article will explain how analysts can plan and implement a cloud orchestrated anomaly alerting system that emails stakeholders when swings in expected values occur.

**Key Objective:**

Outfit your organization with daily anomaly detection alerts based on internal database data.

**Primary Benefits:**

Plan a pipeline's logic and translate the logic to a Python script.

Build and understand implementing multiple anomaly algorithms into an Aiflow script.

Learn Aiflow features including XCOMs, PythonOperators, ShortCircuitOperators, PythonBranchOperators and looping through variable task dependencies.


**Secondary Benefits:**

Alerts based on a single metric.

Alerts based on multiple metrics from a single source.

Using Airflow to interact with BigQuery and Google Cloud Storage.

Using Airflow to send emails with visualization attachments.

## Configure Airflow Environment in Apache Airflow

In a previous article, I covered how to setup an Airflow environment using Google Cloud Platform's Cloud Composer offering. Follow along or research yourself. [Setup Aiflow](https://medium.com/@vanaltrades/orchestrating-api-data-into-bigquery-using-airflow-the-analysts-guide-to-data-engineering-0bd6733b5d91)

Airflow environments in Google come with installed Python packages like pandas and the python core library. One requirement of this alert is the ability to include data visualizations. Be sure to navigate towards your Cloud Composer's PyPi installation page to install the plotly and kaleido packages to allow for rich visualizations. 

This example will send emails from a gmail account. There are other options to send emails like SendGrid, so feel free to augment this instruction with what works with you. To enable the ability to send emails from a gmail account, I followed [these instructions](https://www.letscodemore.com/blog/smtplib-smtpauthenticationerror-username-and-password-not-accepted/). You will be required to turn on Gmail 2 factor authentication, [create](https://myaccount.google.com/apppasswords) a new app password, and store the password in your environment variables.

Be sure to save your email app password. This password will be set as the email sender's password within the cloud composer's environment variables.

![save environment variables](image-1.png)

Finally, a Google Cloud Storage bucket will be used as a data store in the script so make a GCS bucket and use it's name as a variable in the coming script.

![make new gcs bucket](image-2.png)

## Logic Overview

Describe step by step process with XCom approach 

![single metric flow chart](image.png)

* declare output (check if most recent day is anomaly)

* declare variables and threshold for anomaly

* retrieve data from database

* format data

* label anomalous records

* determine if most recent day record is anomalous

* if most recent record is not anomalous then stop all processing

* if most recent record is anomalous then continue processing

* if most recent record is anomalous create a trend visualization that highlights anomalies

* if most recent record is anomalous store the visualization to a GCS bucket for email attachment

* if most recent record is anomalous send the visualization to email recipients

## Logic Approach #1: Quantile Boundary

what are quantile/percentiles and why use them. pros and cons.

## Creating a Single Metric Anomaly DAG

...

![single metric dag graph](image-4.png)

![email result](image-3.png)

## Scaling Your Alert to Check Multiple Metrics

Querying a cloud database will cost money. This means copying this working DAG to new files for each metric will charge your billing account for each query. So how can we query once and loop through a list of metrics to check within a single data source?

In order to productionalize this approach, analysts will need to apply Python looping functionality to dynamically create Airflow tasks with a metric's name in their task_ids.

## Logic Approach #2: Standard Deviation Boundary

what is a standard deviation and why use them. pros and cons.

## Creating a Multiple Metric Anomaly DAG

...

![multiple metric dag graph](image-5.png)

## Sandboxing

Send alerts to a sandbox email to confirm everything works and avoid innacurate notices. Start small with less data processing (data ranges) and scale up when everything looks good.

## Where to Next?

Try different anomaly detection algorithms.

Try weekly/monthly instead.

Expand alerting to breakout dimensions causing anomalies when an anomaly is detected.

-- Jack VanAltrades