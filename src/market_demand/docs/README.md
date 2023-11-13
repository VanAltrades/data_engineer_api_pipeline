Steps to Send Email Notifications in Databricks:

**Set Up an Email Service:**

You can use an email service provider like SendGrid or AWS SES to send emails. Set up an account with one of these services and get the necessary API keys or credentials.

![sendgrid marketplace](image.png)

![new sub](image-1.png)

```
Your new Twilio SendGrid account has been created.

Please refresh the GCP page and click 'MANAGE ON PROVIDER'
```

**Create Identity**

![Alt text](image-2.png)

And verify sender via email link.

**Create API Key**

![settings > api keys](image-3.png)

![add api to databricks secrets](image-4.png)

You should never hard code secrets or store them in plain text. Use the Secrets API 2.0 to manage secrets in the Databricks CLI. Use the Secrets utility (dbutils.secrets) to reference secrets in notebooks and jobs.


**Install Required Libraries**


```
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import pandas as pd
from pytrends.request import TrendReq
import sendgrid
from sendgrid.helpers.mail import *
```

![install from compute > libraries](image-5.png)

![installed libraries](image-6.png)

2. Python Script in Databricks:
Write a Python script within a Databricks notebook to send an email based on a specific condition. Here's a sample code using SendGrid:

![weekly google trends dataframe](image-9.png)

![examining threshold to trigger alerts](image-10.png)

![anomaly logic dataframe](image-11.png)

![weekly keyword anomaly trend](image-7.png)

![schedule weekly](image-8.png)

![sendgrid POST confirmation](image-12.png)