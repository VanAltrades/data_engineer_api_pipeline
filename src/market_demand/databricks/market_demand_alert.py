# send alert email if corduroy hat/corduroy pants/trail running pack deviate from daily trend
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

import pandas as pd
from pytrends.request import TrendReq
from pytrends import dailydata # https://pypi.org/project/pytrends/
import sendgrid
from sendgrid.helpers.mail import *

api_key=""

def get_anomalous_records_std(data, date_key : str, metric,std=2):

    data[date_key] = pd.to_datetime(data[date_key])

    # Filter and sort the DataFrame
    data = data[[date_key, metric]].sort_values(by=date_key, ascending=False)

    # Set timestamp as index
    data.set_index(date_key, inplace=True)

    # Add a column for the day name
    data['day_name'] = data.index.day_name()

    # Calculate mean and standard deviation for each day of the week
    grouped = data.groupby('day_name')
    data['average_day_metric'] = grouped[metric].transform('mean')
    data['average_day_metric_std'] = grouped[metric].transform('std')

    # Calculate the lower and upper bounds for anomalies
    data['average_day_metric_std_min_bound'] = data['average_day_metric'] - (std * data['average_day_metric_std'])
    data['average_day_metric_std_max_bound'] = data['average_day_metric'] + (std * data['average_day_metric_std'])

    # Define a function to set the 'anomaly' column
    def set_anomaly(row):
        return (row[metric] > row['average_day_metric_std_max_bound']) or (row[metric] < row['average_day_metric_std_min_bound'])

    # Add an 'anomaly' column based on the defined function
    data['anomaly'] = data.apply(set_anomaly, axis=1)

    # Replace NaN metrics with 0
    data_anomaly = data.fillna(0)

    return data_anomaly


def get_anomaly_trend(df,metric):

    fig = px.line(
            df,
            x=df.index,
            y=metric,
            title=f"{metric.title()} Anomalies",
            template = 'plotly_dark')
    # create list of outlier_dates
    outlier_dates = df[df['anomaly'] == True].index
    # obtain y metric of anomalies to plot
    y_metrics = [df.loc[i][metric] for i in outlier_dates]
    fig.add_trace(
        go.Scatter(
            x=outlier_dates, 
            y=y_metrics, 
            mode = 'markers',
            name = 'anomaly',
            marker=dict(color='red',size=10)
            )
        )
    image_bytes = fig.to_image(format="png")

    # Render the Matplotlib figure
    plt.figure(figsize=(8, 6))
    plt.imshow(fig)
    plt.axis('off')  # Hide axes if needed

def get_interest_trends_kw(keyword, pytrends = TrendReq(hl='en-US')):
    kw=keyword
    # pytrends = TrendReq()
    pytrends.build_payload([kw], cat=0, timeframe='today 5-y', geo='US')
    df = pytrends.interest_over_time()
    df.reset_index(inplace=True)
    return df

def send_email(api_key, recipient_email, subject, body):
    sg = sendgrid.SendGridAPIClient(api_key=api_key)

    # AI
    from_email = Email("your-email@example.com")
    to_email = To(recipient_email)
    content = Content("text/plain", body)
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    print(response.status_code)

    # SENDGRID
    # message = sendgrid.Mail()

    # message.add_to("test@sendgrid.com")
    # message.set_from("you@youremail.com")
    # message.set_subject("Sending with SendGrid is Fun")
    # message.set_html("and easy to do anywhere, even with Python")
    # sg.send(message)

# Define your condition here
condition_met = True  # Example condition being met

if condition_met:
    api_key = "YOUR_SENDGRID_API_KEY"
    recipient_email = "vanaltrades@gmail.com"
    subject = "Condition Met Notification"
    body = "The condition you set has been met."

    send_email(api_key, recipient_email, subject, body)