# send alert email if corduroy hat/corduroy pants/trail running pack deviate from daily trend

from pytrends.request import TrendReq
from pytrends import dailydata # https://pypi.org/project/pytrends/
import sendgrid
from sendgrid.helpers.mail import *

# pytrends = TrendReq(hl='en-US')

def get_interest_trends_kw(keyword, pytrends = TrendReq(hl='en-US')):
    kw=keyword
    # pytrends = TrendReq()
    pytrends.build_payload([kw], cat=0, timeframe='today 5-y', geo='US')
    df = pytrends.interest_over_time()
    df.reset_index(inplace=True)
    return df

def send_email(api_key, recipient_email, subject, body):
    sg = sendgrid.SendGridAPIClient(api_key=api_key)
    from_email = Email("your-email@example.com")
    to_email = To(recipient_email)
    content = Content("text/plain", body)
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    print(response.status_code)

# Define your condition here
condition_met = True  # Example condition being met

if condition_met:
    api_key = "YOUR_SENDGRID_API_KEY"
    recipient_email = "vanaltrades@gmail.com"
    subject = "Condition Met Notification"
    body = "The condition you set has been met."

    send_email(api_key, recipient_email, subject, body)
