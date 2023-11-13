# echo "export SENDGRID_API_KEY='xxxx'" > sendgrid.env
# echo "sendgrid.env" >> .gitignore
# source ./sendgrid.env

# API Key needs "Full Access" (Settings > API Keys > Edit API Key)

import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email='yy@gmail.com',
    to_emails='xx@gmail.com',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e.message)