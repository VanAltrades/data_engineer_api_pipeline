import os
from google.cloud import bigquery
from google.oauth2 import service_account



def get_gcp_credentials_path():
    # Get the current directory of the gcp_utils.py file
    current_dir = os.path.dirname(__file__)
    config_dir = os.path.join(current_dir, '..', '..', 'config')
    # config_dir = os.path.join(os.path.dirname(__file__), "...\\config")
    json_path = os.path.join(config_dir, "gcp_service_account.json")
    print(f"credential path: {json_path}")
    
    return json_path

key_path = get_gcp_credentials_path()

def return_client(key_path : str):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(credentials=credentials, project=credentials.project_id,)

client = return_client(key_path)