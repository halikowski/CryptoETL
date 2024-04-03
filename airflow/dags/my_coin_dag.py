import requests
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Azure storage access keys
storage_conn_string = os.environ.get('CONN_STRING')
container_name = os.environ.get('CONTAINER_NAME')

# Logging setup
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_tickers():
    """ Ingests a new JSON file to Azure Blob Storage from the CoinPaprika API.
        Each file gets replaced with a new one after every API call."""
    now = datetime.now()

    # Azure connection setup
    blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container
    blob_list = container_client.list_blobs()

    # Delete each old blob in the container
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob.name)
        blob_client.delete_blob()
    
    # Create new blob
    blob_client = container_client.get_blob_client(blob=f'coin_paprika_data_{now}.json')

    # API call with exception handling
    url = 'https://api.coinpaprika.com/v1/tickers'
    try:
        response = requests.get(url=url)
        data = response.json()
        json_file = json.dumps(data)
        blob_client.upload_blob(json_file)

    except requests.RequestException as e:
        logging.error(f'Error during request: {e}')
        return None

    except (KeyError, IndexError) as e:
        logging.error(f'Error during extracting data from the response: {e}')
        return None


# Airflow DAG setup
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 3, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('my_coin_dag', default_args=default_args, schedule_interval='*/5 * * * *') as dag:
    run_script = PythonOperator(
        task_id='get_tickers',
        python_callable=get_tickers
    )