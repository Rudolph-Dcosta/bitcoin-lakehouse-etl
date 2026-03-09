# Databricks notebook source
import requests
import json
from google.cloud import storage
from datetime import datetime
import os

VOLUME_KEY_PATH = "/Volumes/project_lakehouse/bronze_layer/credentials/gcp-key.json"
BUCKET_NAME = "data-lakehouse-bronze"

def run_ingestion():
    with open(VOLUME_KEY_PATH, "r") as f:
        key_content = f.read()

    client = storage.Client.from_service_account_info(json.loads(key_content))

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url, timeout=10)
    data = response.json()

    now = datetime.now()
    data['extraction_timestamp'] = now.isoformat()
    data['source_system'] = "databricks_automated_job"

    folder_path = now.strftime("raw/%Y/%m/%d")
    file_name = f'{folder_path}/btc_price_{now.strftime('%H%M%S')}.json'

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data=json.dumps(data), content_type='application_json')

    print(f'Successfully ingested: {file_name}')

if __name__ == "__main__":
    run_ingestion()