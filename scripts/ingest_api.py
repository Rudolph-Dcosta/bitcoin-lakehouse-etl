import requests
from google.cloud import storage
import json
from datetime import datetime
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
BUCKET_NAME = 'data-lakehouse-bronze'

def fetch_and_upload():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        now =datetime.now()
        data['extraction_timestamp'] = now.isoformat()
        data['source_system'] = "coingecko_api"
        
        folder_path = now.strftime("raw/%Y/%m/%d")
        file_name = f"{folder_path}/btc_price_{now.strftime('%H%M%S')}.json"

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_name)

        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )

        print(f'Success Ingested to gs://{BUCKET_NAME}/{file_name}')

    except requests.exceptions.RequestException as e:
        print(f'API Error: {e}')
    except Exception as e:
        print(f'Unexpected Error: {e}')

if __name__ == "__main__":
    fetch_and_upload()