from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

def verify_connection():
    try:
        client = storage.Client()
        bucket = client.get_bucket('data-lakehouse-bronze')
        print(f'Success Connected to bucket: {bucket.name}')

        blobs = list(bucket.list_blobs())
        print(f"Found {len(blobs)} files in bucket")

    except Exception as e:
        print(f'Connection Failed: {e}')

if __name__ == "__main__":
    verify_connection()