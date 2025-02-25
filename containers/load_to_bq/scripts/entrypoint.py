import os
from google.cloud import bigquery, storage

# Load environment variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
DATASET_ID = os.getenv("BQ_DATASET", "your_dataset")
TABLE_ID = os.getenv("BQ_TABLE", "your_table")
GCS_BUCKET = os.getenv("GCS_BUCKET", "etl-pipeline-assessment")
GCS_FILENAME = os.getenv("GCS_FILENAME", "transactions.csv")
GCS_LOCAL_PATH = os.getenv("GCS_LOCAL_PATH", "/tmp/transactions.csv")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcs-key.json")
ENV = os.getenv("ENV", "dev")

# Set Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS
print(f"Using service account key from: {GOOGLE_CREDENTIALS}")

def download_from_gcs():
    """Download file from GCS to local storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILENAME)
    blob.download_to_filename(GCS_LOCAL_PATH)
    print(f"File downloaded from GCS: gs://{GCS_BUCKET}/{GCS_FILENAME} -> {GCS_LOCAL_PATH}")

def load_data_into_bigquery():
    """Load CSV file from local storage into BigQuery."""
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open(GCS_LOCAL_PATH, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    load_job.result()
    print(f"Data successfully loaded into BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

def main():
    if ENV == "dev":
        print("Running in development environment.")
    elif ENV == "prod":
        print("Running in production environment.")
    else:
        print("Unknown environment.")

    download_from_gcs()
    load_data_into_bigquery()

if __name__ == "__main__":
    main()
