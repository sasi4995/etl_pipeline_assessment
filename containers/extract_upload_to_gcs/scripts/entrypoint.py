import os
import pandas as pd
import psycopg2
from google.cloud import storage
from dotenv import load_dotenv


load_dotenv()


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_TABLE = os.getenv("DB_TABLE", "source_transactions")


GCS_BUCKET = os.getenv("GCS_BUCKET", "etl-pipeline-assessment")
GCS_FILENAME = "transactions.csv"


ENV = os.getenv("ENV", "dev")

def extract_data():
    """Extract data from PostgreSQL and save it as a CSV file."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    query = f"SELECT * FROM {DB_TABLE}"
    df = pd.read_sql(query, conn)
    conn.close()

    local_filepath = "/tmp/transactions.csv"
    df.to_csv(local_filepath, index=False)
    print(f"Data extracted and saved to {local_filepath}")

    return local_filepath

def upload_to_gcs(local_filepath):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILENAME)
    blob.upload_from_filename(local_filepath)
    print(f"File uploaded to GCS: gs://{GCS_BUCKET}/{GCS_FILENAME}")

def main():
    if ENV == "dev":
        print("Running in development environment.")
    elif ENV == "prod":
        print("Running in production environment.")
    else:
        print("Unknown environment.")

    filepath = extract_data()
    upload_to_gcs(filepath)

if __name__ == "__main__":
    main()
