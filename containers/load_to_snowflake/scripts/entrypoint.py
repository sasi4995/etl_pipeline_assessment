import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
TABLE_NAME = os.getenv("TABLE_NAME", "transactions")

GCS_BUCKET = os.getenv("GCS_BUCKET", "etl-pipeline-assessment")
GCS_FILENAME = os.getenv("GCS_FILENAME", "transactions.csv")
ENV = os.getenv("ENV", "dev")

def load_data_to_snowflake():

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()

        query = f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}
            FROM '@~/{GCS_FILENAME}'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """
        cursor.execute(query)
        conn.commit()
        print(f"✅ Data loaded into Snowflake: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}")

    except Exception as e:
        print(f"❌ Error loading data to Snowflake: {e}")

    finally:
        cursor.close()
        conn.close()


def main():
    if ENV == "dev":
        print("Running in development environment.")
    elif ENV == "prod":
        print("Running in production environment.")
    else:
        print("Unknown environment.")

    load_data_to_snowflake()


if __name__ == "__main__":
    main()
