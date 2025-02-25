ELT Pipeline with Airflow, Docker, GCS, and Snowflake

Overview
This project created an ELT pipeline (Extract, Load, Transform)  using Apache Airflow, Docker, Google Cloud Storage (GCS), and Snowflake. 
lets see how it works

1. Extract data from a PostgreSQL database.
2. Save the extracted data as a CSV file and upload it to Google Cloud Storage (GCS).
3. Load the data from GCS into Snowflake for further processing.

Project Structure

elt_pipeline_assessment/
│-- dags/
│   ├── elt_pipeline_local.py      
│-- containers/
│   │-- extract_upload_local/
│   │   ├── scripts/
│   │   │   ├── extract_upload_to_gcs.py    
│   │   │   ├── requirements.txt 
│   │   ├── Dockerfile     
│   │-- load_to_bq/
│   │   ├── scripts/
│   │   │   ├── load_to_bq.py    
│   │   │   ├── requirements.txt 
│   │   ├── Dockerfile     
│-- README.md   

Setup and Installation

Prerequisites
Make sure you have the following installed before getting started:

- Python 3.8+
- Docker
- Apache Airflow
- Google Cloud SDK (for GCS)
- A Snowflake account
- PostgreSQL database

Installing Airflow
You need to have Airflow installed and running. Run the following commands:

pip install apache-airflow
airflow db init
airflow webserver --port 8080
airflow scheduler

Once start running, access the Airflow UI at http://localhost:8080.
Set up PostgreSQL and Snowflake connections under the section Admin > Connections.

Setting Up Google Cloud Storage (GCS)

Authenticate with GCS
Run this command to log in:

gcloud auth application-default login

Create a GCS Bucket
Set up a storage bucket to hold your extracted data:

gsutil mb gs://my-elt-bucket/

Setting Up Snowflake

Create a Snowflake Table
Run this query in Snowflake:

CREATE TABLE transactions (
    id INT,
    amount FLOAT,
    transaction_date TIMESTAMP
);

Grant Permissions
Make sure the role has the necessary access:

GRANT INSERT ON transactions TO ROLE my_role;

Why Use DAGs and Containers?

Using DAGs in Airflow and running tasks in containers makes the workflow efficient and scalable. Here's why:

- Scalability: Each task runs independently in an isolated environment.
- Reproducibility: Containers ensure consistency across different setups.
- Flexibility: DAGs allow better scheduling and dependency management.
- Cloud Integration: Easily connects with cloud storage and computing services.

Running everything as a single script makes debugging and scaling much harder.

Building and Running the Docker Container

Build the Docker Image

docker build -t elt_pipeline .

Run the Container

docker run --rm elt_pipeline

Deploying the DAG in Airflow

Copy the DAG script (elt_pipeline.py) into Airflow’s DAG folder:

cp dags/elt_pipeline.py $AIRFLOW_HOME/dags/

Restart the Airflow scheduler:

airflow scheduler

Then, open the Airflow UI and enable the DAG.

How the Pipeline Works

1. Extract Data: Pulls data from PostgreSQL and saves it as a CSV.
2. Upload to GCS: Moves the CSV file to Google Cloud Storage.
3. Load into Snowflake: Imports the data from GCS into Snowflake.

Running the Pipeline Manually (Without Airflow)

To run the process manually, execute:

python container/scripts/entrypoint.py

Troubleshooting

Airflow UI Not Opening?
Restart the webserver:

airflow webserver --port 8080

Issues with Docker Build?
Try clearing the Docker cache:

docker system prune -a

Snowflake Permission Errors?
Ensure the role has the correct permissions:

GRANT INSERT ON transactions TO ROLE my_role;

Future Improvements

- Add data validation before loading into Snowflake.
- Integrate dbt (Data Build Tool) for transformations.
- we can Implement kubernets pod for dag and also CI/CD for automated deployments.

Contact
For any questions or assistance, feel free to reach out!
EMAIL: sasi.kyadavsasi@gmail.com
CONTACT : 7904912425

