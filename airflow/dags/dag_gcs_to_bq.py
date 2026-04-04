import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
RAW_DATASET = "olist_raw"

def get_clients():
    key_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(key_json)
    )
    bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    gcs_client = storage.Client(credentials=credentials)
    return bq_client, gcs_client

def load_gcs_to_bq(gcs_uri: str, table_id: str, bq_client):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()
    print(f"✓ Loaded {gcs_uri} → {table_id}")

def load_partitioned_table(table_name: str, execution_date: str):
    bq_client, gcs_client = get_clients()
    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=f"raw/{table_name}/")
    for blob in blobs:
        if execution_date in blob.name:
            gcs_uri = f"gs://{BUCKET_NAME}/{blob.name}"
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"
            load_gcs_to_bq(gcs_uri, table_id, bq_client)

def load_static_table(table_name: str):
    bq_client, _ = get_clients()
    gcs_uri = f"gs://{BUCKET_NAME}/raw/static/{table_name}.parquet"
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"
    load_gcs_to_bq(gcs_uri, table_id, bq_client)

with DAG(
    dag_id="dag_gcs_to_bq",
    start_date=datetime(2017, 1, 1),
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["olist", "ingestion"]
) as dag:

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_partitioned_table,
        op_kwargs={
            "table_name": "orders",
            "execution_date": "{{ ds[:7] }}"
        }
    )

    load_order_reviews = PythonOperator(
        task_id="load_order_reviews",
        python_callable=load_partitioned_table,
        op_kwargs={
            "table_name": "order_reviews",
            "execution_date": "{{ ds[:7] }}"
        }
    )

    load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_static_table,
        op_kwargs={"table_name": "order_items"}
    )

    load_order_payments = PythonOperator(
        task_id="load_order_payments",
        python_callable=load_static_table,
        op_kwargs={"table_name": "order_payments"}
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_static_table,
        op_kwargs={"table_name": "customers"}
    )

    load_sellers = PythonOperator(
        task_id="load_sellers",
        python_callable=load_static_table,
        op_kwargs={"table_name": "sellers"}
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_static_table,
        op_kwargs={"table_name": "products"}
    )

    load_geolocation = PythonOperator(
        task_id="load_geolocation",
        python_callable=load_static_table,
        op_kwargs={"table_name": "geolocation"}
    )

    load_category_translation = PythonOperator(
        task_id="load_category_translation",
        python_callable=load_static_table,
        op_kwargs={"table_name": "category_translation"}
    )

    # Dependencies — static tables load first, then partitioned
    [load_order_items, load_order_payments, load_customers,
     load_sellers, load_products, load_geolocation,
     load_category_translation] >> load_orders >> load_order_reviews