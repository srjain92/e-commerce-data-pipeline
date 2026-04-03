import os
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_DIR = "raw_data"
UPLOAD_DIR = "partitioned"

# ── 1. GCS client from env variable (no key file) ────────────────────────────
def get_gcs_client():
    key_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(key_json)
    )
    return storage.Client(credentials=credentials)

# ── 2. Download dataset from Kaggle ──────────────────────────────────────────
def download_dataset():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.system(f"kaggle datasets download -d {KAGGLE_DATASET} -p {DOWNLOAD_DIR} --unzip")
    print("✓ Dataset downloaded")

# ── 3. Upload a single file to GCS ───────────────────────────────────────────
def upload_to_gcs(client, local_path: str, gcs_path: str):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"✓ Uploaded {gcs_path}")

# ── 4. Partition transactional files by month and upload ─────────────────────
def partition_and_upload(client, filename: str, date_column: str, table_name: str):
    df = pd.read_csv(f"{DOWNLOAD_DIR}/{filename}")
    df[date_column] = pd.to_datetime(df[date_column])
    df["partition_month"] = df[date_column].dt.to_period("M").astype(str)

    os.makedirs(f"{UPLOAD_DIR}/{table_name}", exist_ok=True)

    for month, group in df.groupby("partition_month"):
        local_path = f"{UPLOAD_DIR}/{table_name}/{month}.parquet"
        group.drop(columns=["partition_month"]).to_parquet(local_path, index=False)
        upload_to_gcs(client, local_path, f"raw/{table_name}/{table_name}_{month}.parquet")

    print(f"✓ Partitioned and uploaded {table_name}")

# ── 5. Upload static dimension files as-is ───────────────────────────────────
def upload_static(client, filename: str, table_name: str):
    df = pd.read_csv(f"{DOWNLOAD_DIR}/{filename}")
    local_path = f"{UPLOAD_DIR}/{table_name}.parquet"
    df.to_parquet(local_path, index=False)
    upload_to_gcs(client, local_path, f"raw/static/{table_name}.parquet")
    print(f"✓ Uploaded static {table_name}")


# ── 7. Cleanup local files ────────────────────────────────────────────────────
def cleanup():
    import shutil
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR)
        print(f"✓ Deleted {DOWNLOAD_DIR}")
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)
        print(f"✓ Deleted {UPLOAD_DIR}")


# ── 6. Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    client = get_gcs_client()
    download_dataset()

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # Transactional — partitioned by month
    partition_and_upload(client, "olist_orders_dataset.csv",        "order_purchase_timestamp", "orders")
    partition_and_upload(client, "olist_order_reviews_dataset.csv", "review_creation_date",     "order_reviews")

    # Static — uploaded once
    upload_static(client, "olist_order_items_dataset.csv",       "order_items")
    upload_static(client, "olist_order_payments_dataset.csv",    "order_payments")
    upload_static(client, "olist_customers_dataset.csv",         "customers")
    upload_static(client, "olist_sellers_dataset.csv",           "sellers")
    upload_static(client, "olist_products_dataset.csv",          "products")
    upload_static(client, "olist_geolocation_dataset.csv",       "geolocation")
    upload_static(client, "product_category_name_translation.csv", "category_translation")

    print("\n✓ All files uploaded to GCS")

    cleanup()