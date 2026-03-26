import os
import sys
from google.cloud import bigquery
from datetime import datetime

# Thêm root path để có thể import từ config

from config.base import (
    BQ_PROJECT_ID,
    BQ_DATASET_ID,
    BQ_TABLE_IP2LOCATION,
    BQ_TABLE_SUMMARY,
    BQ_TABLE_PRODUCT_INFO,
    GCS_BUCKET_NAME,
    GCS_SUMMARY_FOLDER,
    GCS_IP2LOCATION_FOLDER,
    GCS_DESTINATION_FOLDER
)
from config.logger import setup_logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = setup_logger(
    name="gcs_to_bq",
    log_folder="loaders",
    log_file="gcs_to_bq.log",
)

def load_parquet_from_gcs(client, bucket_name, source_prefix, table_id):
    """Load all Parquet files from a GCS prefix into a BigQuery table."""
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}"
    
    # Cấu hình Load Job: Định dạng PARQUET và ghi đè (hoặc nối vào tùy nhu cầu)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # WRITE_TRUNCATE: Ghi đè toàn bộ bảng (thường dùng cho các lô dữ liệu định kỳ)
        # Nếu muốn nối thêm dữ liệu, đổi thành WRITE_APPEND
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{bucket_name}/{source_prefix}/*.parquet"
    
    logger.info(f"--- Loading data from {uri} to {table_ref} ---")
    
    try:
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Đợi job hoàn thành
        
        logger.info(f"SUCCESS: Loaded {load_job.output_rows} rows into {table_ref}.")
    except Exception as e:
        logger.error(f"FAILED to load BigQuery: {e}")

def run_load():
    start_time = datetime.now()
    logger.info(f"Starting BQ Load Job at {start_time}")

    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # Summary
    load_parquet_from_gcs(client, GCS_BUCKET_NAME, GCS_SUMMARY_FOLDER, BQ_TABLE_SUMMARY)
    
    # IP Location
    # load_parquet_from_gcs(client, GCS_BUCKET_NAME, GCS_IP2LOCATION_FOLDER, BQ_TABLE_IP2LOCATION)
    
    # [NEW] Product Info
    # load_parquet_from_gcs(client, GCS_BUCKET_NAME, GCS_DESTINATION_FOLDER, BQ_TABLE_PRODUCT_INFO)

    end_time = datetime.now()
    logger.info(f"Finished BQ Load Job. Duration: {end_time - start_time}")

if __name__ == "__main__":
    # Gỡ bỏ local credentials nếu chạy trên môi trường có ADC
    # os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    
    run_load()
