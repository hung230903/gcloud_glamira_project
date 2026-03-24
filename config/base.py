import os
import json
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "./data")

# Ip2location
IP2LOCATION_DIR = os.path.join(DATA_DIR, os.getenv("IP2LOCATION_DIR_NAME", "ip2location"))
IP2LOCATION_BATCH_SIZE = int(os.getenv("IP2LOCATION_BATCH_SIZE", 100000))
IP2LOCATION_DB = os.getenv("IP2LOCATION_DB_FILE", "./IP2LOCATION-LITE-DB11.BIN")

# Pid Filter
PID_FILTER_DIR = os.path.join(DATA_DIR, os.getenv("PID_FILTER_DIR_NAME", "pid_filter"))
PID_FILTER_BATCH_SIZE = int(os.getenv("PID_FILTER_BATCH_SIZE", 500))

# Crawler
PRODUCT_INFO_DIR = os.path.join(DATA_DIR, os.getenv("PRODUCT_CRAWLER_DIR_NAME", "product_info"))
CRAWLER_BATCH_SIZE = int(os.getenv("PRODUCT_CRAWLER_BATCH_SIZE", 500))
CRAWLER_SEMAPHORE = int(os.getenv("PRODUCT_CRAWLER_SEMAPHORE", 10))
CRAWLER_TIMEOUT = int(os.getenv("PRODUCT_CRAWLER_TIMEOUT", 15))
CRAWLER_MAX_RETRIES = int(os.getenv("PRODUCT_CRAWLER_MAX_RETRIES", 3))
CRAWLER_HEADERS = json.loads(os.getenv("PRODUCT_CRAWLER_HEADERS", '{}'))

# Export to GCS
SUCCESS_DIR = os.getenv("SUCCESS_DIR", "data/product_info/success")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "glamira_project_bucket")
GCS_DESTINATION_FOLDER = os.getenv("GCS_DESTINATION_FOLDER", "product_info")
GCS_IP2LOCATION_FOLDER = os.getenv("GCS_IP2LOCATION_FOLDER", "ip2location")
GCS_SUMMARY_FOLDER = os.getenv("GCS_SUMMARY_FOLDER", "summary_data")
SUMMARY_BSON_PATH = os.getenv("SUMMARY_BSON_PATH", "data/glamira-data/summary.bson")

# MongoDB
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://admin:strongpass9999@34.81.118.35:27017/admin")
MONGO_DB = os.getenv("MONGODB_DB_NAME", "glamira")
SUMMARY_COLLECTION = os.getenv("MONGODB_RAW_DATA_COL", "summary")
IP_COLLECTION = os.getenv("MONGODB_IP2LOCATION_COL", "ip_location")
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", 500000)) # Số dòng mỗi file Parquet (Part)
PRODUCT_EVENT_COLLECTIONS = json.loads(os.getenv("MONGODB_PRODUCT_EVENTS", '["view_product_detail", "select_product_option", "select_product_option_quality", "add_to_cart_action", "product_detail_recommendation_visible", "product_detail_recommendation_noticed", "product_view_all_recommend_clicked"]'))

# BigQuery
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "raw_glamira_data")
BQ_TABLE_SUMMARY = os.getenv("BQ_TABLE_SUMMARY", "summary")
BQ_TABLE_IP2LOCATION = os.getenv("BQ_TABLE_IP2LOCATION", "raw_ip2location")
BQ_TABLE_PRODUCT_INFO = os.getenv("BQ_TABLE_PRODUCT_INFO", "product_info")
