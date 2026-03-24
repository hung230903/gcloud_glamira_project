import os
import io
import sys
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from google.cloud import storage

from config.base import (
    GCS_BUCKET_NAME,
    GCS_IP2LOCATION_FOLDER,
    GCS_SUMMARY_FOLDER,
    MONGO_URI,
    MONGO_DB,
    SUMMARY_COLLECTION,
    IP_COLLECTION,
    MONGO_BATCH_SIZE,
)
from config.logger import setup_logger
from processing.summary_transformer import (
    transform_summary_data,
    transform_ip2location_data,
)
from utils.checkpoint_utils import get_checkpoint_manager
from bson import ObjectId
from schema.schemas import (
    get_summary_pyarrow_schema,
    get_ip2location_pyarrow_schema,
)

# Thêm root path để có thể import từ config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Sử dụng logger tập trung từ config
logger = setup_logger(
    name="export_to_gcs",
    log_folder="loaders",
    log_file="export.log",
)


def upload_buffer_to_gcs(buffer, bucket_name, destination_blob_name):
    """Upload a memory buffer to GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_file(buffer, content_type="application/octet-stream")
        logger.info(f"SUCCESS Upload: gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        logger.error(f"FAILED Upload {destination_blob_name}: {e}")
        return False


def _ensure_schema_columns(df, schema):
    """Đảm bảo DataFrame có đủ các cột theo schema, thêm cột thiếu nếu cần."""
    for name in schema.names:
        if name not in df.columns:
            if isinstance(schema.field(name).type, pa.ListType):
                df[name] = [[] for _ in range(len(df))]
            else:
                df[name] = None
    return df[schema.names]


def export_collection_to_gcs(
        collection,
        collection_name,
        gcs_folder,
        transform_func=None,
        schema=None,
        batch_size=MONGO_BATCH_SIZE,
):
    """
    Đọc dữ liệu từ một MongoDB collection theo batch,
    chuyển đổi sang Parquet và upload lên GCS.
    Mỗi batch tạo ra một file Parquet riêng (part file).
    Hỗ trợ resume từ checkpoint sử dụng ObjectId.
    """
    checkpoint_manager = get_checkpoint_manager(f"mongo_to_gcs_{collection_name}")
    last_id = checkpoint_manager.get_checkpoint()

    query = {}
    if last_id:
        try:
            query["_id"] = {"$gt": ObjectId(last_id)}
            logger.info(f"[{collection_name}] Resuming from last_id: {last_id}")
        except Exception as e:
            logger.warning(f"[{collection_name}] Invalid checkpoint ID {last_id}: {e}")

    logger.info(
        f"--- Processing {collection_name} | "
        f"Batch size: {batch_size:,} ---"
    )

    # Sắp xếp theo _id để đảm bảo thứ tự ổn định và dùng checkpoint chính xác
    cursor = collection.find(query).sort("_id", 1).batch_size(batch_size)

    part_idx = 0
    current_batch = []
    processed = 0

    for doc in cursor:
        current_batch.append(doc)
        processed += 1

        if processed % 100000 == 0:
            logger.info(f"[{collection_name}] Already read: {processed:,} records...")

        if len(current_batch) >= batch_size:
            part_idx += 1
            _write_batch_to_gcs(
                current_batch, collection_name, gcs_folder,
                part_idx, transform_func, schema,
            )
            
            # Cập nhật checkpoint bằng _id của document cuối trong batch
            checkpoint_manager.save_checkpoint(str(current_batch[-1]["_id"]))
            
            current_batch = []

    # Batch cuối cùng
    if current_batch:
        part_idx += 1
        _write_batch_to_gcs(
            current_batch, collection_name, gcs_folder,
            part_idx, transform_func, schema,
        )
        checkpoint_manager.save_checkpoint(str(current_batch[-1]["_id"]))

    logger.info(
        f"[{collection_name}] DONE | "
        f"Total records exported: {processed:,} | "
        f"Total parts: {part_idx}"
    )


def _write_batch_to_gcs(batch, collection_name, gcs_folder, part_idx, transform_func, schema):
    """Chuyển đổi một batch documents sang Parquet buffer và upload lên GCS."""
    logger.info(
        f"[{collection_name}] Writing part {part_idx} | "
        f"Batch size: {len(batch):,} records..."
    )

    df = pd.DataFrame(batch)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    if transform_func:
        df = transform_func(df)

    if schema:
        df = _ensure_schema_columns(df, schema)
        table = pa.Table.from_pandas(df, schema=schema)
    else:
        table = pa.Table.from_pandas(df)

    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    destination = f"{gcs_folder}/{collection_name}_part_{part_idx:04d}.parquet"
    upload_buffer_to_gcs(parquet_buffer, GCS_BUCKET_NAME, destination)


def export_to_gcs():
    """Luồng chính: Đọc dữ liệu từ MongoDB, chuyển sang Parquet và upload lên GCS."""

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    try:
        # 1. Export SUMMARY
        logger.info("=== [1/2] Exporting SUMMARY collection ===")
        summary_col = db[SUMMARY_COLLECTION]
        summary_schema = get_summary_pyarrow_schema()
        export_collection_to_gcs(
            collection=summary_col,
            collection_name="summary",
            gcs_folder=GCS_SUMMARY_FOLDER,
            transform_func=transform_summary_data,
            schema=summary_schema,
        )

        # 2. Export IP2LOCATION
        logger.info("=== [2/2] Exporting IP2LOCATION collection ===")
        ip2location_col = db[IP_COLLECTION]
        ip2location_schema = get_ip2location_pyarrow_schema()
        export_collection_to_gcs(
            collection=ip2location_col,
            collection_name="ip2location",
            gcs_folder=GCS_IP2LOCATION_FOLDER,
            transform_func=transform_ip2location_data,
            schema=ip2location_schema,
        )
    finally:
        client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    # Gỡ bỏ credentials local để sử dụng ADC (Application Default Credentials) của GCP
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

    start_time = datetime.now()
    logger.info(f"Starting Data Export at {start_time}")

    export_to_gcs()

    end_time = datetime.now()
    logger.info(f"Finished Data Export. Duration: {end_time - start_time}")
