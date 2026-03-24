import os
import json
import glob
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from config.base import (
    PRODUCT_INFO_DIR,
    GCS_BUCKET_NAME,
    GCS_DESTINATION_FOLDER
)
from config.logger import setup_logger
from schema.schemas import get_product_info_pyarrow_schema

logger = setup_logger(
    name="load_product_to_gcs",
    log_folder="loaders",
    log_file="load_product_to_gcs.log",
)

def _get_product_json_files():
    """Lấy danh sách các tệp JSON thành công từ thư mục crawler."""
    success_dir = os.path.join(PRODUCT_INFO_DIR, "success")
    pattern = os.path.join(success_dir, "product_info_*.json")
    return sorted(glob.glob(pattern))

def upload_to_gcs(buffer, destination_blob_name):
    """Upload tệp từ buffer lên Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        
        buffer.seek(0)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        logger.info(f"SUCCESS | Uploaded to GCS: gs://{GCS_BUCKET_NAME}/{destination_blob_name}")
        return True
    except Exception as e:
        logger.error(f"FAILED | Could not upload {destination_blob_name} to GCS: {e}")
        return False

def _filter_dict_to_schema(data, schema_type):
    """
    Hàm đệ quy để lọc các field trong dict/list khớp với PyArrow Struct/List.
    PyArrow Struct rất nghiêm ngặt: không được thừa/thiếu field so với schema.
    """
    if data is None:
        return None
    
    # Nếu schema yêu cầu Struct
    if pa.types.is_struct(schema_type):
        if not isinstance(data, dict):
            return None
        filtered = {}
        for i in range(schema_type.num_fields):
            field = schema_type.field(i)
            val = data.get(field.name)
            filtered[field.name] = _filter_dict_to_schema(val, field.type)
        return filtered
    
    # Nếu schema yêu cầu List
    if pa.types.is_list(schema_type):
        if not isinstance(data, list):
            return []
        item_schema = schema_type.value_type
        return [_filter_dict_to_schema(item, item_schema) for item in data]
    
    # Kiểu dữ liệu nguyên tử (Scalar)
    if pa.types.is_integer(schema_type):
        try: return int(float(data)) if data is not None else None
        except: return None
    if pa.types.is_floating(schema_type):
        try: return float(data) if data is not None else None
        except: return None
    if pa.types.is_boolean(schema_type):
        try:
            if isinstance(data, str):
                return data.lower() in ("true", "1", "yes")
            return bool(data) if data is not None else None
        except: return None
    if pa.types.is_string(schema_type):
        if isinstance(data, (dict, list)):
            return json.dumps(data, ensure_ascii=False)
        return str(data) if data is not None else None
        
    return data

def _ensure_schema_columns(df, schema):
    """Đảm bảo DataFrame khớp hoàn toàn với schema lồng nhau."""
    for name in schema.names:
        field_type = schema.field(name).type
        if name not in df.columns:
            df[name] = None
        
        # Áp dụng lọc recursive cho từng dòng
        df[name] = df[name].apply(lambda x: _filter_dict_to_schema(x, field_type))
            
    return df[schema.names]

def run_load_product_to_gcs():
    """Đọc dữ liệu JSON, chuyển sang Parquet và đẩy lên GCS."""
    logger.info("JOB START | Starting Product Info export to GCS")
    
    json_files = _get_product_json_files()
    if not json_files:
        logger.warning("No product info JSON files found to upload.")
        return

    schema = get_product_info_pyarrow_schema()
    
    for i, file_path in enumerate(json_files, 1):
        filename = os.path.basename(file_path)
        logger.info(f"Processing file {i}/{len(json_files)}: {filename}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                continue

            df = pd.DataFrame(data)
            df = _ensure_schema_columns(df, schema)
            
            # Chuyển đổi sang PyArrow Table
            table = pa.Table.from_pandas(df, schema=schema)
            
            # Ghi vào buffer Parquet
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)
            
            # Xác định tên tệp đích trên GCS
            parquet_name = filename.replace(".json", ".parquet")
            destination = f"{GCS_DESTINATION_FOLDER}/{parquet_name}"
            
            upload_to_gcs(parquet_buffer, destination)
            
        except Exception as e:
            logger.error(f"ERROR | Failed to process {filename}: {e}")

    logger.info("JOB END | Finished Product Info export to GCS")

if __name__ == "__main__":
    run_load_product_to_gcs()
