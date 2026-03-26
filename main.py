import logging
from config.base import CRAWLER_BATCH_SIZE
from extract.pid_filter import run_pid_filter
from extract.product_crawler import run_product_crawler
from loaders.load_ip_to_mongo import run_ip_to_location as run_ip_transform
from loaders.load_product_info_to_gcs import run_load_product_to_gcs
from loaders.load_summary_to_gcs import export_to_gcs
from loaders.gcs_to_bq import run_load as bq_load

# Monitoring (Tạm thời không dùng)
# from monitoring.metrics_handler import StageTimer, push_metrics

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def step_ip_to_location():
    """BƯỚC 1: Chuyển đổi IP sang location và lưu vào MongoDB."""
    logger.info("--- STAGE 1: IP TO LOCATION ---")
    # with StageTimer("ip_to_location"):
    run_ip_transform()

def step_pid_filter():
    """BƯỚC 2: Lọc các Product ID và URL từ dữ liệu summary."""
    logger.info("--- STAGE 2: PID FILTER ---")
    # with StageTimer("pid_filter"):
    run_pid_filter()
#
def step_product_crawler():
    # """BƯỚC 3: Crawl thông tin sản phẩm từ các URL đã lọc."""
    logger.info("--- STAGE 3: PRODUCT CRAWLER ---")
    # with StageTimer("product_crawler"):
    run_product_crawler(batch_size=CRAWLER_BATCH_SIZE)

def step_product_info_to_gcs():
    # """BƯỚC 3.5: Đẩy dữ liệu product_info vừa crawl lên GCS."""
    logger.info("--- STAGE 3.5: PRODUCT INFO TO GCS ---")
    run_load_product_to_gcs()
#
def step_export_to_gcs():
    """BƯỚC 4: Export dữ liệu từ MongoDB sang GCS dưới dạng Parquet."""
    # logger.info("--- STAGE 4: EXPORT TO GCS ---")
    # with StageTimer("export_to_gcs"):
    export_to_gcs()

def step_bigquery_load():
    """BƯỚC 5: Nạp dữ liệu từ GCS vào BigQuery."""
    logger.info("--- STAGE 5: BIGQUERY LOAD ---")
    # with StageTimer("bigquery_load"):
    bq_load()

def main():
    """Điều phối toàn bộ flow của pipeline."""
    logger.info("=== STARTING FULL DATA PIPELINE FLOW ===")
    
    try:
        # 1. Làm giàu dữ liệu IP
#         step_ip_to_location()
        
        # 2. Lọc PID
#         step_pid_filter()
        
        # 3. Crawl dữ liệu sản phẩm mới
        # step_product_crawler()
        
        # 3.5 Load Product Info to GCS (JSON -> Parquet)
        # step_product_info_to_gcs()
        
        # 4. Export dữ liệu sang GCS
        step_export_to_gcs()
        
        # 5. Nạp vào BigQuery
        step_bigquery_load()
        
        logger.info("=== DATA PIPELINE COMPLETED SUCCESSFULLY ===")
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
    # finally:
        # Luôn đẩy metrics lên Gateway dù thành công hay thất bại
        # push_metrics()

if __name__ == "__main__":
    main()
