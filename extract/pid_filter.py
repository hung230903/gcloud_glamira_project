import os
import time

from pymongo import MongoClient

from config.base import (
    MONGO_URI, MONGO_DB, PRODUCT_EVENT_COLLECTIONS,
    PID_FILTER_DIR, PID_FILTER_BATCH_SIZE
)
from config.logger import setup_logger
from utils import time_utils
from utils.file_saving_utils import save_json_batch

# Global logger for the module
logger = setup_logger(
    name="pid_filter",
    log_folder="extract",
    log_file="pid_filter.log",
)

def _build_pipeline():
    """Build the aggregation pipeline.

    1. Match documents from product-related event collections.
    2. Project a unified product_id and url from varying field names.
    3. Filter out documents missing product_id or url.
    4. Group by (product_id, url) to get unique pairs.
    5. Sort by product_id so all URLs for the same product arrive
       consecutively — this allows streaming grouping in Python.
    """
    return [
        {
            "$match": {
                "collection": {"$in": PRODUCT_EVENT_COLLECTIONS},
            }
        },
        {
            "$project": {
                "product_id": {
                    "$cond": [
                        {"$ne": ["$product_id", None]},
                        "$product_id",
                        "$viewing_product_id",
                    ]
                },
                "url": {
                    "$cond": [
                        {"$eq": ["$collection", "product_view_all_recommend_clicked"]},
                        "$referrer_url",
                        "$current_url",
                    ]
                },
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None},
                "url": {"$ne": None},
            }
        },
        {
            "$group": {
                "_id": {
                    "product_id": "$product_id",
                    "url": "$url",
                }
            }
        },
        {
            "$sort": {"_id.product_id": 1}
        },
    ]

def _write_batch(batch, file_idx, output_dir, output_prefix):
    """Write a list of dicts to a numbered JSON file."""
    filename = f"{output_prefix}_{file_idx}.json"
    save_json_batch(data=batch, directory=output_dir, filename=filename, logger=logger)

def run_pid_filter(
    mongo_uri=MONGO_URI,
    mongo_db=MONGO_DB,
    source_collection="summary",
    batch_size=PID_FILTER_BATCH_SIZE,
    output_dir=PID_FILTER_DIR,
    output_prefix="product_url_batch",
):
    """Execute the full extraction pipeline.

    MongoDB returns unique (product_id, url) pairs sorted by product_id.
    We stream through the cursor and group URLs per product_id in Python,
    avoiding the 16MB BSON document size limit.
    """
    
    start_time = time.perf_counter()
    logger.info("JOB START | Starting PID filter extraction")

    os.makedirs(output_dir, exist_ok=True)

    client = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        collection = db[source_collection]

        pipeline = _build_pipeline()
        cursor = collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=10_000,
        )

        batch = []
        file_counter = 1
        total_products = 0

        current_pid = None
        current_urls = []

        for doc in cursor:
            pid = doc["_id"]["product_id"]
            url = doc["_id"]["url"]

            if pid != current_pid:
                # Emit the previous product (if any)
                if current_pid is not None:
                    batch.append({
                        "product_id": current_pid,
                        "urls": current_urls,
                    })
                    total_products += 1

                    if len(batch) >= batch_size:
                        _write_batch(batch, file_counter, output_dir, output_prefix)
                        batch.clear()
                        file_counter += 1

                current_pid = pid
                current_urls = [url]
            else:
                current_urls.append(url)

        # Emit the last product
        if current_pid is not None:
            batch.append({
                "product_id": current_pid,
                "urls": current_urls,
            })
            total_products += 1

        # Save remaining records
        if batch:
            _write_batch(batch, file_counter, output_dir, output_prefix)

        total_time = time.perf_counter() - start_time
        total_time_formatted = time_utils.format_duration(total_time)
        logger.info(
            f"JOB END | PID filter extraction complete | "
            f"Total products: {total_products} | Total time: {total_time_formatted}"
        )
    finally:
        client.close()
        logger.info("PidFilter connection closed")

if __name__ == "__main__":
    run_pid_filter()
