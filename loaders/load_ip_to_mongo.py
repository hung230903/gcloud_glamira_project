import os
import sys
import time
from pymongo import MongoClient, UpdateOne
from concurrent.futures import ProcessPoolExecutor

# Thêm root path để có thể import từ config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.base import (
    IP2LOCATION_DIR, IP2LOCATION_BATCH_SIZE,
    MONGO_URI, MONGO_DB, SUMMARY_COLLECTION, IP_COLLECTION
)
from config.logger import setup_logger
from utils import time_utils
from utils.file_saving_utils import save_json_batch
from processing.ip_transformer import lookup_ip_from_doc
from utils.checkpoint_utils import get_checkpoint_manager

# Module-level logger
logger = setup_logger(
    name="ip_to_location",
    log_folder="loaders",
    log_file="ip_to_location.log",
)

def _get_distinct_ips(summary_col, checkpoint_manager):
    pipeline = [{"$group": {"_id": "$ip"}}]
    
    # Resume from checkpoint if it exists
    checkpoint = checkpoint_manager.get_checkpoint()
    if checkpoint:
        skip_cnt = int(checkpoint)
        pipeline.append({"$skip": skip_cnt})
        logger.info(f"Resuming IP transformation from skip count: {skip_cnt}")
    
    return summary_col.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=100000,
    )

def _write_batch(output_col, mongo_batch, json_batch, file_idx):
    """Write a batch of results to MongoDB and a JSON file."""
    if not mongo_batch:
        return

    result = output_col.bulk_write(mongo_batch, ordered=False)
    logger.info(
        f"Batch {file_idx} MongoDB completed: "
        f"Processed {len(mongo_batch)} records | "
        f"Upserted: {result.upserted_count}"
    )

    filename = f"ip_location_batch_{file_idx}.json"
    save_json_batch(json_data=json_batch, directory=IP2LOCATION_DIR, filename=filename, logger=logger)

def run_ip_to_location(
    mongo_uri=MONGO_URI,
    mongo_db=MONGO_DB,
    raw_collection=SUMMARY_COLLECTION,
    ip_collection=IP_COLLECTION,
    batch_size=IP2LOCATION_BATCH_SIZE,
    workers=10,
):
    """Execute the full IP-to-location transformation pipeline."""
    start_time = time.perf_counter()
    logger.info("JOB START | Starting IP-to-location transformation")
    
    client = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        summary_col = db[raw_collection]
        output_col = db[ip_collection]
        output_col.create_index("ip", unique=True)

        checkpoint_manager = get_checkpoint_manager("ip_to_location")
        cursor = _get_distinct_ips(summary_col, checkpoint_manager)
        os.makedirs(IP2LOCATION_DIR, exist_ok=True)

        with ProcessPoolExecutor(max_workers=workers) as executor:
            mongo_data = []
            ip_cnt = 0
            json_data = []
            file_idx = 1

            for result in executor.map(lookup_ip_from_doc, cursor):
                mongo_data.append(
                    UpdateOne(
                        {"ip": result["ip"]},
                        {"$set": result},
                        upsert=True,
                    )
                )
                json_data.append(result)
                ip_cnt += 1

                if len(mongo_data) >= batch_size:
                    _write_batch(output_col, mongo_data, json_data, file_idx)
                    
                    # Update checkpoint
                    checkpoint = int(checkpoint_manager.get_checkpoint() or 0)
                    checkpoint_manager.save_checkpoint(checkpoint + len(mongo_data))
                    
                    mongo_data.clear()
                    json_data.clear()
                    file_idx += 1

            if mongo_data:
                _write_batch(output_col, mongo_data, json_data, file_idx)
    finally:
        client.close()
        logger.info("IpToLocation connection closed")

    total_time = time.perf_counter() - start_time
    total_time_formatted = time_utils.format_duration(total_time)
    logger.info(
        f"JOB END | IP-to-location transformation complete | "
        f"Total IPs Transform: {ip_cnt} | Total Time: {total_time_formatted}"
    )

if __name__ == "__main__":
    run_ip_to_location()
