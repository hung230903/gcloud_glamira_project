import json
import os

from config.base import PRODUCT_INFO_DIR

SUCCESS_DIR = os.path.join(PRODUCT_INFO_DIR, "success")
ERROR_DIR = os.path.join(PRODUCT_INFO_DIR, "error")


def save_json_batch(data, directory, filename, logger, message="SAVED BATCH"):
    """
    Generic function to save a list of data to a JSON file.
    """
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(
        f"{message} | TOTAL: {len(data)} | FILE: {filename}"
    )


def save_success_data_to_files(file_idx, products, logger):
    """
    Specific wrapper for ProductCrawler success data.
    """
    filename = f"product_info_{file_idx}.json"
    save_json_batch(products, SUCCESS_DIR, filename, logger, message="SUCCESS | SAVED BATCH")


def save_error_data_to_files(status, pid_info, logger):
    """
    Saves error PID to a text file and detailed info to a JSON array.
    """
    if isinstance(pid_info, dict):
        pid = pid_info.get("pid")
        failed_url = pid_info.get("url")
        all_urls = pid_info.get("all_urls")
    else:
        pid = pid_info
        failed_url = None
        all_urls = None

    os.makedirs(ERROR_DIR, exist_ok=True)
    
    # Save PID to text file (as before)
    with open(f"{ERROR_DIR}/{status}.txt", "a") as f:
        f.write(f"{pid}\n")
    
    # Save details to JSON array (for debugging)
    if failed_url:
        detail_path = f"{ERROR_DIR}/{status}_detail.json"
        details = []
        if os.path.exists(detail_path):
            try:
                with open(detail_path, "r", encoding="utf-8") as f:
                    details = json.load(f)
            except Exception:
                details = []
        
        details.append({
            "product_id": pid,
            "failed_url": failed_url,
            "urls": all_urls
        })
        
        with open(detail_path, "w", encoding="utf-8") as f:
            json.dump(details, f, ensure_ascii=False, indent=4)
            
    logger.error(f"FAILED | {status} | ID: {pid}")


def save_exception_data_to_files(status, pid_info, logger):
    """
    Saves exception PID to a text file and detailed info to a JSON array.
    """
    if isinstance(pid_info, dict):
        pid = pid_info.get("pid")
        failed_url = pid_info.get("url")
        all_urls = pid_info.get("all_urls")
    else:
        pid = pid_info
        failed_url = None
        all_urls = None

    os.makedirs(ERROR_DIR, exist_ok=True)
    
    # Save PID to text file
    with open(f"{ERROR_DIR}/{status}.txt", "a") as f:
        f.write(f"{pid}\n")

    # Save details to JSON array
    if failed_url:
        detail_path = f"{ERROR_DIR}/{status}_detail.json"
        details = []
        if os.path.exists(detail_path):
            try:
                with open(detail_path, "r", encoding="utf-8") as f:
                    details = json.load(f)
            except Exception:
                details = []
        
        details.append({
            "product_id": pid,
            "failed_url": failed_url,
            "urls": all_urls
        })
        
        with open(detail_path, "w", encoding="utf-8") as f:
            json.dump(details, f, ensure_ascii=False, indent=4)
            
    logger.error(f"FAILED | EXCEPTION | {status} | ID: {pid}")
