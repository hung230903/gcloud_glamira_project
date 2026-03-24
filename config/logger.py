import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = "./logs"
LOG_FILE = None
LOG_LEVEL = logging.INFO
LOG_MAX_BYTES = 5_000_000
LOG_BACKUP_COUNT = 3


def setup_logger(name=None, log_folder=None, log_file=LOG_FILE):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # Tránh add handler trùng
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_folder:
        log_dir = os.path.join(LOG_DIR, log_folder)
    else:
        log_dir = LOG_DIR

    os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        f"{log_dir}/{log_file}",
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
