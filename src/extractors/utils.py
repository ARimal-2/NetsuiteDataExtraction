import asyncio
from datetime import datetime,timezone

from logs.extractionlogger import (
    setup_logging_for_url,
    save_extraction_outputs,
    save_extraction_metadata,
    load_last_extraction_time,
    get_logger,
    
)
import logging

# src/extractors/utils.py
from datetime import datetime
import os
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def setup_queue_logger(resource_name: str, mode: str) -> logging.Logger:
    """
    Create a queue logger for a given resource with separate daily-rotated logs.
    mode = "send" or "receive"
    Logs go to: logs/queue/{resource_name}/{mode}.log (rotated daily)
    """
    if mode not in ("send", "receive"):
        raise ValueError("mode must be 'send' or 'receive'")
    date_str = datetime.now().strftime("%Y%m%d")
    log_dir = os.path.join("logs", "queue", resource_name,date_str)
    os.makedirs(log_dir, exist_ok=True)

    logger_name = f"queue.{resource_name}.{mode}"
    queue_logger = logging.getLogger(logger_name)

    if not queue_logger.handlers:  # avoid duplicate handlers
        queue_logger.setLevel(logging.INFO)
        log_file = os.path.join(log_dir, f"{mode}.log")

        handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",     # rotate daily
            interval=1,
            backupCount=7,       # keep 7 days
            encoding="utf-8",
            utc=True
        )

        formatter = logging.Formatter(
            f"[{mode.upper()}] %(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        queue_logger.addHandler(handler)

    return queue_logger

def get_extraction_dates(outer_logs_dir, resource_name, now):
    """Get extraction date range for incremental or full load."""

    modified_date_end = utc_dateformat(now)
    last_extracted = load_last_extraction_time(outer_logs_dir, resource_name)
    
    print(f"Last extracted time for {resource_name}: {last_extracted}")
    
    if last_extracted:
        # If last_extracted is a string, convert to datetime
        if isinstance(last_extracted, str):
            try:
                # Handle Zulu time (Z) and convert to ISO format
                last_extracted_dt = datetime.fromisoformat(last_extracted.replace("Z", "+00:00"))
            except Exception as e:
                print(f"Error parsing last_extracted: {e}")
                last_extracted_dt = None
        else:
            last_extracted_dt = last_extracted
            
        if last_extracted_dt:
            last_extracted = utc_dateformat(last_extracted_dt)
        else:
            print("last_extracted could not be parsed, skipping incremental filter.")
            last_extracted = None
    
    return last_extracted, modified_date_end



def utc_dateformat(dt):
    """
    Formats a datetime object in the format:
    YYYY-MM-DDThh:mm:ss[.fffffff]Z
    (UTC, fractional seconds optional up to 7 digits)
    """

    # Ensure datetime is in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    if dt.microsecond > 0:
        # Convert microseconds (6 digits) to ticks (7 digits: 100ns)
        frac = f"{dt.microsecond * 10:07d}"
        # Remove trailing zeros if allowed
        frac = frac.rstrip('0')
        return f"{base}.{frac}Z"
    else:
        return f"{base}Z"

def initialize_logger(resource_name: str, now: datetime):
    """Setup directories and logger for the resource."""
    from logs.extractionlogger import setup_logging_for_url, get_logger
    
    # Get the setup result once
    outer_logs_dir, logs_dir, id_file_path  = setup_logging_for_url(resource_name, now)
    
    # Debug logging (remove in production)
    # print(f"setup_result: {(outer_logs_dir, log_dir, id_file_path)}")
    
    logger = get_logger(resource_name, logs_dir, now)


    # print(f"Logger initialized: {outer_logs_dir}, {log_dir}, {id_file_path}, {logger}")
    
    return outer_logs_dir, logs_dir, id_file_path, logger


async def save_outputs_and_metadata(resource_name: str, resource_data: list, log_dir: str, outer_logs_dir: str, now: datetime,id_file_path: str):
    """Save API data and metadata asynchronously."""
    from logs.extractionlogger import save_extraction_outputs, save_extraction_metadata
    
    # Save outputs 
    await asyncio.to_thread(save_extraction_outputs, resource_data, log_dir, resource_name)
    
    # Handle the case where id_file_path might be None - pass empty string
    if id_file_path is None:
        id_file_path = ''
    extracted_date = utc_dateformat(now)
    await asyncio.to_thread(save_extraction_metadata, outer_logs_dir, resource_name, len(resource_data), extracted_date,id_file_path)
    
def setup_extraction_environment(resource_name, now):
    outer_logs_dir, logs_dir, id_file_path, log_file_path = setup_logging_for_url(resource_name, now)
    logger = get_logger(resource_name, log_file_path)
    return outer_logs_dir, logs_dir, id_file_path, logger


# def setup_extraction_environment(resource_name, now):
#     """Set up logging directories and logger for extraction."""
#     try:
#         outer_logs_dir, log_dir = setup_logging_for_url(resource_name, now)
#         logger = get_logger(resource_name, log_dir, now)
#     except Exception as e:
#         # Fallback: create a basic logger to avoid UnboundLocalError
#         logging.basicConfig(level=logging.INFO)
#         logger = logging.getLogger(resource_name)
#         logger.warning(f"Failed to setup structured logger: {e}")
#         outer_logs_dir, log_dir = ".", "."
#     return outer_logs_dir, log_dir, logger

        


    
        



