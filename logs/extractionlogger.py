import os
import logging
import json
from datetime import datetime, timezone


# # from urllib.parse import urlparse

resource_name_list = ["purchase_orders_types", "purchase_orders_statuses","users_account"]

def get_logger(resource_name: str, logs_dir: str, timestamp: datetime = None):
    """
    Returns a logger object for a given resource.
    Each logger writes to its own log file inside logs_dir.
    """
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S") if timestamp else datetime.now().strftime("%Y%m%d_%H%M%S")
    
    log_file_path = os.path.join(logs_dir, f"{resource_name}.log")
    
    # Create logger
    logger_name = f"{resource_name}_{timestamp_str}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    logger.handlers = []

    # File handler
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # # Console handler
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    logger.info(f"Logger initialized for {resource_name} in {logs_dir}")
    return logger
    
    

def setup_logging_for_url(resource_name, timestamp):
    """
    Returns the outer_logs_dir, logs_dir, id_file_path
    """
    # print(timestamp)
    # print(f"type of timestamp",type(timestamp))
    current_time = timestamp.strftime("%Y%m%d_%H%M%S")
    # print(type(current_time))
    
    # Create directories
    outer_logs_dir = os.path.join("logs", "extraction", resource_name)
    os.makedirs(outer_logs_dir, exist_ok=True)

    logs_dir = os.path.join(outer_logs_dir, current_time)
    os.makedirs(logs_dir, exist_ok=True)    

    # ID file path if needed
    if resource_name in resource_name_list:
        id_file_path = None
    else:
        idlist_dir = os.path.join(logs_dir, "idlist")
        os.makedirs(idlist_dir, exist_ok=True)
        id_file_path = os.path.join(idlist_dir, f"{resource_name}_ids.json")
    
    return outer_logs_dir, logs_dir, id_file_path


def save_extraction_metadata(outer_logs_dir, resource_name, total_count,extraction_date, id_file_path):
    """
    Saves metadata about the extraction to a JSON file with historical record.
    """
    # folder_name = extraction_date.strftime("%Y%m%d_%H%M%S")
    # print(f"folder_name: {folder_name}")
 
    metadata_path = os.path.join(outer_logs_dir, f"{resource_name}_metadata.json")
    extraction_date_str = extraction_date

    if id_file_path is None:
        metadata = {
            "extraction_date": extraction_date_str,
            "count": total_count,
            "history": [],
            "last_extracted": extraction_date_str
        }
    else:
        metadata = {
            "extraction_date": extraction_date_str,
            "count": total_count,
            "id_path": id_file_path,
            "history": [],
            "last_extracted": extraction_date_str
        }
        
    # Try to load existing metadata to append to history
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)
                history = existing_metadata.get("history", [])
                if "extraction_date" in existing_metadata and "count" in existing_metadata:
                    history_record = {
                        "extraction_date": existing_metadata["extraction_date"],
                        "count": existing_metadata["count"]
                    }
                    # Only add id_file_path if it exists in previous metadata and is not None
                    if existing_metadata.get("id_path") is not None:
                        history_record["id_file_path"] = existing_metadata["id_path"]
                    history.append(history_record)
                metadata["history"] = history
    except Exception as e:
        logging.warning(f"Failed to read existing metadata to preserve history: {e}")

    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logging.info(f"Metadata saved to {metadata_path}")
    except Exception as e:
        logging.error(f"Error while saving metadata: {e}")
        

def save_extraction_outputs(data, logs_dir, resource_name):
    """
    Saves data to .json files, sorted files, and count to txt.
    """
    try:
        # all_path = os.path.join(logs_dir, f"{resource_name}.json")
        # with open(all_path, "w", encoding="utf-8") as f:
        #     json.dump(data, f, ensure_ascii=False, indent=4)
        # logging.info(f"Saved all invoices to {all_path}")
        sorted_by_modified = sorted(
        data,
        key=lambda x: x.get('modifiedDate') or '',
        reverse=True
    )

        # latest_1000 = sorted_by_modified[:1000]
        mod_path = os.path.join(logs_dir, f"{resource_name}modifieddate.json")
        with open(mod_path, "w", encoding="utf-8") as f:
            json.dump(sorted_by_modified, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved sorted by modifiedDateEnd to {mod_path}")

        # sorted_by_invoice = sorted(data, key=lambda x: x.get('invoiceNumber', ''))
        # inv_path = os.path.join(logs_dir, f"{resource_name}sorted.json")
        # with open(inv_path, "w", encoding="utf-8") as f:
        #     json.dump(sorted_by_invoice, f, ensure_ascii=False, indent=4)
        # logging.info(f"Saved sorted by invoiceNumber to {inv_path}")

        count_path = os.path.join(logs_dir, f"{resource_name}_count.txt")
        with open(count_path, "w") as f:
            f.write(f"Total invoices fetched: {len(data)}\n")
        logging.info(f"Wrote count to {count_path}")

    except Exception as e:
        logging.error(f"Error while saving outputs: {e}")


def load_last_extraction_time(outer_logs_dir, resource_name):
    """
    Loads the last extraction timestamp from the metadata JSON file if available.
    Returns (last_extracted, folder_name).
    If no data is found, returns (None, None) to indicate first load.
    """
    metadata_file = os.path.join(outer_logs_dir, f"{resource_name}_metadata.json")
    
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            last_extracted = metadata.get("last_extracted")

            if last_extracted is None:
                logging.info("No last extraction date found — treating as first load.")
                return None  # First load
            
            return last_extracted
        
        except Exception as e:
            logging.warning(f"Could not read metadata file: {e}")
            return None  # First load
    
    logging.info("Metadata file not found — treating as first load.")
    return None  # First load

