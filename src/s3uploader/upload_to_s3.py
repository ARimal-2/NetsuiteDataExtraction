import os
import json
import tempfile
import logging
from datetime import datetime, timezone
from src.s3uploader.connect import connect_to_cos
from logs.ingestionlogger import save_ingestion_metadata, setup_logging_for_url
from ibm_boto3.s3.transfer import TransferConfig, TransferManager

# Configure a logger for multipart uploads


def fetch_and_upload(data, resource_name):
    cos, bucket_name = connect_to_cos()
    outer_logs_dir, logs_dir,logger = setup_logging_for_url(resource_name)

    today = datetime.now()
    date_path = today.strftime("%Y/%m/%d")
    timestamp = today.strftime("%Y%m%dT%H%M%S")

    object_key = f"Plex_Api/{resource_name}/{date_path}/{timestamp}_{resource_name}.json"
    logger.info(f"Starting upload for {resource_name} → bucket: {bucket_name}, key: {object_key}")

    # Config for multipart (5 MB parts)
    part_size = 5 * 1024 * 1024
    transfer_config = TransferConfig(
        multipart_threshold=part_size,
        multipart_chunksize=part_size
    )
    transfer_mgr = TransferManager(cos, config=transfer_config)

    # Setup logging

    try:
        # Write JSON to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            json_bytes = json.dumps(data, indent=2).encode("utf-8")
            tmp_file.write(json_bytes)
            tmp_file.flush()
            tmp_path = tmp_file.name

        file_size = os.path.getsize(tmp_path)
   

        if file_size > transfer_config.multipart_threshold:
            logger.info(f"Multipart upload enabled (chunk size = {part_size/1024/1024} MB)")
        else:
            logger.info("Single-part upload (file below threshold)")

        # Upload
        logger.info("Upload started...")
        future = transfer_mgr.upload(tmp_path, bucket_name, object_key)
        future.result()  
        logger.info("Upload finished successfully.")

        # Confirm upload
        response = cos.head_object(Bucket=bucket_name, Key=object_key)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        ingested_date = response['LastModified'].astimezone(timezone.utc).isoformat()

        logger.info(f"Upload confirmed. Status={status_code}, LastModified={ingested_date}")

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        status_code = None
        ingested_date = None
    finally:
        transfer_mgr.shutdown()
        # Optional cleanup
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)
            

    # Save metadata
    save_ingestion_metadata(outer_logs_dir, resource_name,len(data), status_code, ingested_date,object_key)
