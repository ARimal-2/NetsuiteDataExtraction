import os
import json
import tempfile
from datetime import datetime, timezone
from src.s3uploader.connect import connect_to_cos
from logs.ingestionlogger import save_ingestion_metadata, setup_logging_for_url
from ibm_boto3.s3.transfer import TransferConfig, TransferManager


def fetch_and_upload(data, resource_name):
    cos, bucket_name = connect_to_cos()
    outer_logs_dir, logs_dir, logger = setup_logging_for_url(resource_name)

    utc_now = datetime.now(timezone.utc)
    date_path = utc_now.strftime("%Y/%m/%d")
    timestamp1 = utc_now.strftime("%Y%m%dT%H%M%S")

    object_key = f"Netsuite_Api/{resource_name}/{date_path}/{timestamp1}_{resource_name}.json"
    logger.info(f"Starting upload for {resource_name} → bucket: {bucket_name}, key: {object_key}")

    part_size = 20 * 1024 * 1024  # 20 MB
    transfer_config = TransferConfig(
        multipart_threshold=part_size,
        multipart_chunksize=part_size
    )
    transfer_mgr = TransferManager(cos, config=transfer_config)

    try:
        # STREAM JSON TO FILE (NO MEMORY SPIKE)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            tmp_path = tmp_file.name
            tmp_file.write(b"[")

            for i, record in enumerate(data):
                if i > 0:
                    tmp_file.write(b",")

                tmp_file.write(json.dumps(record).encode("utf-8"))

            tmp_file.write(b"]")
            tmp_file.flush()

        file_size = os.path.getsize(tmp_path)
        logger.info(f"Prepared JSON file size: {file_size / 1024 / 1024:.2f} MB")

        # Upload
        logger.info("Upload started...")
        future = transfer_mgr.upload(tmp_path, bucket_name, object_key)
        future.result()
        logger.info("Upload finished successfully.")

        # Confirm upload
        response = cos.head_object(Bucket=bucket_name, Key=object_key)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        ingested_date = response["LastModified"].astimezone(timezone.utc).isoformat()

        logger.info(f"Upload confirmed. Status={status_code}, LastModified={ingested_date}")

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        status_code = None
        ingested_date = None

    finally:
        transfer_mgr.shutdown()
        if "tmp_path" in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)

    save_ingestion_metadata(
        outer_logs_dir,
        resource_name,
        len(data),
        status_code,
        ingested_date,
        object_key
    )
