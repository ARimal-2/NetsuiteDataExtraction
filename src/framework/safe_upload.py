import asyncio
import logging

from src.s3uploader.upload_to_s3 import fetch_and_upload

logger = logging.getLogger(__name__)
async def safe_upload(data: list, resource_name: str):
    """Wraps sync fetch_and_upload in an async-safe manner."""
    try:
        logger.info(f"Uploading {resource_name} with {len(data)} records")
        object_key = await asyncio.to_thread(fetch_and_upload, data, resource_name)
        logger.info(f"{resource_name} uploaded to S3 successfully: {object_key}")
    except Exception as e:
        logger.error(f"{resource_name} upload failed: {e}", exc_info=True)
        raise
