from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
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


def run_async(coro):
    """
    SAFEST way to run async inside Airflow.
    Avoids asyncio.run() which kills the heartbeat.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()



def generate_flow(
    resource_key: str,
    redis_client,
    id_extractor,
    detail_extractor,
):
    redis_queue = f"{resource_key}_queue"

    # ----------------------------------------------------------------------
    @task(task_id=f"fetch_{resource_key}_ids")
    def fetch_ids_task():
        async def run_extractor():
            resource_name, id_list, resource_data = await id_extractor()

            if id_list:
                redis_client.rpush(redis_queue, *id_list)

            if resource_data:
                await safe_upload(resource_data, resource_name)

            return len(id_list)

        return run_async(run_extractor())
        
    @task(task_id=f"fetch_{resource_key}_details")
    def fetch_details_task():
        async def process_batch(batch):
            resource_name, details, *_ = await detail_extractor(batch)
            if details:
                await safe_upload(details, resource_name)

        # Get all IDs from Redis
        ids = []
        while True:
            raw_id = redis_client.lpop(redis_queue)
            if raw_id is None:
                break
            ids.append(raw_id.decode("utf-8"))
        
        if ids:
            run_async(process_batch(ids))
        else:
            logger.warning(f"No IDs found for {resource_key}")
            return

        logger.info(f"Processing {len(ids)} IDs for {resource_key}")
 

    @task.branch(task_id=f"decide_{resource_key}_path")
    def decide_path(id_count: int):
        return (
            f"fetch_{resource_key}_details"
            if id_count > 0
            else f"no_{resource_key}_ids"
        )

    no_ids = EmptyOperator(task_id=f"no_{resource_key}_ids")

    return fetch_ids_task(), decide_path, fetch_details_task(), no_ids
