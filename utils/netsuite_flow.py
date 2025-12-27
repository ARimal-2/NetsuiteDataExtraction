from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import asyncio
import logging

from src.framework.safe_upload import safe_upload

logger = logging.getLogger(__name__)


def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def generate_flow(resource_key: str, redis_client, id_extractor, detail_extractor, batch_size=3000):
    """
    Generates an Airflow flow for a given resource.
    Handles fetching IDs and processing details in batches.
    """

    redis_queue = f"{resource_key}_queue"

    # Fetch IDs task
 
    @task(task_id=f"fetch_{resource_key}_ids")
    def fetch_ids_task():
        async def run_extractor():
            resource_name, id_list, resource_data = await id_extractor()

            # Clear Redis queue first
            if redis_client.llen(redis_queue) > 0:
                logger.info(f"Clearing Redis queue {redis_queue}")
                redis_client.delete(redis_queue)

            # Push new IDs
            if id_list:
                redis_client.rpush(redis_queue, *id_list)
                logger.info(f"Pushed {len(id_list)} IDs into {redis_queue}")

            # Upload any resource-level data if returned
            if resource_data:
                await safe_upload(resource_data, resource_name)

            return len(id_list)

        return run_async(run_extractor())

    # Fetch details task
    @task(task_id=f"fetch_{resource_key}_details")
    def fetch_details_task():
        async def process_batches():
            while True:
                batch = []

                for _ in range(batch_size):
                    raw_id = redis_client.lpop(redis_queue)
                    if raw_id is None:
                        break
                    batch.append(raw_id.decode("utf-8"))

                if not batch:
                    logger.info(f"No more IDs left in Redis queue for {resource_key}")
                    break

                logger.info(f"{resource_key} | Processing batch of {len(batch)} IDs")
                await detail_extractor(batch)

                # Yield to event loop so Airflow heartbeat is safe
                await asyncio.sleep(0)

        run_async(process_batches())

    # Branching task
    @task.branch(task_id=f"decide_{resource_key}_path")
    def decide_path(id_count: int):
        return f"fetch_{resource_key}_details" if id_count > 0 else f"no_{resource_key}_ids"

    no_ids = EmptyOperator(task_id=f"no_{resource_key}_ids")

    return fetch_ids_task(), decide_path, fetch_details_task(), no_ids
