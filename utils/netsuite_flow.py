from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

import asyncio
import logging
from datetime import timedelta

from src.framework.safe_upload import safe_upload

logger = logging.getLogger(__name__)


# -------------------------------------------------
# ASYNC RUNNER
# -------------------------------------------------
def run_async(coro):
    """
    Standard asyncio runner for Airflow tasks.
    Python 3.12 safe.
    """
    return asyncio.run(coro)


# -------------------------------------------------
# NETSUITE FLOW FACTORY
# -------------------------------------------------
def generate_flow(
    resource_key: str,
    redis_client,
    id_extractor,
    detail_extractor,
    batch_size: int = 3000,
):
    """
    Standard NetSuite endpoint flow.

    Returns:
        fetch_ids_task,
        decide_path,
        fetch_details_task,
        no_ids
    """

    redis_queue = f"netsuite:{resource_key}:queue"

    # -------------------------------------------------
    # FETCH IDS (RUNS ONCE)
    # -------------------------------------------------
    @task(
        task_id=f"fetch_{resource_key}_ids",
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )
    def fetch_ids_task():
        async def _run():
            resource_name, id_list, resource_data = await id_extractor()

            # Clear stale queue
            if redis_client.exists(redis_queue):
                logger.info("Clearing Redis queue: %s", redis_queue)
                redis_client.delete(redis_queue)

            if id_list:
                redis_client.rpush(redis_queue, *id_list)
                logger.info(
                    "%s | Queued %s IDs",
                    resource_key,
                    len(id_list),
                )

            if resource_data:
                await safe_upload(resource_data, resource_name)

            return len(id_list)

        return run_async(_run())
    # -------------------------------------------------
    # PROCESS ONE BATCH ONLY
    # -------------------------------------------------
    @task(
        task_id=f"fetch_{resource_key}_details",
        execution_timeout=timedelta(minutes=120),
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    def fetch_details_task():
        async def _run():
            batch = []

            for _ in range(batch_size):
                raw_id = redis_client.lpop(redis_queue)
                if raw_id is None:
                    break
                batch.append(raw_id.decode("utf-8"))

            if not batch:
                logger.info("%s | Redis queue empty", resource_key)
                return False

            logger.info(
                "%s | Processing batch of %s IDs",
                resource_key,
                len(batch),
            )

            await detail_extractor(batch)

            return True

        return run_async(_run())

    # -------------------------------------------------
    # BRANCH DECISION
    # -------------------------------------------------
    @task.branch(task_id=f"decide_{resource_key}_path")
    def decide_path(has_more_work: bool):
        return (
            f"fetch_{resource_key}_details"
            if has_more_work
            else f"no_{resource_key}_ids"
        )

    no_ids = EmptyOperator(task_id=f"no_{resource_key}_ids")


    return fetch_ids_task(), decide_path, fetch_details_task(), no_ids
