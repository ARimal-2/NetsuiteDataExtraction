from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import json
import os
import logging
import tempfile
from src.s3uploader.upload_to_s3 import fetch_and_upload
import redis

from src.extractors.PurchaseOrder.list_purchase_order_details import list_purchase_order
from src.extractors.PurchaseOrder.list_purchase_order_id import list_purchase_order_id



r = redis.Redis(host="redis", port=6379, db=0)
logger = logging.getLogger(__name__)

async def safe_upload(data: list, resource_name: str):
    try:
        logger.info(f"Uploading {resource_name} with {len(data)} records")
        object_key = await asyncio.to_thread(fetch_and_upload, data, resource_name)
        logger.info(f"{resource_name} uploaded to S3 successfully: {object_key}")
    except Exception as e:
        logger.error(f"{resource_name} upload failed: {e}", exc_info=True)
        raise


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="netsuite_purchase_order_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["netsuite"],
) as dag:


    # ---------------------------
    # inventory items
    # ---------------------------
    def fetch_purchase_orders_ids(**context):
        try:
            # extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_purchase_order_id())

            if id_list:
                r.rpush("purchase_order_id_queue", *id_list)
                print(f"Pushed {len(id_list)} purchase order IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_purchase_order_id")
        except Exception as e:
            logger.error(f"fetch_purchase_orders_ids failed: {e}", exc_info=True)
            raise

    def fetch_purchase_orders_details(**context):
        try:
            all_ids = []
            while True:
                invent_id = r.lpop("purchase_order_id_queue")
                if invent_id is None:
                    break
                all_ids.append(invent_id.decode('utf-8'))

            resource_name, all_items = asyncio.run(list_purchase_order(all_ids))
            if all_items:
                asyncio.run(safe_upload(all_items, resource_name))
            else:
                logger.warning("list_purchase_orders returned no data")
        except Exception as e:
            logger.error(f"fetch_purchase_order_details failed: {e}", exc_info=True)
            raise

    list_purchase_orders_ids_task = PythonOperator(
        task_id="list_purchase_orders_ids",
        python_callable=fetch_purchase_orders_ids 
    )

    list_purchase_orders_details_task = PythonOperator(
        task_id="list_purchase_orders_details",      # FIXED
        python_callable=fetch_purchase_orders_details
    )

    list_purchase_orders_ids_task >> list_purchase_orders_details_task
