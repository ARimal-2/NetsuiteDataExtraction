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

from src.extractors.InventoryItem.list_inventory_items import list_inventory_items
from src.extractors.InventoryItem.list_inventory_item_id import list_inventory_item_id



from src.extractors.InventoryNumber.list_inventory_number import list_inventory_id
from src.extractors.InventoryNumber.list_inventory_details import list_inventory_details


from src.extractors.InventoryTransfer.list_inventory_transfer_id import list_inventory_transfer_id
from src.extractors.InventoryTransfer.list_inventory_transfer_details import list_inventory_transfer_details


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
    dag_id="netsuite_inventory_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["netsuite"],
) as dag:


    # ---------------------------
    # inventory items
    # ---------------------------
    def fetch_inventory_items_ids(**context):
        try:
            # extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_inventory_item_id())

            if id_list:
                r.rpush("inventory_items_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory items IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
        except Exception as e:
            logger.error(f"fetch_inventory_items_ids failed: {e}", exc_info=True)
            raise

    def fetch_inventory_items_details(**context):
        try:
            all_ids = []
            while True:
                invent_id = r.lpop("inventory_items_id_queue")
                if invent_id is None:
                    break
                all_ids.append(invent_id.decode('utf-8'))

            data = asyncio.run(list_inventory_items(all_ids))
            if data:
                asyncio.run(safe_upload(data, "inventory_items_details"))
            else:
                logger.warning("list_inventory_items returned no data")
        except Exception as e:
            logger.error(f"fetch_inventory items_details failed: {e}", exc_info=True)
            raise

    list_inventory_items_ids_task = PythonOperator(
        task_id="list_inventory_items_ids",
        python_callable=fetch_inventory_items_ids 
    )

    list_inventory_items_details_task = PythonOperator(
        task_id="list_inventory_items_details",      # FIXED
        python_callable=fetch_inventory_items_details
    )


    
    # ---------------------------
    # inventory Number
    # ---------------------------
    def fetch_inventory_number_ids(**context):
        try:
            # extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_inventory_id())
            if id_list:
                r.rpush("inventory_number_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory number IDs to Redis queue")
         

            # Upload only the ID list (not the full resource_data)
            if resource_data:
                asyncio.run(safe_upload(resource_data,resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
        except Exception as e:
            logger.error(f"fetch_inventory_items_ids failed: {e}", exc_info=True)
            raise

    def fetch_inventory_details(**context):
        try:
            all_ids = []
            while True:
                invent_id = r.lpop("inventory_number_id_queue")
                if invent_id is None:
                    break
                all_ids.append(invent_id.decode('utf-8'))
            resource_name, all_items, numbers = asyncio.run(list_inventory_details(all_ids))
            if all_items:
                asyncio.run(safe_upload(all_items, resource_name))
            else:
                logger.warning("list_inventory returned no data")
        except Exception as e:
            logger.error(f"fetch_inventory_details failed: {e}", exc_info=True)
            raise

    list_inventory_ids_task = PythonOperator(
        task_id="list_inventory_number_ids",
        python_callable=fetch_inventory_number_ids 
    )

    list_inventory_details_task = PythonOperator(
        task_id="list_inventory_details",      # FIXED
        python_callable=fetch_inventory_details
    )

    # ---------------------------
    # inventory Transfer
    # ---------------------------
    def fetch_inventory_transfer_ids(**context):
        try:
            # Extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_inventory_transfer_id())
            
            if id_list:
                r.rpush("inventory_transfer_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory transfer IDs to Redis queue")

            # Upload the full inventory transfer data (not just the ID list)
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No inventory transfer data returned from list_inventory transfers_ids")
        except Exception as e:
            logger.error(f"fetch_inventory transfer_ids failed: {e}", exc_info=True)
            raise

    def fetch_inventory_transfer_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            all_ids = []
            while True:
                invent_id = r.lpop("inventory_transfer_id_queue")
                if invent_id is None:
                    break
                all_ids.append(invent_id.decode('utf-8'))
            
            logger.info(f"Loaded {len(all_ids)} inventory IDs from temp file: {id_file_path}")
            if not all_ids:
                raise ValueError("No inventory IDs found in file")

            resource_name, all_items = asyncio.run(list_inventory_transfer_details(all_ids))
            if all_items:
                asyncio.run(safe_upload(all_items,resource_name))
            else:
                logger.warning("list_inventory returned no data")
        except Exception as e:
            logger.error(f"fetch_inventory_transfer_details failed: {e}", exc_info=True)
            raise

    list_inventory_transfer_ids_task = PythonOperator(
        task_id="list_inventory_transfer_id",
        python_callable=fetch_inventory_transfer_ids 
    )

    list_inventory__transfer_details_task = PythonOperator(
        task_id="list_inventory_transfer_details",      
        python_callable=fetch_inventory_transfer_details
    )


    list_inventory_items_ids_task >> list_inventory_items_details_task
    list_inventory_ids_task >> list_inventory_details_task
    list_inventory_transfer_ids_task >> list_inventory__transfer_details_task