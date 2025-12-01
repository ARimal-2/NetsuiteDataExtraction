from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import json
import os
import logging
import tempfile

from src.s3uploader.upload_to_s3 import fetch_and_upload

# Customer
from src.extractors.Customer.list_customers_ids import list_customers_ids
from src.extractors.Customer.list_customer import customers_list

# Payment
from src.extractors.CustomerPayment.list_customer_payment_id import list_customer_payment_id
from src.extractors.CustomerPayment.list_customer_payment import list_customer_payment

# Category
from src.extractors.CustomerCategory.list_customer_category_id import list_customer_category_id
from src.extractors.CustomerCategory.list_customer_category import list_customer_category

#Message
from src.extractors.CustomerMessage.list_customer_message import list_customer_message
from src.extractors.CustomerMessage.customer_message_id import list_customers_message_ids

from src.extractors.CustomerSubsidiaryRelationship.list_customer_subsidiary_relationship import list_customer_subsidiary_relationship
from src.extractors.CustomerSubsidiaryRelationship.list_customer_subsidiary_relationship_id import list_customer_subsidiary_relationship_id


import redis


logger = logging.getLogger(__name__)
r = redis.Redis(host="redis", port=6379, db=0)
# ---------------------------
# Async-safe upload wrapper
# ---------------------------
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
    dag_id="netsuite_customer_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["netsuite"],
) as dag:

    # ---------------------------
    # CUSTOMER
    # ---------------------------
    def fetch_customer_ids(**context):
        try:
            # Extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_customers_ids())
            
            if id_list:
                r.rpush("customer_id_queue", *id_list)
                print(f"Pushed {len(id_list)} customer IDs to Redis queue")

            # Upload the full customer data (not just the ID list)
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No customer data returned from list_customers_ids")
        except Exception as e:
            logger.error(f"fetch_customer_ids failed: {e}", exc_info=True)
            raise

    def fetch_customer_details(**context):
        try:
            all_ids = []
            while True:
                cust_id = r.lpop("customer_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            print(f"id list: {all_ids}")
            logger.info(f"Loaded {len(all_ids)} customer IDs from Redis queue")
            if not all_ids:
                raise ValueError("No Customer IDs found")

            # customers_list returns (resource_name, all_items, customer_ids)
            resource_name, data, _ = asyncio.run(customers_list(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("customers_list returned no data")
        except Exception as e:
            logger.error(f"fetch_customer_details failed: {e}", exc_info=True)
            raise

    list_customer_ids_task = PythonOperator(
        task_id="list_customer_ids",
        python_callable=fetch_customer_ids
    )

    list_customer_details_task = PythonOperator(
        task_id="list_customer_details",      # FIXED
        python_callable=fetch_customer_details
    )

#     # ---------------------------
#     # CATEGORY
#     # ---------------------------
    def fetch_category_ids(**context):
        try:
            # Extractor returns (resource_name, id_list, resource_data)
            resource_name, id_list, resource_data = asyncio.run(list_customer_category_id())
            if id_list:
                r.rpush("category_id_queue", *id_list)
                print(f"Pushed {len(id_list)} category IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No resource_data returned from list_customer_category_id")
        except Exception as e:
            logger.error(f"fetch_category_ids failed: {e}", exc_info=True)
            raise

    def fetch_category_details(**context):
        try:
            all_ids = []
            while True:
                cust_id = r.lpop("category_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            print(f"id list: {all_ids}")
            logger.info(f"Loaded {len(all_ids)} categories IDs from Redis queue")
            if not all_ids:
                raise ValueError("No Customer IDs found")
            resource_name, data, _  = asyncio.run(list_customer_category(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list_customer_category returned no data")
        except Exception as e:
            logger.error(f"fetch_category_details failed: {e}", exc_info=True)
            raise

    list_category_ids_task = PythonOperator(
        task_id="list_category_ids",
        python_callable=fetch_category_ids
    )

    list_category_details_task = PythonOperator(
        task_id="list_category_details",
        python_callable=fetch_category_details
    )

#     # ---------------------------
#     # PAYMENT
#     # ---------------------------
    def fetch_payment_ids(**context):
        try:
            resource_name, id_list, resource_data = asyncio.run(list_customer_payment_id())
            if id_list:
                    r.rpush("payment_id_queue", *id_list)
                    print(f"Pushed {len(id_list)} payment IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data,resource_name))
            else:
                logger.warning("No resource_data returned from list_customer_payment_id")
        except Exception as e:
            logger.error(f"fetch_payment_ids failed: {e}", exc_info=True)
            raise

    def fetch_payment_details(**context):
        try:
            all_ids = []
            while True:
                cust_id = r.lpop("payment_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            print(f"id list: {all_ids}")
            logger.info(f"Loaded {len(all_ids)} categories IDs from Redis queue")
    
            if not all_ids:
                raise ValueError("No Payment IDs found in file")

            resource_name, data, _  = asyncio.run(list_customer_payment(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list_customer_payment returned no data")
        except Exception as e:
            logger.error(f"fetch_payment_details failed: {e}", exc_info=True)
            raise

    list_payment_ids_task = PythonOperator(
        task_id="list_payment_ids",
        python_callable=fetch_payment_ids
    )

    list_payment_details_task = PythonOperator(
        task_id="list_payment_details",
        python_callable=fetch_payment_details
    )
    # ---------------------------
    # Message
    # ---------------------------
    def fetch_message_ids(**context):
        try:
            resource_name, id_list, resource_data = asyncio.run(list_customers_message_ids())
            if id_list:
                r.rpush("message_id_queue", *id_list)
                print(f"Pushed {len(id_list)} message IDs to Redis queue")

            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No resource_data returned from customer_message_ids")
        except Exception as e:
            logger.error(f"fetch_customer_message_ids failed: {e}", exc_info=True)
            raise

    def fetch_message_details(**context):
        try:
            all_ids = []
            while True:
                cust_id = r.lpop("message_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))

            resource_name, data, _  = asyncio.run(list_customer_message(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list_customer_message returned no data")
        except Exception as e:
            logger.error(f"fetch_message_details failed: {e}", exc_info=True)
            raise

    list_message_ids_task = PythonOperator(
        task_id="list_message_ids",
        python_callable=fetch_message_ids
    )

    list_message_details_task = PythonOperator(
        task_id="list_customer_message_details",
        python_callable=fetch_message_details
    )


 # ---------------------------
    # subsidiary_relationship
    # ---------------------------
    def fetch_subsidiary_relationship_ids(**context):
        try:
            resource_name, id_list, resource_data = asyncio.run(list_customer_subsidiary_relationship_id())
            if id_list:
                r.rpush("subsidiary_relationship_id_queue", *id_list)
                print(f"Pushed {len(id_list)} subsidiary_relationship IDs to Redis queue")

            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No resource_data returned from customer_subsidiary_relationship_ids")
        except Exception as e:
            logger.error(f"fetch_customer_subsidiary_relationship_ids failed: {e}", exc_info=True)
            raise

    def fetch_subsidiary_relationship_details(**context):
        try:
            all_ids = []
            while True:
                cust_id = r.lpop("subsidiary_relationship_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            print(f"id list: {all_ids}")
            
            logger.info(f"Loaded {len(all_ids)} subsidiary_relationship IDs")
            if not all_ids:
                raise ValueError("No subsidiary_relationship IDs found in file")

            resource_name, data, _  = asyncio.run(list_customer_subsidiary_relationship(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list_customer_subsidiary_relationship returned no data")
        except Exception as e:
            logger.error(f"fetch_subsidiary_relationship_details failed: {e}", exc_info=True)
            raise

    list_subsidiary_relationship_ids_task = PythonOperator(
        task_id="list_subsidiary_relationship_ids",
        python_callable=fetch_subsidiary_relationship_ids
    )

    list_subsidiary_relationship_details_task = PythonOperator(
        task_id="list_customer_subsidiary_relationship_details",
        python_callable=fetch_subsidiary_relationship_details
    )

#     # ---------------------------
#     # DEPENDENCIES
#     # ---------------------------
    list_customer_ids_task >> list_customer_details_task
    list_category_ids_task >> list_category_details_task
    list_payment_ids_task >> list_payment_details_task
    list_message_ids_task >> list_message_details_task
    list_subsidiary_relationship_ids_task >> list_subsidiary_relationship_details_task
