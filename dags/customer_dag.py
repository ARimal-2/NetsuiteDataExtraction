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

logger = logging.getLogger(__name__)

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
            resource_data, id_list, _ = asyncio.run(list_customers_ids())
            
            # Write IDs to temp file and push filepath via XCom
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(id_list, f)
                id_file_path = f.name
            
            context["ti"].xcom_push(key="customer_ids_file", value=id_file_path)
            logger.info(f"Wrote {len(id_list)} customer IDs to temp file: {id_file_path}")

            if resource_data:
                asyncio.run(safe_upload(resource_data, "customer_ids"))
            else:
                logger.warning("No resource_data returned from list_customers_ids")
        except Exception as e:
            logger.error(f"fetch_customer_ids failed: {e}", exc_info=True)
            raise

    def fetch_customer_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            id_file_path = context["ti"].xcom_pull(task_ids="list_customer_ids", key="customer_ids_file")
            if not id_file_path or not os.path.exists(id_file_path):
                raise ValueError("No customer IDs file found")
            
            with open(id_file_path, 'r') as f:
                id_list = json.load(f)
            
            logger.info(f"Loaded {len(id_list)} customer IDs from temp file: {id_file_path}")
            if not id_list:
                raise ValueError("No Customer IDs found in file")

            data = asyncio.run(customers_list(id_list))
            if data:
                asyncio.run(safe_upload(data, "customer_details"))
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

    # ---------------------------
    # CATEGORY
    # ---------------------------
    def fetch_category_ids(**context):
        try:
            resource_data, id_list, _ = asyncio.run(list_customer_category_id())
            
            # Write IDs to temp file and push filepath via XCom
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(id_list, f)
                id_file_path = f.name
            
            context["ti"].xcom_push(key="category_ids_file", value=id_file_path)
            logger.info(f"Wrote {len(id_list)} category IDs to temp file: {id_file_path}")

            if resource_data:
                asyncio.run(safe_upload(resource_data, "category_ids"))
            else:
                logger.warning("No resource_data returned from list_customer_category_id")
        except Exception as e:
            logger.error(f"fetch_category_ids failed: {e}", exc_info=True)
            raise

    def fetch_category_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            id_file_path = context["ti"].xcom_pull(task_ids="list_category_ids", key="category_ids_file")
            if not id_file_path or not os.path.exists(id_file_path):
                raise ValueError("No category IDs file found")
            
            with open(id_file_path, 'r') as f:
                id_list = json.load(f)
            
            logger.info(f"Loaded {len(id_list)} category IDs from temp file: {id_file_path}")
            if not id_list:
                raise ValueError("No Category IDs found in file")

            data = asyncio.run(list_customer_category(id_list))
            if data:
                asyncio.run(safe_upload(data, "category_details"))
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

    # ---------------------------
    # PAYMENT
    # ---------------------------
    def fetch_payment_ids(**context):
        try:
            resource_data, id_list, _ = asyncio.run(list_customer_payment_id())
            
            # Write IDs to temp file and push filepath via XCom
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(id_list, f)
                id_file_path = f.name
            
            context["ti"].xcom_push(key="payment_ids_file", value=id_file_path)
            logger.info(f"Wrote {len(id_list)} payment IDs to temp file: {id_file_path}")

            if resource_data:
                asyncio.run(safe_upload(resource_data, "payment_ids"))
            else:
                logger.warning("No resource_data returned from list_customer_payment_id")
        except Exception as e:
            logger.error(f"fetch_payment_ids failed: {e}", exc_info=True)
            raise

    def fetch_payment_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            id_file_path = context["ti"].xcom_pull(task_ids="list_payment_ids", key="payment_ids_file")
            if not id_file_path or not os.path.exists(id_file_path):
                raise ValueError("No payment IDs file found")
            
            with open(id_file_path, 'r') as f:
                id_list = json.load(f)
            
            logger.info(f"Loaded {len(id_list)} payment IDs from temp file: {id_file_path}")
            if not id_list:
                raise ValueError("No Payment IDs found in file")

            data = asyncio.run(list_customer_payment(id_list))
            if data:
                asyncio.run(safe_upload(data, "payment_details"))
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
            resource_data, id_list, _ = asyncio.run(list_customers_message_ids())
            
            # Write IDs to temp file and push filepath via XCom
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(id_list, f)
                id_file_path = f.name
            
            context["ti"].xcom_push(key="message_ids_file", value=id_file_path)
            logger.info(f"Wrote {len(id_list)} payment IDs to temp file: {id_file_path}")

            if resource_data:
                asyncio.run(safe_upload(resource_data, "customer_message_ids"))
            else:
                logger.warning("No resource_data returned from customer_message_ids")
        except Exception as e:
            logger.error(f"fetch_customer_message_ids failed: {e}", exc_info=True)
            raise

    def fetch_message_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            id_file_path = context["ti"].xcom_pull(task_ids="list_message_ids", key="message_ids_file")
            if not id_file_path or not os.path.exists(id_file_path):
                raise ValueError("No message IDs file found")
            
            with open(id_file_path, 'r') as f:
                id_list = json.load(f)
            
            logger.info(f"Loaded {len(id_list)} message IDs from temp file: {id_file_path}")
            if not id_list:
                raise ValueError("No message IDs found in file")

            data = asyncio.run(list_customer_message(id_list))
            if data:
                asyncio.run(safe_upload(data, "message_details"))
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
            resource_data, id_list, _ = asyncio.run(list_customer_subsidiary_relationship_id())
            
            # Write IDs to temp file and push filepath via XCom
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(id_list, f)
                id_file_path = f.name
            
            context["ti"].xcom_push(key="subsidiary_relationship_ids_file", value=id_file_path)
            logger.info(f"Wrote {len(id_list)} payment IDs to temp file: {id_file_path}")

            if resource_data:
                asyncio.run(safe_upload(resource_data, "customer_subsidiary_relationship_ids"))
            else:
                logger.warning("No resource_data returned from customer_subsidiary_relationship_ids")
        except Exception as e:
            logger.error(f"fetch_customer_subsidiary_relationship_ids failed: {e}", exc_info=True)
            raise

    def fetch_subsidiary_relationship_details(**context):
        try:
            # Pull filepath from XCom and read IDs from temp file
            id_file_path = context["ti"].xcom_pull(task_ids="list_subsidiary_relationship_ids", key="subsidiary_relationship_ids_file")
            if not id_file_path or not os.path.exists(id_file_path):
                raise ValueError("No subsidiary_relationship IDs file found")
            
            with open(id_file_path, 'r') as f:
                id_list = json.load(f)
            
            logger.info(f"Loaded {len(id_list)} subsidiary_relationship IDs from temp file: {id_file_path}")
            if not id_list:
                raise ValueError("No subsidiary_relationship IDs found in file")

            data = asyncio.run(list_customer_subsidiary_relationship(id_list))
            if data:
                asyncio.run(safe_upload(data, "subsidiary_relationship_details"))
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

    # ---------------------------
    # DEPENDENCIES
    # ---------------------------
    list_customer_ids_task >> list_customer_details_task
    list_category_ids_task >> list_category_details_task
    list_payment_ids_task >> list_payment_details_task
    list_message_ids_task >> list_message_details_task
    list_subsidiary_relationship_ids_task >> list_subsidiary_relationship_details_task
