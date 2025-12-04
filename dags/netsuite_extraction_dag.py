from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import asyncio
import logging
import redis

from src.s3uploader.upload_to_s3 import fetch_and_upload


from src.extractors.Customer.list_customers_ids import list_customers_ids
from src.extractors.Customer.list_customer import customers_list


from src.extractors.InventoryItem.list_inventory_item_id import list_inventory_item_id
from src.extractors.InventoryItem.list_inventory_items import list_inventory_items


from src.extractors.InventoryNumber.list_inventory_number import list_inventory_id
from src.extractors.InventoryNumber.list_inventory_details import list_inventory_details


from src.extractors.InventoryTransfer.list_inventory_transfer_id import list_inventory_transfer_id
from src.extractors.InventoryTransfer.list_inventory_transfer_details import list_inventory_transfer_details

from src.extractors.inventoryCount.list_inventory_count import list_inventory_count
from src.extractors.inventoryCount.list_inventory_count_id import list_inventory_count_id


from src.extractors.PurchaseOrder.list_purchase_order_details import list_purchase_order
from src.extractors.PurchaseOrder.list_purchase_order_id import list_purchase_order_id

from src.extractors.SalesOrder.sales_order_details import list_sales_order
from src.extractors.SalesOrder.sales_order_ids import list_sales_order_ids

from src.extractors.account.list_account_id import list_account_id
from src.extractors.account.list_account_details import list_account_details

from src.extractors.AssemblyItem.list_assembly_items import list_assembly_items
from src.extractors.AssemblyItem.list_assembly_item_id import list_assembly_items_id

from src.extractors.department.list_department_id import list_department_id
from src.extractors.department.list_department import list_department_details

from src.extractors.InterCompanyTransferOrder.list_intercompany_Transfer_Order_id import list_intercompanyTransferOrder_id
from src.extractors.InterCompanyTransferOrder.list_intercompanyTransferOrder_details import list_intercompanyTransferOrder_details

from src.extractors.invoice.list_invoice import list_invoice_details
from src.extractors.invoice.list_invoice_id import list_invoice_id

from src.extractors.ItemFulfillment.list_item_fulfillment_id import list_item_fulfillment_id
from src.extractors.ItemFulfillment.list_item_fulfillment import list_item_fulfillment_details

from src.extractors.itemReceipt.list_itemReceipt import list_itemReceipt_details
from src.extractors.itemReceipt.list_itemReceipt_id import list_itemReceipt_id

from src.extractors.location.list_location_id import list_location_id
from src.extractors.location.list_location import list_location_details

from src.extractors.Subsidiary.list_subsidiary_id import list_subsidiary_id
from src.extractors.Subsidiary.list_subsidiary import list_subsidiary_details

from src.extractors.TransferOrder.list_transferOrder_id import list_transferOrder_id
from src.extractors.TransferOrder.list_transferOrder import list_transferOrder_details

from src.extractors.vendor.list_vendor import list_vendor_details
from src.extractors.vendor.list_vendor_id import list_vendor_id


from src.extractors.vendorBill.list_vendor_id import list_vendorBill_id
from src.extractors.vendorBill.list_vendor import list_vendorBill_details

from src.extractors.vendorCategory.list_vendorCategory_id import list_vendorCategory_id
from src.extractors.vendorCategory.list_vendorCategory import list_vendorCategory_details

logger = logging.getLogger(__name__)

try:
    r = redis.Redis(host="redis", port=6379, db=0)
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")

# ---------------------------
# Async-safe upload wrapper
# ---------------------------
async def safe_upload(data: list, resource_name: str):
    """Wraps sync fetch_and_upload in an async-safe manner for use in an async task."""
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

@dag(
    dag_id="netsuite_extraction_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["netsuite data extraction"],
)
def netsuite_pipeline():
    start = EmptyOperator(task_id="start_extraction")
    
    # ---------------------------
    # CUSTOMER TASKS DEFINITION
    # ---------------------------

    @task(task_id="fetch_customer_ids")
    def fetch_customer_ids_task() -> int:
        """
        Fetches customer IDs, uploads the initial list data, pushes IDs to Redis, 
        and returns the count of IDs for branching logic.
        """
        try:
            resource_name, id_list, resource_data = asyncio.run(list_customers_ids())
            
            if id_list:
                r.rpush("customer_id_queue", *id_list)
                print(f"Pushed {len(id_list)} customer IDs to Redis queue")

            # Upload the full customer data (not just the ID list)
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No customer data returned from list_customers_ids")
            
            return len(id_list)

        except Exception as e:
            logger.error(f"fetch_customer_ids failed: {e}", exc_info=True)
            raise

    @task(task_id="fetch_customer_details")
    def fetch_customer_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed customer records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("customer_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} customer IDs from Redis queue")

            resource_name, data, _ = asyncio.run(customers_list(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("customers_list returned no data")
        
        except Exception as e:
            logger.error(f"fetch_customer_details failed: {e}", exc_info=True)
            raise

    @task.branch(task_id="decide_processing_path")
    def decide_processing_path_task(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_customer_details" if id_count > 0 else "no_customer_ids"
    # Define a target task for the "do nothing" branch 
    no_customer_ids = EmptyOperator(task_id="no_customer_ids")
    
    # ---------------------------
    # INVENTORY TASKS DEFINITION
    # ---------------------------

    
    # ---------------------------
    # inventory items
    # ---------------------------
    @task(task_id="fetch_inventory_items_ids")
    def fetch_inventory_items_ids_task():
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
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_inventory_items_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_inventory_items_details")
    def fetch_inventory_item_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("inventory_items_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory items IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_inventory_items(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory item returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory items details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_inventory_processing_path")
    def decide_inventory_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_customer_details" if id_count > 0 else "no_inventory_items_id"
    # Define a target task for the "do nothing" branch 
    no_inventory_items_id = EmptyOperator(task_id="no_inventory_items_id")
    
    # ---------------------------
    # inventory Number
    # ---------------------------
    @task(task_id="fetch_inventory_number_ids")
    def fetch_inventory_number_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_inventory_id())

            if id_list:
                r.rpush("inventory_number_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory number IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_inventory_number_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_inventory_number_details")
    def fetch_inventory_number_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("inventory_number_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory number IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_inventory_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory number returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory numbers details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_inventory_number_processing_path")
    def decide_inventory_number_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_inventory_number_details" if id_count > 0 else "no_inventory_number_id"
    # Define a target task for the "do nothing" branch 
    no_inventory_numbers_id = EmptyOperator(task_id="no_inventory_number_id")
    

    # ---------------------------
    # inventory transfer
    # ---------------------------
    @task(task_id="fetch_inventory_transfer_ids")
    def fetch_inventory_transfer_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_inventory_transfer_id())

            if id_list:
                r.rpush("inventory_transfer_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory transfer IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_inventory_transfer_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_inventory_transfer_details")
    def fetch_inventory_transfer_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("inventory_transfer_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory transfer IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_inventory_transfer_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory transfer returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory transfers details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_inventory_transfer_processing_path")
    def decide_inventory_transfer_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_inventory_transfer_details" if id_count > 0 else "no_inventory_transfer_id"
    # Define a target task for the "do nothing" branch 
    no_inventory_transfers_id = EmptyOperator(task_id="no_inventory_transfer_id")
    
    # ---------------------------
    # inventory Count
    # ---------------------------
    @task(task_id="fetch_inventory_count_ids")
    def fetch_inventory_count_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_inventory_count_id())

            if id_list:
                r.rpush("inventory_count_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory count IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_inventory_count_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_inventory_count_details")
    def fetch_inventory_count_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("inventory_count_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory count IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_inventory_count(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory count returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory counts details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_inventory_count_processing_path")
    def decide_inventory_count_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_inventory_count_details" if id_count > 0 else "no_inventory_count_id"
    # Define a target task for the "do nothing" branch 
    no_inventory_counts_id = EmptyOperator(task_id="no_inventory_count_id")
    
    # ---------------------------
    # PURCHASE ORDER TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_purchase_orders_ids_task")
    def fetch_purchase_orders_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_purchase_order_id())

            if id_list:
                r.rpush("purchase_orders_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory count IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_purchase_orders_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_purchase_orders_details")
    def fetch_purchase_orders_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("purchase_orders_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory count IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_purchase_order(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory count returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory counts details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_purchase_orders_processing_path")
    def decide_purchase_orders_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_purchase_orders_details" if id_count > 0 else "no_purchase_orders_id"
    # Define a target task for the "do nothing" branch 
    no_purchase_orders_id = EmptyOperator(task_id="no_purchase_orders_id")
    
    # ---------------------------
    # SALES ORDER TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_sales_orders_ids_task")
    def fetch_sales_orders_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_sales_order_ids())

            if id_list:
                r.rpush("sales_orders_id_queue", *id_list)
                print(f"Pushed {len(id_list)} inventory count IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list_inventory_item_id")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_sales_orders_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_sales_orders_details")
    def fetch_sales_orders_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed inventory item records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("sales_orders_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} inventory count IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_sales_order(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory count returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory counts details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_sales_orders_processing_path")
    def decide_sales_orders_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_sales_orders_details" if id_count > 0 else "no_sales_orders_id"
    # Define a target task for the "do nothing" branch 
    no_sales_orders_id = EmptyOperator(task_id="no_sales_orders_id")

    # ---------------------------
    # Account TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_account_ids_task")
    def fetch_account_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_account_id())

            if id_list:
                r.rpush("account_id_queue", *id_list)
                print(f"Pushed {len(id_list)} account IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list account")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_account_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_account_details")
    def fetch_account_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed account records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("account_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} account IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_account_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("inventory count returned no data")
        
        except Exception as e:
            logger.error(f"fetch inventory counts details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_account_processing_path")
    def decide_account_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_account_details" if id_count > 0 else "no_account_id"
    # Define a target task for the "do nothing" branch 
    no_account_id = EmptyOperator(task_id="no_account_id")
    
    # ---------------------------
    # ASSEMBLY TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_assembly_items_ids_task")
    def fetch_assembly_items_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_assembly_items_id())

            if id_list:
                r.rpush("assembly_items_id_queue", *id_list)
                print(f"Pushed {len(id_list)} assembly_items IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list assembly_items")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_assembly_items_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_assembly_items_details")
    def fetch_assembly_items_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed assembly_items records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("assembly_items_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} assembly_items IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_assembly_items(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("assembly item returned no data")
        
        except Exception as e:
            logger.error(f"fetch assembly items details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_assembly_items_processing_path")
    def decide_assembly_items_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_assembly_items_details" if id_count > 0 else "no_assembly_items_id"
    # Define a target task for the "do nothing" branch 
    no_assembly_items_id = EmptyOperator(task_id="no_assembly_items_id")


    # ---------------------------
    # SUBSIDIARY TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_subsidiary_ids_task")
    def fetch_subsidiary_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_subsidiary_id())

            if id_list:
                r.rpush("subsidiary_id_queue", *id_list)
                print(f"Pushed {len(id_list)} subsidiary IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list subsidiary")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_subsidiary_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_subsidiary_details")
    def fetch_subsidiary_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed subsidiary records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("subsidiary_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} subsidiary IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_subsidiary_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list subsidiary returned no data")
        
        except Exception as e:
            logger.error(f"fetch_subsidiary_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_subsidiary_processing_path")
    def decide_subsidiary_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_subsidiary_details" if id_count > 0 else "no_subsidiary_id"
    # Define a target task for the "do nothing" branch 
    no_subsidiary_id = EmptyOperator(task_id="no_subsidiary_id")



    # ---------------------------
    # intercompanyTransferOrder TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_intercompanyTransferOrder_ids_task")
    def fetch_intercompanyTransferOrder_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_intercompanyTransferOrder_id())

            if id_list:
                r.rpush("intercompanyTransferOrder_id_queue", *id_list)
                print(f"Pushed {len(id_list)} intercompanyTransferOrder IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list intercompanyTransferOrder")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_intercompanyTransferOrder_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_intercompanyTransferOrder_details")
    def fetch_intercompanyTransferOrder_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed intercompanyTransferOrder records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("intercompanyTransferOrder_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} intercompanyTransferOrder IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_intercompanyTransferOrder_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list intercompanyTransferOrder returned no data")
        
        except Exception as e:
            logger.error(f"fetch_intercompanyTransferOrder_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_intercompanyTransferOrder_processing_path")
    def decide_intercompanyTransferOrder_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_intercompanyTransferOrder_details" if id_count > 0 else "no_intercompanyTransferOrder_id"
    # Define a target task for the "do nothing" branch 
    no_intercompanyTransferOrder_id = EmptyOperator(task_id="no_intercompanyTransferOrder_id")


    # ---------------------------
    # invoice TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_invoice_ids_task")
    def fetch_invoice_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_invoice_id())

            if id_list:
                r.rpush("invoice_id_queue", *id_list)
                print(f"Pushed {len(id_list)} invoice IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list invoice")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_invoice_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_invoice_details")
    def fetch_invoice_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed invoice records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("invoice_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} invoice IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_invoice_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list invoice returned no data")
        
        except Exception as e:
            logger.error(f"fetch_invoice_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_invoice_processing_path")
    def decide_invoice_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_invoice_details" if id_count > 0 else "no_invoice_id"
    # Define a target task for the "do nothing" branch 
    no_invoice_id = EmptyOperator(task_id="no_invoice_id")

    
    # ---------------------------
    # ITEM FULFILLMENT TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_itemfulfillment_ids_task")
    def fetch_itemfulfillment_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_item_fulfillment_id())

            if id_list:
                r.rpush("itemfulfillment_id_queue", *id_list)
                print(f"Pushed {len(id_list)} itemfulfillment IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list itemfulfillment")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_itemfulfillment_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_itemfulfillment_details")
    def fetch_itemfulfillment_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed itemfulfillment records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("itemfulfillment_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} itemfulfillment IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_item_fulfillment_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list itemfulfillment returned no data")
        
        except Exception as e:
            logger.error(f"fetch_itemfulfillment_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_itemfulfillment_processing_path")
    def decide_itemfulfillment_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_itemfulfillment_details" if id_count > 0 else "no_itemfulfillment_id"
    # Define a target task for the "do nothing" branch 
    no_itemfulfillment_id = EmptyOperator(task_id="no_itemfulfillment_id")
 
    # ---------------------------
    # itemReceipt TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_itemReceipt_ids_task")
    def fetch_itemReceipt_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_itemReceipt_id())

            if id_list:
                r.rpush("itemReceipt_id_queue", *id_list)
                print(f"Pushed {len(id_list)} itemReceipt IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list itemReceipt")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_itemReceipt_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_itemReceipt_details")
    def fetch_itemReceipt_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed itemReceipt records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("itemReceipt_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} itemReceipt IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_itemReceipt_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list itemReceipt returned no data")
        
        except Exception as e:
            logger.error(f"fetch_itemReceipt_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_itemReceipt_processing_path")
    def decide_itemReceipt_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_itemReceipt_details" if id_count > 0 else "no_itemReceipt_id"
    # Define a target task for the "do nothing" branch 
    no_itemReceipt_id = EmptyOperator(task_id="no_itemReceipt_id")

    
    # ---------------------------
    # LOCATION TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_location_ids_task")
    def fetch_location_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_location_id())

            if id_list:
                r.rpush("location_id_queue", *id_list)
                print(f"Pushed {len(id_list)} location IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list location")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_location_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_location_details")
    def fetch_location_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed location records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("location_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} location IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_location_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list location returned no data")
        
        except Exception as e:
            logger.error(f"fetch_location_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_location_processing_path")
    def decide_location_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_location_details" if id_count > 0 else "no_location_id"
    # Define a target task for the "do nothing" branch 
    no_location_id = EmptyOperator(task_id="no_location_id")

    
    # ---------------------------
    # DEPARTMENT TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_department_ids_task")
    def fetch_department_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_department_id())

            if id_list:
                r.rpush("department_id_queue", *id_list)
                print(f"Pushed {len(id_list)} department IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list department")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_department_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_department_details")
    def fetch_department_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed department records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("department_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} department IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_department_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list department returned no data")
        
        except Exception as e:
            logger.error(f"fetch_department_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_department_processing_path")
    def decide_department_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_department_details" if id_count > 0 else "no_department_id"
    # Define a target task for the "do nothing" branch 
    no_department_id = EmptyOperator(task_id="no_department_id")

    
    # ---------------------------
    # TRANSFERORDER TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_transferOrder_ids_task")
    def fetch_transferOrder_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_transferOrder_id())

            if id_list:
                r.rpush("transferOrder_id_queue", *id_list)
                print(f"Pushed {len(id_list)} transferOrder IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list transferOrder")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_transferOrder_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_transferOrder_details")
    def fetch_transferOrder_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed transferOrder records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("transferOrder_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} transferOrder IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_transferOrder_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list transferOrder returned no data")
        
        except Exception as e:
            logger.error(f"fetch_transferOrder_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_transferOrder_processing_path")
    def decide_transferOrder_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_transferOrder_details" if id_count > 0 else "no_transferOrder_id"
    # Define a target task for the "do nothing" branch 
    no_transferOrder_id = EmptyOperator(task_id="no_transferOrder_id")


    
    # ---------------------------
    # VENDOR TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_vendor_ids_task")
    def fetch_vendor_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_vendor_id())

            if id_list:
                r.rpush("vendor_id_queue", *id_list)
                print(f"Pushed {len(id_list)} vendor IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list vendor")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_vendor_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_vendor_details")
    def fetch_vendor_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed vendor records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("vendor_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} vendor IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_vendor_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list vendor returned no data")
        
        except Exception as e:
            logger.error(f"fetch_vendor_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_vendor_processing_path")
    def decide_vendor_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_vendor_details" if id_count > 0 else "no_vendor_id"
    # Define a target task for the "do nothing" branch 
    no_vendor_id = EmptyOperator(task_id="no_vendor_id")
    # ---------------------------
    # DEPENDENCIES / WORKFLOW
    # ---------------------------
    
    # ---------------------------
    # DEPARTMENT TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_vendorBill_ids_task")
    def fetch_vendorBill_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_vendorBill_id())

            if id_list:
                r.rpush("vendorBill_id_queue", *id_list)
                print(f"Pushed {len(id_list)} vendorBill IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list vendorBill")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_vendorBill_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_vendorBill_details")
    def fetch_vendorBill_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed vendorBill records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("vendorBill_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} vendorBill IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_vendorBill_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list vendorBill returned no data")
        
        except Exception as e:
            logger.error(f"fetch_vendorBill_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_vendorBill_processing_path")
    def decide_vendorBill_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_vendorBill_details" if id_count > 0 else "no_vendorBill_id"
    # Define a target task for the "do nothing" branch 
    no_vendorBill_id = EmptyOperator(task_id="no_vendorBill_id")

    
    # ---------------------------
    # DEPARTMENT TASKS DEFINITION
    # ---------------------------
    @task(task_id="fetch_vendorCategory_ids_task")
    def fetch_vendorCategory_ids_task():
        try:
            resource_name, id_list, resource_data = asyncio.run(list_vendorCategory_id())

            if id_list:
                r.rpush("vendorCategory_id_queue", *id_list)
                print(f"Pushed {len(id_list)} vendorCategory IDs to Redis queue")
            if resource_data:
                asyncio.run(safe_upload(resource_data, resource_name))
            else:
                logger.warning("No IDs returned from list vendorCategory")
            return len(id_list)
        except Exception as e:
            logger.error(f"fetch_vendorCategory_ids failed: {e}", exc_info=True)
            raise
    @task(task_id="fetch_vendorCategory_details")
    def fetch_vendorCategory_details_task():
        """
        Pops IDs from the Redis queue and fetches/uploads detailed vendorCategory records.
        This task runs only if IDs were found.
        """
        try:
            all_ids = []
            # Pop all IDs from the queue until empty
            while True:
                cust_id = r.lpop("vendorCategory_id_queue")
                if cust_id is None:
                    break
                all_ids.append(cust_id.decode('utf-8'))
            
            # The previous task ensures all_ids is not empty if this runs.
            logger.info(f"Loaded {len(all_ids)} vendorCategory IDs from Redis queue")

            resource_name, data, _ = asyncio.run(list_vendorCategory_details(all_ids))
            if data:
                asyncio.run(safe_upload(data, resource_name))
            else:
                logger.warning("list vendorCategory returned no data")
        
        except Exception as e:
            logger.error(f"fetch_vendorCategory_details failed: {e}", exc_info=True)
            raise
    @task.branch(task_id="decide_vendorCategory_processing_path")
    def decide_vendorCategory_processing_path(id_count: int) -> str:
        """
        Uses the output (id_count) from the previous task to decide 
        which downstream task_id to execute next.
        """
        return "fetch_vendorCategory_details" if id_count > 0 else "no_vendorCategory_id"
    # Define a target task for the "do nothing" branch 
    no_vendorCategory_id = EmptyOperator(task_id="no_vendorCategory_id")

    #vendorCategory flow
    vendorCategory_id = fetch_vendorCategory_ids_task()
    decision = decide_vendorCategory_processing_path(vendorCategory_id)
    decision >> [fetch_vendorCategory_details_task(), no_vendorCategory_id]

    #vendorBill flow
    vendorBill_id = fetch_vendorBill_ids_task()
    decision = decide_vendorBill_processing_path(vendorBill_id)
    decision >> [fetch_vendorBill_details_task(), no_vendorBill_id]

    #vendor flow
    vendor_id = fetch_vendor_ids_task()
    decision = decide_vendor_processing_path(vendor_id)
    decision >> [fetch_vendor_details_task(), no_vendor_id]

    #transfer order flow
    transfer_order_id = fetch_transferOrder_ids_task()
    decision = decide_transferOrder_processing_path(transfer_order_id)
    decision >> [fetch_transferOrder_details_task(), no_transferOrder_id]
    #subsidiary flow
    subsidiary_ids = fetch_subsidiary_ids_task()
    decision = decide_subsidiary_processing_path(subsidiary_ids)
    decision >> [fetch_subsidiary_details_task(), no_subsidiary_id]

    #location flow
    location_ids = fetch_location_ids_task()
    decision = decide_location_processing_path(location_ids)
    decision >> [fetch_location_details_task(), no_location_id]

    #item receipt flow
    item_receipt_ids = fetch_itemReceipt_ids_task()
    decision = decide_itemReceipt_processing_path(item_receipt_ids)
    decision >> [fetch_itemReceipt_details_task(), no_itemReceipt_id]

    #item fulfillment flow
    item_fulfillment_ids = fetch_itemfulfillment_ids_task()
    decision = decide_itemfulfillment_processing_path(item_fulfillment_ids)
    decision >> [fetch_itemfulfillment_details_task(), no_itemfulfillment_id]

    #invoice flow
    invoice_ids = fetch_invoice_ids_task()
    decision = decide_invoice_processing_path(invoice_ids)
    decision >> [fetch_invoice_details_task(), no_invoice_id]

    #intercompanyTransferOrder flow
    intercompanyTransferOrder_ids = fetch_intercompanyTransferOrder_ids_task()
    decision = decide_intercompanyTransferOrder_processing_path(intercompanyTransferOrder_ids)
    decision >> [fetch_intercompanyTransferOrder_details_task(), no_intercompanyTransferOrder_id]

    #department flow
    department_ids = fetch_department_ids_task()
    decision = decide_department_processing_path(department_ids)
    decision >> [fetch_department_details_task(), no_department_id]

    #assembly item flow
    assembly_item_ids = fetch_assembly_items_ids_task()
    decision = decide_assembly_items_processing_path(assembly_item_ids)
    decision >> [fetch_assembly_items_details_task(), no_assembly_items_id]
    #account flow
    account_ids = fetch_account_ids_task()
    decision = decide_account_processing_path(account_ids)
    decision >> [fetch_account_details_task(), no_account_id]

    # Customer flow
    customer_ids = fetch_customer_ids_task()
    decision = decide_processing_path_task(customer_ids)
    decision >> [fetch_customer_details_task(), no_customer_ids]


    # # Inventory Items flow
    inventory_item_ids = fetch_inventory_items_ids_task()
    decision = decide_inventory_processing_path(inventory_item_ids)
    decision >> [fetch_inventory_item_details_task(), no_inventory_items_id]


    ## Inventory Number flow
    inventory_number_ids = fetch_inventory_number_ids_task()
    decision = decide_inventory_number_processing_path(inventory_number_ids)
    decision >> [fetch_inventory_number_details_task(), no_inventory_numbers_id]
    
    ## Inventory Number flow
    inventory_transfer_ids = fetch_inventory_transfer_ids_task()
    decision = decide_inventory_transfer_processing_path(inventory_transfer_ids)
    decision >> [fetch_inventory_transfer_details_task(), no_inventory_transfers_id]
    
    ## Inventory Count flow
    inventory_count_ids = fetch_inventory_count_ids_task()
    decision = decide_inventory_count_processing_path(inventory_count_ids)
    decision >> [fetch_inventory_count_details_task(), no_inventory_counts_id]

    # Purchase Order flow
    purchase_order_ids = fetch_purchase_orders_ids_task()
    decision = decide_purchase_orders_processing_path(purchase_order_ids)
    decision >> [fetch_purchase_orders_details_task(), no_purchase_orders_id]
    
    # Sales Order flow
    sales_order_ids = fetch_sales_orders_ids_task()
    decision = decide_sales_orders_processing_path(sales_order_ids)
    decision >> [fetch_sales_orders_details_task(), no_sales_orders_id]

    start >> [inventory_item_ids,
              customer_ids
              ,inventory_number_ids
              ,inventory_transfer_ids
              ,inventory_count_ids
              ,purchase_order_ids
              ,sales_order_ids
              ,account_ids
              ,assembly_item_ids
              ,department_ids
              ,intercompanyTransferOrder_ids
              ,invoice_ids
              ,item_fulfillment_ids
              ,item_receipt_ids
              ,location_ids
              ,subsidiary_ids
              ,transfer_order_id
              ,vendor_id
              ,vendorBill_id
              ,vendorCategory_id
              ]

# Instantiate the DAG
netsuite_dag = netsuite_pipeline()