from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import redis

from src.extractors.Customer.customer_flow import customer_flow
from src.extractors.inventoryCount.inventory_count_flow import inventory_count_flow
from src.extractors.account.account_flow import account_flow
from src.extractors.AssemblyItem.assemply_item_flow import assembly_item_flow
from src.extractors.department.department_flow import department_flow
from src.extractors.InterCompanyTransferOrder.interCompanyTransferOrder_flow import (
    interCompanyTransferOrder_flow,
)
from src.extractors.InventoryItem.inventort_item_flow import InventoryItem_flow
from src.extractors.InventoryNumber.inventory_number_flow import inventoryNumber_flow
from src.extractors.InventoryTransfer.inventoryTransfer_flow import inventoryTransfer_flow
from src.extractors.invoice.invoice_flow import invoice_flow
from src.extractors.ItemFulfillment.itemFulfillment_flow import itemFulfillment_flow
from src.extractors.location.location_flow import location_flow
from src.extractors.PurchaseOrder.PurchaseOrder_flow import PurchaseOrder_flow
from src.extractors.SalesOrder.SalesOrder_flow import SalesOrder_flow
from src.extractors.vendorCategory.vendorCategory_flow import vendorCategory_flow
from src.extractors.vendorBill.vendorBill_flow import vendorBill_flow
from src.extractors.vendor.vendor_flow import vendor_flow
from src.extractors.TransferOrder.transferOrder_flow import transferOrder_flow
from src.extractors.Subsidiary.subsidiary_flow import subsidiary_flow
from src.extractors.itemReceipt.itemReceipt_flow import itemReceipt_flow


# ------------------------------------------------------------
# Default args
# ------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2)
}


# ------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------
@dag(
    dag_id="netsuite_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",     
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,         
    tags=["netsuite", "etl"],
)
def netsuite_pipeline():

    # Redis connection (shared by flows)
    r = redis.Redis(host="redis", port=6379, db=0)

    start = EmptyOperator(task_id="start")

  

    # cust_ids = customer_flow(r)
    # invCount_ids = inventory_count_flow(r)
    # acc_ids = account_flow(r)
    # assem_ids = assembly_item_flow(r)
    # dept_ids = department_flow(r)
    # ICTO_ids = interCompanyTransferOrder_flow(r)

    # InvItem_ids = InventoryItem_flow(r)

    # InvNum_ids = inventoryNumber_flow(r)
    # invTrans_ids = inventoryTransfer_flow(r)

    # itemRec_ids = itemReceipt_flow(r)
    #invoice_ids = invoice_flow(r)

    # itm_full_ids = itemFulfillment_flow(r)
    # location_ids = location_flow(r)
    # PO_ids = PurchaseOrder_flow(r)
    SO_ids = SalesOrder_flow(r)
    # subsi_ids = subsidiary_flow(r)
    # TO_ids = transferOrder_flow(r)
    # vendor_ids = vendor_flow(r)
    # venBill_ids = vendorBill_flow(r)
    # venCat_ids = vendorCategory_flow(r)

    # ----------------------------
    # Dependencies
    # ----------------------------
    start >> [
        # InvItem_ids,
        # itemRec_ids,
        SO_ids,
    ]


# DAG object
netsuite_dag = netsuite_pipeline()
