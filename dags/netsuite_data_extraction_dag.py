from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import redis

from src.extractors.Customer.customer_flow import customer_flow
from src.extractors.inventoryCount.inventory_count_flow import inventory_count_flow
from src.extractors.account.account_flow import account_flow
from src.extractors.AssemblyItem.assemply_item_flow import assembly_item_flow
from src.extractors.department.department_flow import department_flow
from src.extractors.InterCompanyTransferOrder.interCompanyTransferOrder_flow import interCompanyTransferOrder_flow
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
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="netsuite_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    default_args=default_args,
    catchup=False,
)
def netsuite_pipeline():

    r = redis.Redis(host="redis", port=6379, db=0)
    start = EmptyOperator(task_id="start")

    #customer
    cust_ids = customer_flow(r)

    # AssemblyItems
    assem_ids = assembly_item_flow(r)

    #Department
    dept_ids = department_flow(r)
    
    #inventoryCountFlow
    invCount_ids = inventory_count_flow(r)

    #accounts
    acc_ids = account_flow(r)

    #interCompanyTransferOrder
    ICTO_ids = interCompanyTransferOrder_flow(r)

    #inventoryItem
    InvItem_ids = InventoryItem_flow(r)

    #inventoryNumber
    InvNum_ids = inventoryNumber_flow(r)

    #inventoryTransfer
    invTrans_ids = inventoryTransfer_flow(r)

    itemRec_ids = itemReceipt_flow(r)

    #invoice
    invoice_ids = invoice_flow(r)

    #itemFulfillment
    itm_full_ids = itemFulfillment_flow(r)

    #location
    location_ids = location_flow(r)

    #PurchaseOrder
    PO_ids = PurchaseOrder_flow(r)

    #SalesOrder
    SO_ids = SalesOrder_flow(r)

    #subsidiary
    subsi_ids = subsidiary_flow(r)

    #transferOrder
    TO_ids = transferOrder_flow(r)

    #vendor
    vendor_ids = vendor_flow(r)

    #vendorBill
    venBill_ids = vendorBill_flow(r)

    #vendorCategory
    venCat_ids = vendorCategory_flow(r)



    start >> [cust_ids
              , invCount_ids
              ,acc_ids,
              assem_ids,
              dept_ids,
              ICTO_ids,
              InvItem_ids,
              InvNum_ids,
              invTrans_ids,
              itemRec_ids,
              invoice_ids,
              itm_full_ids,
              location_ids,
              PO_ids,
              SO_ids,
              subsi_ids,
              TO_ids,
              vendor_ids,
              venBill_ids,
              venCat_ids
              ]

netsuite_dag = netsuite_pipeline()
