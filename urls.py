import os
from dotenv import load_dotenv

load_dotenv()
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
if not ACCOUNT_ID:
	raise RuntimeError(
		"Environment variable `ACCOUNT_ID` is not set.\n"
		"Ensure your `.env` file is present and loaded in the Airflow environment or set `ACCOUNT_ID` in the container environment.\n"
		"If using docker-compose, make sure `env_file: - ${AIRFLOW_PROJ_DIR:-.}/.env` is configured in `docker-compose.yaml`."
	)

base_url = f"https://{ACCOUNT_ID.lower().replace('_', '-')}.suitetalk.api.netsuite.com/services/rest/record/v1"

Customer_ID_URL = f"{base_url}/customer"
Customer_URL = f"{base_url}/customer/{{id}}"

Customer_Deposit_URL = f"{base_url}/customerDeposit/{{id}}"
Customer_Deposit_ID_URL = f"{base_url}/customerDeposit"


CUSTOMER_CATEGORY_URL = f"{base_url}/customerCategory"
CUSTOMER_CATEGORY_LIST_URL = f"{base_url}/customerCategory/{{id}}"

CUSTOMER_PAYMENT_ID_URL = f"{base_url}/customerPayment"
CUSTOMER_PAYMENT_LIST_URL = f"{base_url}/customerPayment/{{id}}"


CUSTOMER_MESSAGE_ID_URL = f"{base_url}/customerMessage"
CUSTOMER_MESSAGE_LIST_URL = f"{base_url}/customerMessage/{{id}}"

CUSTOMER_SUBSIDIARY_RELATIONSHIP_ID_URL = f"{base_url}/customerSubsidiaryRelationship"
CUSTOMER_SUBSIDIARY_RELATIONSHIP_LIST_URL = f"{base_url}/customerSubsidiaryRelationship/{{id}}"


INVENTORY_ITEM_ID_URL = f"{base_url}/inventoryItem"
INVENTORY_ITEM_LIST_URL = f"{base_url}/inventoryItem/{{id}}"


INVENTORY_NUMBER_URL = f"{base_url}/inventoryNumber"
INVENTORY_DETAILS_URL = f"{base_url}/inventoryNumber/{{id}}"

INVENTORY_TRANSFER_ID_URL = f"{base_url}/inventoryTransfer"
INVENTORY__TRANSFER_DETAILS_URL = f"{base_url}/inventoryTransfer/{{id}}"

SALES_ORDER_ID_URL = f"{base_url}/salesOrder"
SALES_ORDER_DETAILS_URL = f"{base_url}/salesOrder/{{id}}"


PURHCHASE_ORDER_ID_URL = f"{base_url}/purchaseOrder"
PURCHASE_ORDER_DETAILS_URL = f"{base_url}/purchaseOrder/{{id}}"

INVENTORY_COUNT_ID_URL = f"{base_url}/inventoryCount"
INVENTORY_COUNT_DETAILS_URL = f"{base_url}/inventoryCount/{{id}}"

ACCOUNT_ID_URL = f"{base_url}/account"
ACCOUNT_DETAILS_URL = f"{base_url}/account/{{id}}"

ASSEMBLY_ITEM_ID_URL = f"{base_url}/assemblyItem"
ASSEMBLY_ITEM_DETAILS_URL = f"{base_url}/assemblyItem/{{id}}"

DEPARTMENT_ID_URL = f"{base_url}/department"
DEPARTMENT_DETAILS_URL = f"{base_url}/department/{{id}}"

INTERCOMPANY_TRANSFER_ORDER_URL = f"{base_url}/interCompanyTransferOrder"
INTERCOMPANY_TRANSFER_ORDER_DETAILS_URL = f"{base_url}/interCompanyTransferOrder/{{id}}"

INVOICE_ID_URL = f"{base_url}/invoice"
INVOICE_DETAILS_URL = f"{base_url}/invoice/{{id}}"

ITEM_FULFILLMENT_ID_URL = f"{base_url}/itemFulfillment"
ITEM_FULFILLMENT_DETAILS_URL = f"{base_url}/itemFulfillment/{{id}}"

ITEM_RECEIPT_ID_URL = f"{base_url}/itemReceipt"
ITEM_RECEIPT_DETAILS_URL = f"{base_url}/itemReceipt/{{id}}"

LOCATION_ID_URL = f"{base_url}/location"
LOCATION_DETAILS_URL = f"{base_url}/location/{{id}}"	

SUBSIDIARY_ID_URL = f"{base_url}/subsidiary"
SUBSIDIARY_DETAILS_URL = f"{base_url}/subsidiary/{{id}}"

TRANSFER_ORDER_ID_URL = f"{base_url}/transferOrder"
TRANSFER_ORDER_DETAILS_URL = f"{base_url}/transferOrder/{{id}}"

VENDOR_ID_URL = f"{base_url}/vendor"
VENDOR_DETAILS_URL = f"{base_url}/vendor/{{id}}"

VENDOR_BILL_ID_URL = f"{base_url}/vendorBill"
VENDOR_BILL_DETAILS_URL = f"{base_url}/vendorBill/{{id}}"

VENDOR_CATEGORY_ID_URL = f"{base_url}/vendorCategory"
VENDOR_CATEGORY_DETAILS_URL = f"{base_url}/vendorCategory/{{id}}"

