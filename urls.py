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
Customer_URL = f"{base_url}/customer/{{id}}?expandSubResources=true"

Customer_Deposit_URL = f"{base_url}/customerDeposit/{{id}}"
Customer_Deposit_ID_URL = f"{base_url}/customerDeposit"


CUSTOMER_CATEGORY_URL = f"{base_url}/customerCategory"
CUSTOMER_CATEGORY_LIST_URL = f"{base_url}/customerCategory/{{id}}?expandSubResources=true"

CUSTOMER_PAYMENT_ID_URL = f"{base_url}/customerPayment"
CUSTOMER_PAYMENT_LIST_URL = f"{base_url}/customerPayment/{{id}}?expandSubResources=true"


CUSTOMER_MESSAGE_ID_URL = f"{base_url}/customerMessage"
CUSTOMER_MESSAGE_LIST_URL = f"{base_url}/customerMessage/{{id}}?expandSubResources=true"

CUSTOMER_SUBSIDIARY_RELATIONSHIP_ID_URL = f"{base_url}/customerSubsidiaryRelationship"
CUSTOMER_SUBSIDIARY_RELATIONSHIP_LIST_URL = f"{base_url}/customerSubsidiaryRelationship/{{id}}?expandSubResources=true"


INVENTORY_ITEM_ID_URL = f"{base_url}/inventoryItem"
INVENTORY_ITEM_LIST_URL = f"{base_url}/inventoryItem/{{id}}?expandSubResources=true"


INVENTORY_NUMBER_URL = f"{base_url}/inventoryNumber"
INVENTORY_DETAILS_URL = f"{base_url}/inventoryNumber/{{id}}?expandSubResources=true"

INVENTORY_TRANSFER_ID_URL = f"{base_url}/inventoryTransfer"
INVENTORY__TRANSFER_DETAILS_URL = f"{base_url}/inventoryTransfer/{{id}}?expandSubResources=true"

SALES_ORDER_ID_URL = f"{base_url}/salesOrder"
SALES_ORDER_DETAILS_URL = f"{base_url}/salesOrder/{{id}}?expandSubResources=true"

SALES_ROLE_ID_URL = f"{base_url}/salesRole"
SALES_ROLE_DETAILS = f"{base_url}/salesRole/{{id}}?expandSubResources=true"

SALES_TAX_ITEM_ID_URL = f"{base_url}/salesTaxItem"
SALES_TAX_ITEM_DETAILS = f"{base_url}/salesTaxItem/{{id}}?expandSubResources=true"

PURHCHASE_ORDER_ID_URL = f"{base_url}/purchaseOrder"
PURCHASE_ORDER_DETAILS_URL = f"{base_url}/purchaseOrder/{{id}}?expandSubResources=true"

INVENTORY_COUNT_ID_URL = f"{base_url}/inventoryCount"
INVENTORY_COUNT_DETAILS_URL = f"{base_url}/inventoryCount/{{id}}?expandSubResources=true"

ACCOUNT_ID_URL = f"{base_url}/account"
ACCOUNT_DETAILS_URL = f"{base_url}/account/{{id}}?expandSubResources=true"

ASSEMBLY_ITEM_ID_URL = f"{base_url}/assemblyItem"
ASSEMBLY_ITEM_DETAILS_URL = f"{base_url}/assemblyItem/{{id}}?expandSubResources=true"

DEPARTMENT_ID_URL = f"{base_url}/department"
DEPARTMENT_DETAILS_URL = f"{base_url}/department/{{id}}?expandSubResources=true"