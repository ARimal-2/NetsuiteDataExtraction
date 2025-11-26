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
