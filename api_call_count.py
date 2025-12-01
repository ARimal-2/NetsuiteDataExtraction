from requests_oauthlib import OAuth1
import os
from dotenv import load_dotenv
import requests

load_dotenv()
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

RECORD_TYPES = ["customer","customerMessage","customerCategory","customerPayment","customerSubsidiaryRelationship","inventoryNumber","inventoryTransfer", "inventoryItem", "salesOrder"]  # Add more if needed
PAGE_SIZE = 1000
auth = OAuth1(
    client_key="6a09f99c617ac806e87b5397aa4177f6853eadd8220b87d6379a375ca9acc7c9",
    client_secret="0c5d176f12fe1d84ca57223800dff8880875db5c838797ab8428dbd4033a5cea",
    resource_owner_key="b0be1133b87dcd40725785bfc60bde73f5e5eda901e6c0e16997dc29e3a77985",
    resource_owner_secret="12144b6a62bc7ad755d6723cc60935cb46840941f870704a0d0935a511e12983",
    realm="1301862_SB1",
    signature_method="HMAC-SHA256",
)
# -----------------------
def get_count_suiteql(record_type):
    suiteql_url = f"https://{ACCOUNT_ID.lower().replace('_','-')}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    query = {"q": f"SELECT COUNT(*) AS total FROM {record_type}"}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(suiteql_url, auth=auth, headers=headers, json=query)
        response.raise_for_status()
        data = response.json()
        return data["items"][0]["total"]
    except Exception as e:
        return None

# -----------------------
# GET + PAGING FUNCTION (fallback)
# -----------------------
def get_count_get(record_type):
    base_url = f"https://{ACCOUNT_ID.lower().replace('_','-')}.suitetalk.api.netsuite.com/services/rest/record/v1"
    url = f"{base_url}/{record_type}"
    headers = {"Content-Type": "application/json"}
    count = 0
    offset = 0
    while True:
        params = {"limit": PAGE_SIZE, "offset": offset}
        response = requests.get(url, auth=auth, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching {record_type}: {response.status_code} {response.text}")
            return None
        data = response.json()
        items = data.get("items", [])
        count += len(items)
        if not data.get("hasMore", False):
            break
        offset += PAGE_SIZE
    return count

# -----------------------
# MAIN LOOP
# -----------------------
if __name__ == "__main__":
    for record_type in RECORD_TYPES:
        total = get_count_suiteql(record_type)
        if total is None:
            total = get_count_get(record_type)
        print(f"Record type: {record_type} | Total records: {total}")