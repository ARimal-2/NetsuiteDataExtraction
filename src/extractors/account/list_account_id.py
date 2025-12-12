from framework.list_ids import fetch_all_ids
from urls import ACCOUNT_ID_URL

async def list_account_id():
    return await fetch_all_ids(ACCOUNT_ID_URL, "list_account_id")
