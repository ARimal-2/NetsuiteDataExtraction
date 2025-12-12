from framework.list_ids import fetch_all_ids
from urls import VENDOR_ID_URL

async def list_vendor_id():
    return await fetch_all_ids(VENDOR_ID_URL, "list_vendor_id")

 
