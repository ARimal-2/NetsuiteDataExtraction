
from framework.list_ids import fetch_all_ids
from urls import ITEM_RECEIPT_DETAILS_URL

async def list_itemReceipt_id():
    return await fetch_all_ids(ITEM_RECEIPT_DETAILS_URL, "list_itemReceipt_id")

 