
from framework.list_ids import fetch_all_ids
from urls import TRANSFER_ORDER_ID_URL

async def list_transferOrder_id():
    return await fetch_all_ids(TRANSFER_ORDER_ID_URL, "list_transferOrder_id")

 