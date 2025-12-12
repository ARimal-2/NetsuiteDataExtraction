from framework.list_ids import fetch_all_ids
from urls import INVENTORY_TRANSFER_ID_URL

async def list_inventory_transfer_id():
    return await fetch_all_ids(INVENTORY_TRANSFER_ID_URL, "list_inventory_transfer_id")

