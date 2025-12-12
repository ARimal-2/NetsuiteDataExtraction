from framework.list_ids import fetch_all_ids
from urls import INVENTORY_NUMBER_URL

async def list_inventory_id():
    return await fetch_all_ids(INVENTORY_NUMBER_URL, "list_inventory_id")
