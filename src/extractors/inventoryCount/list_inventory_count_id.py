from framework.list_ids import fetch_all_ids
from urls import INVENTORY_COUNT_ID_URL

async def list_inventory_count_id():
    return await fetch_all_ids(INVENTORY_COUNT_ID_URL, "list_inventory_count_id")
