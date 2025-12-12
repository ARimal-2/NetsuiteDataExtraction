
from urls import ASSEMBLY_ITEM_ID_URL
from framework.list_ids import fetch_all_ids

async def list_assembly_items_id():
    return await fetch_all_ids(ASSEMBLY_ITEM_ID_URL, "list_assembly_items_id")

