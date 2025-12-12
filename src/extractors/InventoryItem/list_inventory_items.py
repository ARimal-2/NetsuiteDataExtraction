from urls import INVENTORY_ITEM_LIST_URL
from framework.list_details import fetch_all_details

async def list_inventory_items(ids):

    return await fetch_all_details(
        url_template=INVENTORY_ITEM_LIST_URL,
        resource_name="list_inventory_items",
        ids=ids
    )
