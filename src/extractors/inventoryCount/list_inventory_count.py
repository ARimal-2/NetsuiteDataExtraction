from urls import INVENTORY_COUNT_DETAILS_URL
from framework.list_details import fetch_all_details

async def list_inventory_count(ids):

    return await fetch_all_details(
        url_template=INVENTORY_COUNT_DETAILS_URL,
        resource_name="list_inventory_count",
        ids=ids
    )
