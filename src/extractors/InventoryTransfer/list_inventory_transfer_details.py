
from urls import INVENTORY__TRANSFER_DETAILS_URL

from framework.list_details import fetch_all_details


async def list_inventory_transfer_details(ids):

    return await fetch_all_details(
        url_template=INVENTORY__TRANSFER_DETAILS_URL,
        resource_name="list_inventory_transfer_details",
        ids=ids
    )
