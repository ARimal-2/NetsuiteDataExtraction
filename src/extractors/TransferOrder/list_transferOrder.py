
from urls import TRANSFER_ORDER_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_transferOrder_details(ids):

    return await fetch_all_details(
        url_template=TRANSFER_ORDER_DETAILS_URL,
        resource_name="list_transferOrder_details",
        ids=ids
    )
