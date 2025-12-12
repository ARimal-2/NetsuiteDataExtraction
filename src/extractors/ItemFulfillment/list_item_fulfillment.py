
from urls import ITEM_FULFILLMENT_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_item_fulfillment_details(ids):

    return await fetch_all_details(
        url_template=ITEM_FULFILLMENT_DETAILS_URL,
        resource_name="list_item_fulfillment_details",
        ids=ids
    )
