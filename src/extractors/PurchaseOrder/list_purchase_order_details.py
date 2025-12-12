
from urls import PURCHASE_ORDER_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_purchase_order(ids):

    return await fetch_all_details(
        url_template=PURCHASE_ORDER_DETAILS_URL,
        resource_name="list_purchase_order",
        ids=ids
    )

