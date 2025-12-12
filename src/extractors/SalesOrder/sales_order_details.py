
from urls import SALES_ORDER_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_sales_order(ids):

    return await fetch_all_details(
        url_template=SALES_ORDER_DETAILS_URL,
        resource_name="list_sales_order",
        ids=ids
    )
