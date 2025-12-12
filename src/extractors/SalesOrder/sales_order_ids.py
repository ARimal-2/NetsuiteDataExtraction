
from framework.list_ids import fetch_all_ids
from urls import SALES_ORDER_ID_URL

async def list_sales_order_ids():
    return await fetch_all_ids(SALES_ORDER_ID_URL, "list_sales_order_ids")

