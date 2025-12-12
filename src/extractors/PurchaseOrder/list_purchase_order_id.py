from framework.list_ids import fetch_all_ids
from urls import PURHCHASE_ORDER_ID_URL

async def list_purchase_order_id():
    return await fetch_all_ids(PURHCHASE_ORDER_ID_URL, "list_purchase_order_id")

