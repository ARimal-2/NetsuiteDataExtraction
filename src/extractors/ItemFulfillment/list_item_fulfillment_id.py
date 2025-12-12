

from framework.list_ids import fetch_all_ids
from urls import ITEM_FULFILLMENT_ID_URL

async def list_item_fulfillment_id():
    return await fetch_all_ids(ITEM_FULFILLMENT_ID_URL, "list_item_fulfillment_id")
