from urls import ITEM_RECEIPT_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_itemReceipt_details(ids):

    return await fetch_all_details(
        url_template=ITEM_RECEIPT_DETAILS_URL,
        resource_name="list_itemReceipt_details",
        ids=ids
    )
