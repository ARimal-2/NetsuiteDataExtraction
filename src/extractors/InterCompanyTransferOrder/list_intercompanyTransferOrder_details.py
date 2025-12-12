
from urls import INTERCOMPANY_TRANSFER_ORDER_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_intercompanyTransferOrder_details(ids):

    return await fetch_all_details(
        url_template=INTERCOMPANY_TRANSFER_ORDER_DETAILS_URL,
        resource_name="list_intercompanyTransferOrder_details",
        ids=ids
    )
