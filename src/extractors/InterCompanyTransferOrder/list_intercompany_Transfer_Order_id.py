from framework.list_ids import fetch_all_ids
from urls import INTERCOMPANY_TRANSFER_ORDER_URL

async def list_intercompanyTransferOrder_id():
    return await fetch_all_ids(INTERCOMPANY_TRANSFER_ORDER_URL, "list_intercompanyTransferOrder_id")
