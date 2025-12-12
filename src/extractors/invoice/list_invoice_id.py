
from framework.list_ids import fetch_all_ids
from urls import INVOICE_ID_URL

async def list_invoice_id():
    return await fetch_all_ids(INVOICE_ID_URL, "list_invoice_id")

 