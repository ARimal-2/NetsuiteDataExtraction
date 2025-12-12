from urls import INVOICE_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_invoice_details(ids):

    return await fetch_all_details(
        url_template=INVOICE_DETAILS_URL,
        resource_name="list_invoice_details",
        ids=ids
    )
