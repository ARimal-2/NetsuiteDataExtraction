
from urls import VENDOR_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_vendor_details(ids):

    return await fetch_all_details(
        url_template=VENDOR_DETAILS_URL,
        resource_name="list_vendor_details",
        ids=ids
    )
