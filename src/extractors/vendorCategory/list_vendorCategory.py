from framework.list_details import fetch_all_details

from urls import VENDOR_DETAILS_URL

async def list_vendorCategory_details(vendorCategory_ids):
    """Fetch all vendorCategory given the list of IDs"""
    return await fetch_all_details(VENDOR_DETAILS_URL, "list_vendorCategory_details", vendorCategory_ids)
