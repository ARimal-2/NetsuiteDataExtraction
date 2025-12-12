from framework.list_ids import fetch_all_ids
from urls import VENDOR_CATEGORY_ID_URL
async def list_vendorCategory_id():
    """Entry point to fetch all vendorCategory."""
    return await fetch_all_ids(VENDOR_CATEGORY_ID_URL, "list_vendorCategory_id")
