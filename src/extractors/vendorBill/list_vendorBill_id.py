from framework.list_ids import fetch_all_ids

from urls import VENDOR_BILL_ID_URL

async def list_vendorBill_id():
    """Entry point to fetch all vendorBill."""
    return await fetch_all_ids(VENDOR_BILL_ID_URL, "list_vendorBill_id")
