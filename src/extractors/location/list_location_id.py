
from framework.list_ids import fetch_all_ids
from urls import LOCATION_ID_URL

async def list_location_id():
    return await fetch_all_ids(LOCATION_ID_URL, "list_location_id")

 