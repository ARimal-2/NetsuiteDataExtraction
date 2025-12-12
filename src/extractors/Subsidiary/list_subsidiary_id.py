
from framework.list_ids import fetch_all_ids
from urls import SUBSIDIARY_ID_URL

async def list_subsidiary_id():
    return await fetch_all_ids(SUBSIDIARY_ID_URL, "list_subsidiary_id")

 