from framework.list_ids import fetch_all_ids
from urls import DEPARTMENT_ID_URL

async def list_department_id():
    return await fetch_all_ids(DEPARTMENT_ID_URL, "list_department_id")
