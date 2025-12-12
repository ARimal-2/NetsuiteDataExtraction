
from urls import DEPARTMENT_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_department_details(ids):

    return await fetch_all_details(
        url_template=DEPARTMENT_DETAILS_URL,
        resource_name="list_department_details",
        ids=ids
    )
