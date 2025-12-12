from urls import LOCATION_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_location_details(ids):

    return await fetch_all_details(
        url_template=LOCATION_DETAILS_URL,
        resource_name="list_location_details",
        ids=ids
    )
