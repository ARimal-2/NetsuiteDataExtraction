from urls import SUBSIDIARY_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_subsidiary_details(ids):

    return await fetch_all_details(
        url_template=SUBSIDIARY_DETAILS_URL,
        resource_name="list_subsidiary_details",
        ids=ids
    )
