
from urls import ACCOUNT_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_account_details(ids):

    return await fetch_all_details(
        url_template=ACCOUNT_DETAILS_URL,
        resource_name="list_account_details",
        ids=ids
    )
