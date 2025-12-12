
from urls import ASSEMBLY_ITEM_DETAILS_URL
from framework.list_details import fetch_all_details


async def list_assembly_items(ids):

    return await fetch_all_details(
        url_template=ASSEMBLY_ITEM_DETAILS_URL,
        resource_name="list_assembly_items",
        ids=ids
    )
