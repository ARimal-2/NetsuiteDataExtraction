from urls import Customer_URL
from framework.list_details import fetch_all_details


async def customers_list(ids):
    return await fetch_all_details(
        url_template=Customer_URL,
        resource_name="customers_list",
        ids=ids
    )
