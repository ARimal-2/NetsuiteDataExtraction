
from urls import Customer_ID_URL
from framework.list_ids import fetch_all_ids


async def list_customers_ids():
    return await fetch_all_ids(Customer_ID_URL, "list_customers_ids")
