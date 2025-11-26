# import asyncio
# import aiohttp
# import logging
# import json
# import os
# from datetime import datetime, timezone
# from src.extractors.utils import (
#     setup_extraction_environment,
#     save_outputs_and_metadata,
#     get_extraction_dates
# )
# from utils.headers import get_netsuite_headers
# from urls import Customer_URL

# # -------------------------------
# # Helper functions
# # -------------------------------




# async def fetch_resource_data(url, logger):
#     """Fetch data from NetSuite API using signed OAuth1 headers."""
#     try:
#         headers = get_netsuite_headers(url, method="GET")
#         async with aiohttp.ClientSession() as session:
#             async with session.get(url, headers=headers) as resp:
#                 resp.raise_for_status()
#                 try:
#                     data = await resp.json()
#                 except Exception:
#                     text = await resp.text()
#                     logger.error(f"Failed to parse JSON: {text}")
#                     data = {}
#                 return data if isinstance(data, dict) else {}
#     except aiohttp.ClientResponseError as e:
#         # Handle HTTP errors at this level
#         logger.error(f"HTTP error: {e.status}, message='{e.message}', url={url}")
#         raise
#     except aiohttp.ClientError as e:
#         logger.error(f"HTTP client error: {e}, url={url}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error: {e}, url={url}")
#         raise


# # -------------------------------
# # Expand linked fields safely
# # -------------------------------

# async def fetch_field(field, logger, cache):
#     """
#     Fetch linked record if it exists. Skip 403/404 errors.
#     """
#     if isinstance(field, dict) and "links" in field:
#         self_link = next((l["href"] for l in field["links"] if l.get("rel") == "self"), None)
#         record_id = str(field.get("id", ""))

#         # Skip system/default negative IDs
#         if self_link and not record_id.startswith("-"):
#             if self_link in cache:
#                 return cache[self_link]

#             try:
#                 data = await fetch_resource_data(self_link, logger)
#                 print("Self link", self_link)
#                 cache[self_link] = data
#                 print(data)
#                 return data
#             except aiohttp.ClientResponseError as e:
#                 if e.status in (403, 404):
#                     logger.warning(f"Skipping linked record {self_link}: {e.status} {e.message}")
#                     return None  # or return field to keep original id/href
#                 else:
#                     raise
#     return field


# async def expand_linked_fields(record, logger, cache):
#     print("Records", record)
#     """Replace linked references in a record with actual data from NetSuite API."""
#     tasks = {k: fetch_field(v, logger, cache) for k, v in record.items()}
#     results = await asyncio.gather(*tasks.values())
#     return dict(zip(tasks.keys(), results))


# #main function

# async def fetch_resource(url, resource_name, id_file_path):
#     """Fetch all customer details for each customer ID and expand linked fields."""
#     logger = logging.getLogger(resource_name)
#     now = datetime.now(timezone.utc)
#     outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

#     # Read customer IDs
#     if not os.path.exists(id_file_path):
#         logger.warning(f"ID file not found: {id_file_path}")
#         return resource_name, [], id_file_path

#     with open(id_file_path, "r") as f:
#         customer_ids = json.load(f)

#     all_items = []
#     cache = {}  # Cache for linked records

#     for cust_id in customer_ids:
#         next_url = url.replace("{id}", str(cust_id))

#         while next_url:
#             try:
#                 data = await fetch_resource_data(next_url, logger)
#             except Exception:
#                 # Skip this customer if top-level fetch fails
#                 logger.warning(f"Skipping customer {cust_id} due to fetch error.")
#                 break

#             if "items" in data:
#                 items = data["items"]
#             elif "id" in data:  # single customer record
#                 items = [data]
#             else:
#                 items = []

#             # Expand linked fields for each item
#             expanded_items = []
#             for item in items:
#                 expanded_item = await expand_linked_fields(item, logger, cache)
#                 expanded_items.append(expanded_item)

#             all_items.extend(expanded_items)
#             logger.info(f"Fetched {len(expanded_items)} items for customer {cust_id}")

#             # Pagination
#             next_links = [link["href"] for link in data.get("links", []) if link.get("rel") == "next"]
#             next_url = next_links[0] if next_links else None

#     logger.info(f"Total {len(all_items)} {resource_name} fetched across all customers")

#     # Save all customer data and metadata
#     await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, id_file_path)

#     return resource_name, all_items, id_file_path


# async def customers_list(id_file_path):
#     """Entry point for fetching all customer details."""
#     return await fetch_resource(Customer_URL, "customers_list", id_file_path)


import asyncio
import aiohttp
import logging
import json
import os
from datetime import datetime, timezone
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
    get_extraction_dates
)
from utils.headers import get_netsuite_headers
from urls import CUSTOMER_PAYMENT_LIST_URL
from dotenv import load_dotenv
import os
from oauthlib.oauth1 import Client

# Load environment variables
load_dotenv()

# -------------------------------------------------------
# Fetch from NetSuite API
# -------------------------------------------------------
async def fetch_resource_data(url, logger):
    """Fetch data from NetSuite API using signed OAuth1 headers."""
    try:
        headers = get_netsuite_headers(url, method="GET")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()

                # Try JSON parse
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    logger.error(f"Failed to parse JSON: {text}")
                    return {}

                return data if isinstance(data, dict) else {}

    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP error {e.status}: {e.message} | URL={url}")
        raise

    except aiohttp.ClientError as e:
        logger.error(f"HTTP client error: {e} | URL={url}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e} | URL={url}")
        raise


# -------------------------------------------------------
# Main dynamic resource fetcher
# -------------------------------------------------------
async def fetch_resource(url, resource_name, customer_ids):
    """Fetch customer records from NetSuite (no linked-field expansion)."""
    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    # Use provided customer IDs list
    if not customer_ids:
        logger.warning("Customer ID list is empty")
        return resource_name, [], customer_ids

    all_items = []

    # ---------------------------------------------------
    # Loop through each ID and fetch info
    # ---------------------------------------------------
    for cust_id in customer_ids:
        next_url = url.replace("{id}", str(cust_id))

        while next_url:
            try:
                data = await fetch_resource_data(next_url, logger)
            except Exception:
                logger.warning(f"Skipping customer {cust_id} due to fetch error.")
                break

            # Items can be a list or a single object
            if "items" in data:
                items = data["items"]
            elif "id" in data:
                items = [data]
            else:
                items = []

            all_items.extend(items)
            logger.info(f"Fetched {len(items)} items for customer {cust_id}")

            # Pagination handling
            next_links = [
                link["href"] 
                for link in data.get("links", []) 
                if link.get("rel") == "next"
            ]
            next_url = next_links[0] if next_links else None

    logger.info(f"TOTAL {len(all_items)} {resource_name} records fetched")

    # ---------------------------------------------------
    # Save results + logs + metadata
    # ---------------------------------------------------
    await save_outputs_and_metadata(
        resource_name,
        all_items,
        log_dir,
        outer_logs_dir,
        now,
        None
    )

    return resource_name, all_items, customer_ids


# -------------------------------------------------------
# Entry for Customer list
# -------------------------------------------------------
async def list_customer_payment(customer_payment_ids):
    """Entry point to fetch all customers using provided ID list."""
    return await fetch_resource(CUSTOMER_PAYMENT_LIST_URL, "list_customer_payment", customer_payment_ids)
