import asyncio
import aiohttp
import logging
import json
from datetime import datetime, timezone
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
    get_extraction_dates
)
from utils.headers import get_netsuite_headers
from urls import INVENTORY_ITEM_ID_URL
import urllib.parse





def extract_ids(resource_data, logger):
    """
    Extract 'id' from each item in resource_data and return as list.
    """
    try:
        id_list = [item["id"] for item in resource_data if isinstance(item, dict) and "id" in item]
        logger.info(f"Extracted {len(id_list)} inventory item IDs")
        return id_list
    except Exception as e:
        logger.error(f"Error extracting IDs: {e}")
        return []


def build_incremental_url(url, last_extracted, logger):
    """Build URL with date filters for incremental loading."""
    
    if last_extracted:
        query = f'dateCreated ON_OR_AFTER "{last_extracted}"'
        encoded_query = urllib.parse.quote(query)
        print("Encoded Query:", encoded_query)
        filtered_url=f"{url}?q={encoded_query}"
        logger.info(f"Incremental load from date: {last_extracted} ")
        return filtered_url
    logger.info("First load — fetching full dataset")
    return url


async def fetch_resource_data(url, logger):
    """Fetch data from NetSuite API using signed OAuth1 headers."""
    try:
        headers = get_netsuite_headers(url, method="GET")
        print("Headers",headers)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    logger.error(f"Failed to parse JSON: {text}")
                    data = {}
                return data if isinstance(data, dict) else {}
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {}


async def fetch_resource(url, resource_name):
    """Main function to fetch all inventory items with pagination and save IDs."""
    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, id_file_path, logger = setup_extraction_environment(resource_name, now)

    # Incremental URL
    last_extracted, modified_date_end = get_extraction_dates(outer_logs_dir, resource_name, now)
    if last_extracted:
                # Convert ISO 8601 to datetime
                dt = datetime.fromisoformat(last_extracted.replace("Z", "+00:00"))
                
                last_extracted = dt.strftime("%m/%d/%y")
                print(f"Converted last_extracted to NetSuite format: {last_extracted}")
    next_url = build_incremental_url(url, last_extracted, logger)
    print(f"Final URL for fetching inventory items: {next_url}")
    all_items = []

    while next_url:
        data = await fetch_resource_data(next_url, logger)
        items = data.get("items", [])
        all_items.extend(items)
        logger.info(f"Fetched {len(items)} items from {next_url}")

        # Pagination
        next_links = [link["href"] for link in data.get("links", []) if link.get("rel") == "next"]
        next_url = next_links[0] if next_links else None

    logger.info(f"Total {len(all_items)} inventory items fetched")
    resource_data = all_items[:200]
    # Save fetched data and metadata
    await save_outputs_and_metadata(resource_name, resource_data, log_dir, outer_logs_dir, now, id_file_path)
    # Extract and return IDs directly
    id_list = extract_ids(resource_data, logger)

    return resource_name, id_list, resource_data


async def list_inventory_item_id():
    """Entry point to fetch all inventory item id."""
    return await fetch_resource (INVENTORY_ITEM_ID_URL, "list_inventory_item_id")
