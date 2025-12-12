import asyncio
import aiohttp
import logging
import urllib.parse
from datetime import datetime, timezone

from framework.utils_shared import extract_ids, to_netsuite_display, validate_json
from utils.headers import get_netsuite_headers
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
    get_extraction_dates,
)


async def fetch_resource_data(url, logger):
    """Fetch data from NetSuite with error handling."""
    try:
        headers = get_netsuite_headers(url, method="GET")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()

                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    logger.error(f"Failed to parse JSON: {text}")
                    return {}

                return validate_json(data, logger)

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error: {e}")
        return {}

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {}


def build_incremental_url(base_url, last_timestamp, logger):
    """Create NetSuite incremental query."""
    if not last_timestamp:
        logger.info("First load — full dataset")
        return base_url

    ns_date = to_netsuite_display(last_timestamp, logger)

    if not ns_date:
        return base_url

    query = f'lastModifiedDate ON_OR_AFTER "{ns_date}"'
    encoded = urllib.parse.quote(query)

    final_url = f"{base_url}?q={encoded}"
    logger.info(f"Incremental query: {final_url}")

    return final_url


async def fetch_all_ids(url, resource_name):
    """
    Shared async extractor for ALL ID endpoints.
    """
    logger = logging.getLogger(resource_name)

    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, id_file_path, logger = setup_extraction_environment(resource_name, now)

    last_extracted, _ = get_extraction_dates(outer_logs_dir, resource_name, now)

    # Build incremental URL
    next_url = build_incremental_url(url, last_extracted, logger)

    all_items = []

    while next_url:
        data = await fetch_resource_data(next_url, logger)
        items = data.get("items", [])
        all_items.extend(items)

        logger.info(f"Fetched {len(items)} items from {next_url}")

        # Pagination
        next_links = [link["href"] for link in data.get("links", []) if link.get("rel") == "next"]
        next_url = next_links[0] if next_links else None

    logger.info(f"Total {len(all_items)} {resource_name} records fetched")

 

    # Save metadata
    await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, id_file_path)

    # Extract IDs
    id_list = extract_ids(all_items, logger)

    return resource_name, id_list, all_items
