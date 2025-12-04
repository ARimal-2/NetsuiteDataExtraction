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
from urls import INVENTORY__TRANSFER_DETAILS_URL
from dotenv import load_dotenv
import os
from oauthlib.oauth1 import Client

from utils.rate_limiter import global_throttle

load_dotenv()

MAX_CONCURRENCY = 2        # max parallel requests per task
MAX_RETRIES = 3                # max retries per request
BASE_BACKOFF = 2               # initial backoff in seconds

# Per-task concurrency
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# -------------------------------------------------------
# Fetch from NetSuite API
# -------------------------------------------------------
async def fetch_resource_data(url, logger, session):
    """
    Fetch NetSuite data with retries, exponential backoff, and concurrency control.
    """
    async with semaphore:
        async with global_throttle:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    headers = get_netsuite_headers(url, method="GET")
                    async with session.get(url, headers=headers) as resp:
                        resp.raise_for_status()
                        try:
                            data = await resp.json()
                        except Exception:
                            
                            logger.error(f"Failed to parse JSON")

                        return data if isinstance(data, dict) else {}

                except (aiohttp.ClientError, aiohttp.ClientResponseError) as e:
                    wait_time = BASE_BACKOFF ** attempt
                    logger.warning(
                        f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)

                except Exception as e:
                    logger.error(f"Unexpected error for {url}: {e}")
                    raise

            # After all retries failed
            logger.error(f"All {MAX_RETRIES} retries failed for {url}")
            return {}

# -----------------------------
# Fetch all resources concurrently
# -----------------------------
async def fetch_resource(url_template, resource_name, transfer_ids):
    """
    Fetch multiple inventory transfer itemss concurrently with pagination.
    """
    logger = logging.getLogger(resource_name)

    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    if not transfer_ids:
        logger.warning("inventory transfer item ID list is empty")
        return resource_name, [], transfer_ids

    all_items = []

    async with aiohttp.ClientSession() as session:

        async def fetch_inventoryTransfer(t_id):
            all_inventoryTransfer = []
            next_url = url_template.replace("{id}", str(t_id))

            while next_url:
                data = await fetch_resource_data(next_url, logger, session)

                
                # Handle response
                if "items" in data:
                    items = data["items"]
                elif "id" in data:
                    items = [data]
                else:
                    items = []
                logger.info(f"Fetched {len(items)} items for inventory transfer item ID {t_id} from {next_url}")
                all_inventoryTransfer.extend(items)

                # Pagination
                next_links = [link["href"] for link in data.get("links", []) if link.get("rel") == "next"]
                next_url = next_links[0] if next_links else None

            return all_inventoryTransfer

        # Launch all inventory transfer items fetches concurrently
        tasks = [fetch_inventoryTransfer(t_id) for t_id in transfer_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Error in inventory transfer items task: {r}")
            else:
                all_items.extend(r)

    logger.info(f"TOTAL {len(all_items)} {resource_name} records fetched")

    # Save results + metadata
    await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, None)

    return resource_name, all_items, transfer_ids

# -------------------------------------------------------
# Entry for inventory id list
# -------------------------------------------------------
async def list_inventory_transfer_details(transfer_id):
    """Entry point to fetch all inventory ids using provided ID list."""
    return await fetch_resource( INVENTORY__TRANSFER_DETAILS_URL, "list_inventory_transfer_details", transfer_id)
