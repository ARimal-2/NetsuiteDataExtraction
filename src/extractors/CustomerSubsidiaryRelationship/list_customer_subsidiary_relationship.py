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
from urls import CUSTOMER_SUBSIDIARY_RELATIONSHIP_LIST_URL
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
async def fetch_resource(url, resource_name, refund_ids):
    """Fetch customer records from NetSuite (no linked-field expansion)."""
    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    # Use provided customer IDs list
    if not refund_ids:
        logger.warning("Customer ID list is empty")
        return resource_name, [], refund_ids

    all_items = []

    # ---------------------------------------------------
    # Loop through each ID and fetch info
    # ---------------------------------------------------
    for refundId in refund_ids:
        next_url = url.replace("{id}", str(refundId))

        while next_url:
            try:
                data = await fetch_resource_data(next_url, logger)
            except Exception:
                logger.warning(f"Skipping customer {refundId} due to fetch error.")
                break

            # Items can be a list or a single object
            if "items" in data:
                items = data["items"]
            elif "id" in data:
                items = [data]
            else:
                items = []

            all_items.extend(items)
            logger.info(f"Fetched {len(items)} items for customer {refundId}")

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

    return resource_name, all_items, refund_ids


# -------------------------------------------------------
# Entry for Customer list
# -------------------------------------------------------
async def list_customer_subsidiary_relationship(customer_refund_ids):
    """Entry point to fetch all customers using provided ID list."""
    return await fetch_resource(CUSTOMER_SUBSIDIARY_RELATIONSHIP_LIST_URL, "list_customer_subsidiary_relationship", customer_refund_ids)
