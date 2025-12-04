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
from urls import INVOICE_DETAILS_URL

# --------------------------
# ---
# Settings
# -----------------------------
MAX_CONCURRENCY = 10           # max parallel requests
MAX_RETRIES = 3                # max retries per request
BASE_BACKOFF = 2               # initial backoff in seconds

semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


# -----------------------------
# Fetch data with retries/backoff
# -----------------------------
async def fetch_resource_data(url, logger, session):
    """
    Fetch NetSuite data with retries, exponential backoff, and concurrency control.
    """
    async with semaphore:
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
async def fetch_resource(url_template, resource_name, invoice_ids):
    """
    Fetch multiple invoices concurrently with pagination.
    """
    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    if not invoice_ids:
        logger.warning("invoice ID list is empty")
        return resource_name, [], invoice_ids

    all_items = []

    # Reuse a single session for efficiency
    async with aiohttp.ClientSession() as session:

        async def fetch_invoice(dep_id):
            items_for_invoice = []
            next_url = url_template.replace("{id}", str(dep_id))

            while next_url:
                data = await fetch_resource_data(next_url, logger, session)
                
                # Handle response
                if "items" in data:
                    items = data["items"]
                elif "id" in data:
                    items = [data]
                else:
                    items = []

                items_for_invoice.extend(items)

                # Pagination
                next_links = [link["href"] for link in data.get("links", []) if link.get("rel") == "next"]
                next_url = next_links[0] if next_links else None

            return items_for_invoice

        # Launch all invoice fetches concurrently
        tasks = [fetch_invoice(dep_id) for dep_id in invoice_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Error in invoice task: {r}")
            else:
                all_items.extend(r)

    logger.info(f"TOTAL {len(all_items)} {resource_name} records fetched")

    # Save results + metadata
    await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, None)

    return resource_name, all_items, invoice_ids


# -----------------------------
# Entry point for invoice list
# -----------------------------
async def list_invoice_details(invoice_ids):
    """Fetch all invoice given the list of IDs"""
    return await fetch_resource(INVOICE_DETAILS_URL, "list_invoice_details", invoice_ids)
