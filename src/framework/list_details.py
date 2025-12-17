import asyncio
import aiohttp
import logging
import random
from datetime import datetime, timezone

from utils.headers import get_netsuite_headers
from utils.rate_limiter import global_throttle
from framework.utils_shared import validate_json
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
)

# -----------------------------
# Configurable constants
# -----------------------------
MAX_CONCURRENCY = 2
MAX_RETRIES = 6          # Increased from 3
BASE_BACKOFF = 2         # Exponential backoff base
THROTTLE_SLEEP = 0.15    # Steady-state delay per request


async def fetch_with_retries(url, logger, session, semaphore):
    """
    Fetch with retry, exponential backoff + jitter, limited concurrency.
    Returns {} on permanent failure (IDs may be lost).
    """
    async with semaphore:
        async with global_throttle:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    headers = get_netsuite_headers(url, method="GET")
                    async with session.get(url, headers=headers) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                        # small steady-state sleep to prevent throttling
                        await asyncio.sleep(THROTTLE_SLEEP)

                        return validate_json(data, logger)

                except Exception as e:
                    if attempt == MAX_RETRIES:
                        logger.error(f"All {MAX_RETRIES} retries failed for {url}: {e}")
                        return {}  # permanent failure, ID may be lost

                    # exponential backoff + jitter
                    wait = (BASE_BACKOFF ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}. "
                        f"Retrying in {wait:.1f}s..."
                    )
                    await asyncio.sleep(wait)


async def fetch_all_details(url_template, resource_name, ids):
    """
    Shared async extractor for all detail endpoints.
    Fetches multiple IDs concurrently, with pagination and retries.
    """
    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    if not ids:
        logger.warning(f"{resource_name} ID list is empty")
        return resource_name, [], ids

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    all_items = []

    async with aiohttp.ClientSession() as session:

        async def fetch_single(item_id):
            """
            Fetch a single ID, handling pagination and retries.
            """
            next_url = url_template.replace("{id}", str(item_id))
            results = []

            while next_url:
                data = await fetch_with_retries(next_url, logger, session, semaphore)

                if "items" in data:
                    results.extend(data["items"])
                elif "id" in data:
                    results.append(data)
                else:
                    results.extend([])

                logger.info(f"Total {len(results)} records fetched for URL: {next_url}")

                # Pagination
                next_links = [l["href"] for l in data.get("links", []) if l.get("rel") == "next"]
                next_url = next_links[0] if next_links else None

            return results

        # Prepare tasks for all IDs
        tasks = [fetch_single(i) for i in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Error in detail task: {r}")
            else:
                all_items.extend(r)

    logger.info(f"Total {len(all_items)} {resource_name} detail records fetched")

    # Save outputs 
    await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, None)

    return resource_name, all_items, ids
