import asyncio
import aiohttp
import logging
import random
from datetime import datetime, timezone
from itertools import islice
from typing import List, Tuple

from utils.headers import get_netsuite_headers
from utils.rate_limiter import global_throttle
from framework.utils_shared import validate_json
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
)

# ===========================
# Config
# ===========================
MAX_CONCURRENCY = 5
MAX_RETRIES = 6
BASE_BACKOFF = 1
THROTTLE_SLEEP = 0.15
ID_CHUNK_SIZE = 100

HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=300,
    connect=10,
    sock_read=300,
)


# ===========================
# Helpers
# ===========================
def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk



async def fetch_with_retries(url, logger, session, semaphore):
    """
    Fetch a URL with retries, capped linear backoff, and heartbeat-safe async sleeps.
    """
    async with semaphore:
        async with global_throttle:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    headers = get_netsuite_headers(url, method="GET")
                    async with session.get(url, headers=headers, timeout=HTTP_TIMEOUT) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                        # Small throttle to respect API limits
                        await asyncio.sleep(THROTTLE_SLEEP)
                        return validate_json(data, logger)

                except Exception as exc:
                    if attempt == MAX_RETRIES:
                        logger.error(
                            f"Permanent failure after {MAX_RETRIES} retries "
                            f"for URL={url} :: {exc}"
                        )
                        return {}

                    # Heartbeat-safe linear backoff with jitter
                    wait = min(BASE_BACKOFF * attempt + random.uniform(0, 1), 10)  # cap at 10s
                    logger.warning(
                        f"Retry {attempt}/{MAX_RETRIES} failed for {url}: {exc}. "
                        f"Sleeping {wait:.1f}s"
                    )

                    # Split sleep into 1-second chunks for heartbeat safety
                    remaining = wait
                    while remaining > 0:
                        sleep_time = min(1, remaining)
                        await asyncio.sleep(sleep_time)
                        remaining -= sleep_time


async def fetch_all_details(
    url_template: str,
    resource_name: str,
    ids: List[int],
) -> Tuple[str, list, List[int]]:

    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)
    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(
        resource_name, now
    )

    if not ids:
        logger.warning(f"{resource_name} ID list is empty")
        return resource_name, [], ids

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    all_items = []

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:

        async def fetch_single(item_id):
            next_url = url_template.replace("{id}", str(item_id))
            results = []

            while next_url:
                data = await fetch_with_retries(next_url, logger, session, semaphore)

                if "items" in data:
                    results.extend(data["items"])
                elif "id" in data:
                    results.append(data)

                logger.info(
                    f"{resource_name} | ID={item_id} | records fetched={len(results)}"
                )

                next_links = [l["href"] for l in data.get("links", []) if l.get("rel") == "next"]
                next_url = next_links[0] if next_links else None

                await asyncio.sleep(0)  

            return results

        for chunk_no, id_chunk in enumerate(chunked(ids, ID_CHUNK_SIZE), start=1):
            logger.info(f"{resource_name} | Processing chunk {chunk_no} ({len(id_chunk)} IDs)")
            tasks = [fetch_single(i) for i in id_chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, Exception):
                    logger.error(f"Chunk fetch failed: {r}")
                else:
                    all_items.extend(r)

            await asyncio.sleep(0)

    logger.info(f"{resource_name} | Total records fetched={len(all_items)}")

    await save_outputs_and_metadata(resource_name, all_items, log_dir, outer_logs_dir, now, None)

    return resource_name, all_items, ids
