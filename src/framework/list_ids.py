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


RETRYABLE_STATUS_CODES = {401, 429, 500, 502, 503, 504}

async def fetch_resource_data(session, url, logger, max_retries=5, base_delay=.4):
    """
    Fetch data from NetSuite with retry logic.
    OAuth headers are regenerated per attempt.
    """
    attempt = 0

    while True:
        try:
            headers = get_netsuite_headers(url, method="GET")

            async with session.get(url, headers=headers) as resp:
                if resp.status in RETRYABLE_STATUS_CODES:
                    text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=text,
                        headers=resp.headers,
                    )

                resp.raise_for_status()

                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    logger.error(f"Failed to parse JSON: {text}")
                    raise

                return validate_json(data, logger)

        except aiohttp.ClientResponseError as e:
            attempt += 1

            if attempt > max_retries:
                logger.error(
                    f"Max retries exceeded for {url} (status={e.status})"
                )
                raise

            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                f"Retry {attempt}/{max_retries} for {url} "
                f"(status={e.status}), sleeping {delay:.1f}s"
            )
            await asyncio.sleep(delay)

        except aiohttp.ClientError as e:
            attempt += 1

            if attempt > max_retries:
                logger.error(f"Network retries exhausted for {url}: {e}")
                raise

            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                f"Network error retry {attempt}/{max_retries} "
                f"sleeping {delay:.1f}s: {e}"
            )
            await asyncio.sleep(delay)


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
    outer_logs_dir, log_dir, id_file_path, logger = (
        setup_extraction_environment(resource_name, now)
    )

    last_extracted, _ = get_extraction_dates(
        outer_logs_dir, resource_name, now
    )

    next_url = build_incremental_url(url, last_extracted, logger)

    all_items = []

    timeout = aiohttp.ClientTimeout(total=None)

   
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while next_url:
            data = await fetch_resource_data(
                session=session,
                url=next_url,
                logger=logger,
                max_retries=5,
                base_delay=1.0,
            )

            items = data.get("items", [])
            all_items.extend(items)

            logger.info(
                f"Fetched {len(items)} items from {next_url}"
            )

            # Pagination
            next_links = [
                link["href"]
                for link in data.get("links", [])
                if link.get("rel") == "next"
            ]
            next_url = next_links[0] if next_links else None

         
            await asyncio.sleep(0.4)

    logger.info(
        f"Total {len(all_items)} {resource_name} records fetched"
    )

 

    await save_outputs_and_metadata(
        resource_name,
        all_items,
        log_dir,
        outer_logs_dir,
        now,
        id_file_path,
    )

    id_list = extract_ids(all_items, logger)

    return resource_name, id_list, all_items


