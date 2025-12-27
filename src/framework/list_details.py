
import asyncio
from itertools import islice
from typing import List, Tuple
import logging
import aiohttp
from datetime import datetime, timezone

from utils.headers import get_netsuite_headers
from utils.rate_limiter import global_throttle
from framework.utils_shared import validate_json
from src.extractors.utils import (
    setup_extraction_environment,
    save_outputs_and_metadata,
)
from src.framework.safe_upload import safe_upload


MAX_CONCURRENCY = 3            
ID_MICRO_CHUNK_SIZE = 50      
UPLOAD_CHUNK_SIZE = 2000          
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=10, sock_read=300)

SINGLE_FILE_PER_BATCH = True


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

async def fetch_with_retries(url, logger, session, semaphore):
    async with semaphore, global_throttle:
        for attempt in range(6):
            try:
                headers = get_netsuite_headers(url, method="GET")
                async with session.get(url, headers=headers, timeout=HTTP_TIMEOUT) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    await asyncio.sleep(0.15)
                    return validate_json(data, logger)
            except Exception as exc:
                if attempt == 5:
                    logger.error("Permanent failure | url=%s | error=%s", url, exc)
                    return {}
                wait = min(1 * (attempt + 1), 10)
                logger.warning(
                    "Retry %s | url=%s | wait=%ss | error=%s",
                    attempt + 1, url, wait, exc,
                )
                await asyncio.sleep(wait)


async def fetch_all_details(
    url_template: str,
    resource_name: str,
    ids: List[int],
) -> Tuple[str, int, List[int]]:

    logger = logging.getLogger(resource_name)
    now = datetime.now(timezone.utc)

    outer_logs_dir, log_dir, _, logger = setup_extraction_environment(resource_name, now)

    logger.info("%s | Starting detail fetch | total_ids=%s", resource_name, len(ids))

    if not ids:
        logger.warning("%s | ID list is empty", resource_name)
        return resource_name, 0, ids

   

    # If single-file-per-batch is enabled, ensure we flush once per batch
    effective_flush_size = len(ids) if SINGLE_FILE_PER_BATCH else UPLOAD_CHUNK_SIZE

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    buffer: List[dict] = []
    total_records_uploaded = 0
    upload_part = 1
    flush_lock = asyncio.Lock() 

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        async def fetch_single(item_id: int):
            nonlocal buffer, total_records_uploaded, upload_part

            next_url = url_template.replace("{id}", str(item_id))
            id_total_items = 0
            page = 1

            while next_url:
                data = await fetch_with_retries(next_url, logger, session, semaphore)
                if not data:
                    break

                records = data.get("items", [data])
                fetched_count = len(records)
                id_total_items += fetched_count

                # Append and flush if needed
                for record in records:
                    buffer.append(record)

                    # Concurrency-safe flush
                    if len(buffer) >= effective_flush_size:
                        async with flush_lock:
                            # re-check inside lock to avoid double flush
                            if len(buffer) >= effective_flush_size:
                                logger.info(
                                    "%s | Uploading chunk %s | records=%s",
                                    resource_name, upload_part, len(buffer),
                                )

                                # Use the per-batch unique name for uploads
                                await safe_upload(buffer, resource_name)

                                # Keep metadata under the logical resource_name
                                await save_outputs_and_metadata(
                                    resource_name,
                                    buffer,
                                    log_dir,
                                    outer_logs_dir,
                                    now,
                                    None,
                                )

                                total_records_uploaded += len(buffer)
                                buffer.clear()
                                upload_part += 1

                # pagination
                next_links = [
                    l["href"] for l in data.get("links", []) if l.get("rel") == "next"
                ]
                next_url = next_links[0] if next_links else None
                page += 1

                # yield control to event loop (keeps heartbeat responsive)
                await asyncio.sleep(0.05)

            logger.info(
                "%s | ID=%s | Fetch completed | total_items=%s",
                resource_name, item_id, id_total_items,
            )

        for batch_no, id_batch in enumerate(chunked(ids, ID_MICRO_CHUNK_SIZE), start=1):
            logger.info(
                "%s | Processing ID micro-batch %s | batch_size=%s",
                resource_name, batch_no, len(id_batch),
            )
            await asyncio.gather(*(fetch_single(i) for i in id_batch))
            await asyncio.sleep(0.1)  


    if buffer:
        async with flush_lock:
            logger.info("%s | Uploading final chunk | records=%s", resource_name, len(buffer))
            await safe_upload(buffer, resource_name)
            await save_outputs_and_metadata(
                resource_name,
                buffer,
                log_dir,
                outer_logs_dir,
                now,
                None,
            )
            total_records_uploaded += len(buffer)
            buffer.clear()

    logger.info(
        "%s | Completed | total_records_uploaded=%s | total_ids=%s",
        resource_name, total_records_uploaded, len(ids),
    )

    return resource_name, total_records_uploaded, ids
