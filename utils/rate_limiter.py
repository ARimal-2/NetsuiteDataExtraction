import asyncio

RATE_LIMIT = 20
global_throttle = asyncio.Semaphore(RATE_LIMIT)