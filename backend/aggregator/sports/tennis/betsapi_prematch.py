"""
Fetches tennis prematch data from BetsAPI.
"""

import logging
import aiohttp
import asyncio
import time
from typing import Any, Dict, List, Optional
from asyncio import Semaphore

# -----------------------------------------------------------------------------
# Logging Configuration (Console + File)
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # DEBUG logs everything, switch to INFO in prod

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# File Handler (writes at INFO level or higher)
file_handler = logging.FileHandler("betsapi_prematch.log", mode='a')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add both handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# -----------------------------------------------------------------------------
# Hardcoded BetsAPI Credentials / Settings
# -----------------------------------------------------------------------------
API_TOKEN = "215152-OYDi6ziW1Szobx"    # BetsAPI token
BASE_URL = "https://api.b365api.com/v3"  # BetsAPI v3 base URL
SPORT_ID = "13"  # 13 = Tennis

# -----------------------------------------------------------------------------
# Class Definition
# -----------------------------------------------------------------------------
class BetsapiPrematch:
    """
    Fetches tennis event IDs from BetsAPI (in-play), then fetches Bet365 prematch data
    for each of those events. Returns raw combined data.

    Features:
      - File + console logging
      - Simple retry logic for timeouts / rate limits
      - Configurable concurrency limit
    """

    def __init__(self, concurrency_limit: int = 5, max_retries: int = 3):
        """
        :param concurrency_limit: Maximum number of concurrent requests.
        :param max_retries: Number of retries for each request (on timeout or rate-limit errors).
        """
        self.token = API_TOKEN
        self.base_url = BASE_URL
        self.semaphore = Semaphore(concurrency_limit)
        self.max_retries = max_retries

    async def fetch_data(
        self,
        url: str,
        session: aiohttp.ClientSession,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Performs an async GET request to a BetsAPI endpoint with simple retry logic.
        Returns the parsed JSON on success or an empty dict on error.
        """
        if params is None:
            params = {}
        params["token"] = self.token  # Always include the token in query params

        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            try:
                async with self.semaphore:
                    async with session.get(url, params=params, timeout=10) as response:
                        status = response.status
                        if status == 200:
                            data = await response.json()
                            logger.debug(f"[{attempt}/{self.max_retries}] Success: {url}")
                            return data
                        elif status == 429:
                            # Simple rate-limit handling: wait a couple seconds, then retry
                            error_text = await response.text()
                            logger.warning(f"[{attempt}/{self.max_retries}] 429 Rate Limit: {error_text}")
                            if attempt < self.max_retries:
                                await asyncio.sleep(2)  # Wait before next retry
                            else:
                                logger.error("Max retries reached after 429 rate limit.")
                            continue
                        else:
                            # Other errors (4xx, 5xx, etc.)
                            error_text = await response.text()
                            logger.error(f"[{attempt}/{self.max_retries}] Error {status}: {error_text}")
                            # No auto retry on these unless you want to handle 500, etc.
                            return {}
            except asyncio.TimeoutError:
                logger.error(f"[{attempt}/{self.max_retries}] Request timed out for URL: {url}")
                if attempt < self.max_retries:
                    await asyncio.sleep(2)  # Wait before retry
                else:
                    logger.error("Max retries reached on timeout.")
            except aiohttp.ClientError as e:
                logger.error(f"[{attempt}/{self.max_retries}] ClientError for URL {url}: {e}")
                # Depending on the error type, you could decide to retry
                # Here we'll just do a short wait and try again
                if attempt < self.max_retries:
                    await asyncio.sleep(2)
                else:
                    logger.error("Max retries reached on client error.")
            except Exception as e:
                # Unexpected exception
                logger.error(f"[{attempt}/{self.max_retries}] Unexpected error: {e}")
                # Decide if you want to retry or break immediately
                if attempt < self.max_retries:
                    await asyncio.sleep(2)
                else:
                    logger.error("Max retries reached on unexpected error.")
            # If we get here, loop continues to next attempt
        return {}

    async def fetch_inplay_tennis_events(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """
        Fetches the current in-play tennis events from BetsAPI (v3).
        Endpoint: GET /v3/events/inplay?sport_id=13
        """
        url = f"{self.base_url}/events/inplay"
        params = {"sport_id": SPORT_ID}

        data = await self.fetch_data(url, session, params)
        if not data or "results" not in data:
            logger.warning("No 'results' found in the in-play data.")
            return []

        events = data["results"]
        logger.info(f"Found {len(events)} in-play tennis events.")
        return events

    async def fetch_prematch_data(self, bet365_id: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """
        Fetch Bet365 prematch data for a single match ID.
        Endpoint: GET /v3/bet365/prematch?FI=<bet365_id>
        """
        url = f"{self.base_url}/bet365/prematch"
        params = {"FI": bet365_id}

        prematch_json = await self.fetch_data(url, session, params)
        if not prematch_json or "results" not in prematch_json:
            logger.warning(f"No 'results' key for Bet365 ID={bet365_id} or empty data.")
            return {}

        return prematch_json["results"]

    async def fetch_prematch_data_for_events(
        self, 
        events: List[Dict[str, Any]], 
        session: aiohttp.ClientSession
    ) -> List[Dict[str, Any]]:
        """
        For each in-play event, look up its Bet365 ID and fetch the prematch data concurrently.
        Returns a list of dicts in the form:
          [
            {
              "inplay_event": <raw event data>,
              "bet365_id": <the Bet365 ID>,
              "raw_prematch_data": <full prematch JSON from BetsAPI>
            },
            ...
          ]
        """
        tasks = []
        valid_events = []

        for event in events:
            bet365_id = str(event.get("bet365_id") or "")
            if not bet365_id:
                logger.debug(f"Skipping event with no 'bet365_id': {event.get('id')}")
                continue

            tasks.append(self.fetch_prematch_data(bet365_id, session))
            valid_events.append((event, bet365_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        combined = []
        for (inplay_event, bet365_id), prematch_data in zip(valid_events, results):
            if isinstance(prematch_data, Exception):
                logger.error(f"Error fetching prematch for bet365_id={bet365_id}: {prematch_data}")
                continue

            combined.append({
                "inplay_event": inplay_event,
                "bet365_id": bet365_id,
                "raw_prematch_data": prematch_data
            })

        return combined

    async def get_tennis_data(self) -> List[Dict[str, Any]]:
        """
        Main entry point:
          1) Fetch in-play tennis events 
          2) Extract each event's bet365_id
          3) Fetch prematch data for each bet365_id
          4) Return combined records

        Returns:
            A list of dicts like:
            [
              {
                "inplay_event": { ... },
                "bet365_id": "<string>",
                "raw_prematch_data": { ... }
              },
              ...
            ]
        """
        try:
            async with aiohttp.ClientSession() as session:
                inplay_events = await self.fetch_inplay_tennis_events(session)
                if not inplay_events:
                    logger.warning("No in-play tennis events found, returning empty list.")
                    return []

                # Fetch prematch data for all in-play events with a valid bet365_id
                combined_data = await self.fetch_prematch_data_for_events(inplay_events, session)
                logger.info(f"Successfully fetched prematch data for {len(combined_data)} events.")
                return combined_data

        except Exception as e:
            logger.error(f"Critical error in get_tennis_data: {e}")
            return []

# Make the class available for import
__all__ = ['BetsapiPrematch']

# ------------------------------------------------------------------------------
# Quick Test / Example Usage
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    async def test_fetcher():
        # Suppose we want concurrency=5, up to 3 retries per request:
        fetcher = BetsapiPrematch(concurrency_limit=5, max_retries=3)
        data = await fetcher.get_tennis_data()

        logger.info(f"\nTotal combined events fetched: {len(data)}")
        if data:
            first_match = data[0]
            logger.info("\n--- Example of First Combined Event ---")
            logger.info(f"In-Play Event: {first_match['inplay_event']}")
            logger.info(f"Bet365 ID: {first_match['bet365_id']}")
            logger.info(f"Raw Prematch Data: {first_match['raw_prematch_data']}")

    asyncio.run(test_fetcher())