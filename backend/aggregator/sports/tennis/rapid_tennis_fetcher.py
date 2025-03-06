import logging
import aiohttp
import asyncio
from typing import Any, Dict, List
from asyncio import Semaphore

# Configure root logger once here
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # DEBUG for detailed logs; change to INFO in production

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# --- HARDCODED API CREDENTIALS & URLS HERE ---
API_KEY = "750ad01770msh9716fc05e7ecc56p15565fjsn93e405806783"
API_HOST = "bet365-api-inplay.p.rapidapi.com"
BASE_URL = "https://bet365-api-inplay.p.rapidapi.com"

# Limit how many concurrent odds fetches happen at once
MAX_CONCURRENT_REQUESTS = 5

class RapidInplayOddsFetcher:
    """
    Fetches in-play tennis odds from RapidAPI bet365 endpoints (raw data).
    """

    def __init__(self):
        # Hardcoded config
        self.api_key = API_KEY
        self.api_host = API_HOST
        self.base_url = BASE_URL
        self.headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.api_host
        }
        self.semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)  # Concurrency limit

    async def fetch_data(self, url: str, session: aiohttp.ClientSession) -> Any:
        """Make async HTTP request to RapidAPI; return raw JSON or an empty dict on error."""
        logger.debug(f"Fetching data from URL: {url}")
        try:
            # Use the semaphore to limit concurrent requests
            async with self.semaphore:
                async with session.get(url, headers=self.headers, timeout=10) as response:
                    if response.status == 200:
                        logger.debug(f"Data fetched successfully from: {url}")
                        return await response.json()
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"RapidAPI error: Status code {response.status} - {error_text}"
                        )
                        return {}
        except asyncio.TimeoutError:
            logger.error(f"Request timed out for URL: {url}")
            return {}
        except aiohttp.ClientError as e:
            logger.error(f"ClientError for URL {url}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in fetch_data for URL {url}: {str(e)}")
            return {}

    async def fetch_odds_for_events(
        self,
        events: List[Dict[str, Any]],
        session: aiohttp.ClientSession
    ) -> List[Dict[str, Any]]:
        """
        Fetch odds for a list of in-play tennis events concurrently, respecting the semaphore limit.
        Returns a list of dicts: [{'raw_event_data': ..., 'raw_odds_data': ...}, ...].
        """
        tasks = []
        valid_events = []

        # Minimal validation: ensure we have a list of event dicts
        if not isinstance(events, list):
            logger.warning(f"Expected a list of events, got {type(events)}. Data: {events}")
            return []

        for event in events:
            # Double-check event is a dict
            if not isinstance(event, dict):
                logger.warning(f"Skipping non-dict event: {event}")
                continue

            event_id = event.get("marketFI")
            if not event_id:
                logger.warning(f"Skipping event with missing 'marketFI': {event}")
                continue

            # Prepare concurrency tasks
            odds_url = f"{self.base_url}/bet365/get_event_with_markets/{event_id}"
            tasks.append(self.fetch_data(odds_url, session))
            valid_events.append(event)

        # Fetch all odds concurrently
        odds_data_list = await asyncio.gather(*tasks, return_exceptions=True)

        matches = []
        for event, odds_data in zip(valid_events, odds_data_list):
            if isinstance(odds_data, Exception):
                logger.error(f"Error fetching odds for event ID {event.get('marketFI')}: {odds_data}")
                continue

            # Extract just the markets and stats from the odds data
            if isinstance(odds_data, dict):
                markets = odds_data.get('markets', [])
                stats = odds_data.get('stats', {})
                odds_data = {'markets': markets, 'stats': stats}
            else:
                logger.warning(f"Odds data is not a dict: {odds_data}")
                continue

            match_data = {
                'raw_event_data': event,
                'raw_odds_data': odds_data
            }
            matches.append(match_data)

        return matches

    async def get_tennis_data(self) -> List[Dict[str, Any]]:
        """
        Fetch tennis in-play events and their odds, returning raw data.
        Each item in the returned list will include:
          - 'raw_event_data': the raw JSON for that event
          - 'raw_odds_data': the raw JSON for the event's odds
        """
        try:
            async with aiohttp.ClientSession() as session:
                # 1) Fetch the list of in-play tennis events
                events_url = f"{self.base_url}/bet365/get_sport_events/tennis"
                logger.debug("Fetching in-play tennis events...")
                events_data = await self.fetch_data(events_url, session)

                if not events_data:
                    logger.warning("No tennis events found from RapidAPI (events_data empty).")
                    return []

                # 2) Fetch odds for all events concurrently
                matches = await self.fetch_odds_for_events(events_data, session)
                logger.info(f"Fetched raw data for {len(matches)} in-play tennis matches.")
                return matches

        except Exception as e:
            logger.error(f"Error in get_tennis_data: {e}")
            return []

__all__ = ['RapidInplayOddsFetcher']

# Example usage / quick test
if __name__ == "__main__":
    # No separate logging config call here, we already configured the logger above.

    async def test_fetcher():
        fetcher = RapidInplayOddsFetcher()
        matches = await fetcher.get_tennis_data()
        logger.info(f"\nFetched {len(matches)} matches in total.")
        if matches:
            logger.info("\nSample match data:\n")
            logger.info(f"Raw Event Data: {matches[0]['raw_event_data']}")
            logger.info(f"Raw Odds Data: {matches[0]['raw_odds_data']}")

    asyncio.run(test_fetcher())