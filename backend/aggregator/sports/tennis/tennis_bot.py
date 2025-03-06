import os
import asyncio
import logging
import signal
import time
import json
from typing import Optional, Dict
from datetime import datetime, timedelta
import pytz
import threading
from fastapi import FastAPI, WebSocket, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from starlette.websockets import WebSocketState, WebSocketDisconnect

# Using absolute imports
from aggregator.sports.tennis.betsapi_prematch import BetsapiPrematch
from aggregator.sports.tennis.rapid_tennis_fetcher import RapidInplayOddsFetcher
from aggregator.sports.tennis.tennis_merger import TennisMerger

###############################################################################
# Configuration via Environment Variables or defaults
###############################################################################
DEFAULT_CONCURRENCY = int(os.getenv("TENNIS_BOT_CONCURRENCY", "5"))
DEFAULT_MAX_RETRIES = int(os.getenv("TENNIS_BOT_MAX_RETRIES", "3"))
DEFAULT_FETCH_INTERVAL = float(os.getenv("TENNIS_BOT_FETCH_INTERVAL", "60"))
COUNTER_FILE = "tennis_bot_counters.json"

###############################################################################
# Global Variables
###############################################################################
# Store the latest merged data globally to share between TennisBot and FastAPI
latest_tennis_data = []

###############################################################################
# Logging Configuration
###############################################################################
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Set eastern timezone for logging
eastern_tz = pytz.timezone('US/Eastern')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                              datefmt='%Y-%m-%d %H:%M:%S')
formatter.converter = lambda *args: datetime.now(eastern_tz).timetuple()
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler("tennis_bot.log", mode='a')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Avoid adding handlers multiple times
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

###############################################################################
# Graceful Shutdown Setup
###############################################################################
def shutdown_handler(loop: asyncio.AbstractEventLoop) -> None:
    """
    Signal handler to gracefully shut down the bot.

    This function is triggered by signal handlers (SIGINT, SIGTERM),
    logs a shutdown message, and cancels all running tasks on the
    provided event loop. This allows for an orderly exit of the
    application.
    
    :param loop: The asyncio event loop whose tasks we need to cancel.
    """
    logger.info("Received shutdown signal. Cancelling tasks...")
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()

###############################################################################
# TennisBot Class (Orchestrator)
###############################################################################
class TennisBot:
    """
    Orchestrates the complete flow:
      1) Fetch tennis prematch data from BetsAPI (with concurrency/retry logic)
      2) Fetch in-play odds data from RapidAPI (no concurrency/retry params)
      3) Merge the two data sets using TennisMerger
      4) Display raw merged data on Vue frontend web browser

    Repeats every fetch_interval seconds.
    """

    def __init__(
        self,
        fetch_interval: Optional[float] = None,
        concurrency_limit: Optional[int] = None,
        max_retries: Optional[int] = None
    ) -> None:
        """
        Initialize the TennisBot with optional configuration overrides.

        :param fetch_interval: Interval in seconds between fetch cycles.
                             Defaults to TENNIS_BOT_FETCH_INTERVAL or 60.
        :param concurrency_limit: Max concurrent requests for BetsAPI.
                                Defaults to TENNIS_BOT_CONCURRENCY or 5.
        :param max_retries: Max retries for BetsAPI requests.
                          Defaults to TENNIS_BOT_MAX_RETRIES or 3.
        """
        self.fetch_interval = fetch_interval or DEFAULT_FETCH_INTERVAL

        # Initialize API fetchers
        self.betsapi_fetcher = BetsapiPrematch(
            concurrency_limit=concurrency_limit or DEFAULT_CONCURRENCY,
            max_retries=max_retries or DEFAULT_MAX_RETRIES
        )
        self.rapid_fetcher = RapidInplayOddsFetcher()

        # Load or initialize counters for API calls
        self.load_counters()
        est = pytz.timezone('America/New_York')
        now = datetime.now(est)
        self.last_reset_date = now.date()
        self.last_reset_month = now.month
        self.last_reset_hour = now.hour
        self.last_fetch_time = time.time()

    def load_counters(self) -> None:
        """Load counters from file or initialize new ones if file doesn't exist"""
        try:
            with open(COUNTER_FILE, 'r') as f:
                data = json.load(f)
                self.current_cycle_calls = data.get('current_cycle_calls', {'betsapi': 0, 'rapidapi': 0})
                self.hourly_total_calls = data.get('hourly_total_calls', {'betsapi': 0, 'rapidapi': 0})
                self.daily_total_calls = data.get('daily_total_calls', {'betsapi': 0, 'rapidapi': 0})
                self.monthly_total_calls = data.get('monthly_total_calls', {'betsapi': 0, 'rapidapi': 0})
                logger.info("Loaded persisted counters from file")
        except FileNotFoundError:
            logger.info("No persisted counters found, initializing new ones")
            self.current_cycle_calls = {'betsapi': 0, 'rapidapi': 0}
            self.hourly_total_calls = {'betsapi': 0, 'rapidapi': 0}
            self.daily_total_calls = {'betsapi': 0, 'rapidapi': 0}
            self.monthly_total_calls = {'betsapi': 0, 'rapidapi': 0}

    def save_counters(self) -> None:
        data = {
            'current_cycle_calls': self.current_cycle_calls,
            'hourly_total_calls': self.hourly_total_calls,
            'daily_total_calls': self.daily_total_calls,
            'monthly_total_calls': self.monthly_total_calls
        }
        with open(COUNTER_FILE, 'w') as f:
            json.dump(data, f)

    def reset_hourly_counters(self) -> None:
        est = pytz.timezone('America/New_York')
        current_hour = datetime.now(est).hour
        if current_hour != self.last_reset_hour:
            logger.info(f"Resetting hourly counters (Hour changed from {self.last_reset_hour} to {current_hour})")
            self.hourly_total_calls = {'betsapi': 0, 'rapidapi': 0}
            self.last_reset_hour = current_hour

    def reset_daily_counters(self) -> None:
        est = pytz.timezone('America/New_York')
        current_date = datetime.now(est).date()
        if current_date > self.last_reset_date:
            self.daily_total_calls = {'betsapi': 0, 'rapidapi': 0}
            self.last_reset_date = current_date

    def reset_monthly_counters(self) -> None:
        est = pytz.timezone('America/New_York')
        current_date = datetime.now(est)
        if current_date.month != self.last_reset_month:
            logger.info(f"Resetting monthly counters (Month changed from {self.last_reset_month} to {current_date.month})")
            self.monthly_total_calls = {'betsapi': 0, 'rapidapi': 0}
            self.last_reset_month = current_date.month

    def reset_cycle_counters(self) -> None:
        self.current_cycle_calls = {'betsapi': 0, 'rapidapi': 0}

    async def run(self) -> None:
        logger.info("TennisBot started. Press Ctrl+C to stop.")
        try:
            while True:
                start_time = time.time()
                logger.info("Starting fetch cycle...")
                self.reset_cycle_counters()
                self.reset_hourly_counters()
                self.reset_daily_counters()
                self.reset_monthly_counters()

                # 1) Fetch data from BetsAPI
                logger.info("[BetsAPI] Beginning fetch...")
                bets_start_time = time.time()
                bets_data = await self.betsapi_fetcher.get_tennis_data()
                bets_elapsed = time.time() - bets_start_time
                self.current_cycle_calls['betsapi'] = len(bets_data)
                self.hourly_total_calls['betsapi'] += len(bets_data)
                self.daily_total_calls['betsapi'] += len(bets_data)
                self.monthly_total_calls['betsapi'] += len(bets_data)
                logger.info(f"[BetsAPI] Fetch returned {len(bets_data)} records in {bets_elapsed:.2f} seconds.")

                # 2) Fetch data from RapidAPI
                logger.info("[RapidAPI] Beginning fetch...")
                rapid_start_time = time.time()
                rapid_data = await self.rapid_fetcher.get_tennis_data()
                rapid_elapsed = time.time() - rapid_start_time
                self.current_cycle_calls['rapidapi'] = len(rapid_data)
                self.hourly_total_calls['rapidapi'] += len(rapid_data)
                self.daily_total_calls['rapidapi'] += len(rapid_data)
                self.monthly_total_calls['rapidapi'] += len(rapid_data)
                logger.info(f"[RapidAPI] Fetch returned {len(rapid_data)} records in {rapid_elapsed:.2f} seconds.")

                # Save counters after updating
                self.save_counters()

                # 3) Merge data using TennisMerger
                logger.info("Merging data...")
                merge_start_time = time.time()
                merger = TennisMerger()
                merged_data = merger.merge(bets_data, rapid_data)
                merge_elapsed = time.time() - merge_start_time
                stats = merger.get_match_stats()
                total_elapsed = time.time() - start_time
                logger.info("\nAPI AND MATCH STATISTICS:")
                logger.info(f"  BetsAPI fetch time: {bets_elapsed:.2f} seconds")
                logger.info(f"  RapidAPI fetch time: {rapid_elapsed:.2f} seconds")
                logger.info(f"  Merge time: {merge_elapsed:.2f} seconds")
                logger.info(f"  Total cycle time: {total_elapsed:.2f} seconds")
                if merged_data and len(merged_data) > 0:
                    sample = merged_data[0]
                    logger.info("\nSAMPLE MATCHED EVENT DATA:")
                    if sample.get('betsapi_data'):
                        logger.info("  BetsAPI Data:")
                    if sample.get('rapid_data'):
                        logger.info("\n  RapidAPI Data:")

                logger.info("    Current Cycle:")
                logger.info(f"      BetsAPI calls: {self.current_cycle_calls['betsapi']}")
                logger.info(f"      RapidAPI calls: {self.current_cycle_calls['rapidapi']}")
                logger.info("    Hourly Totals (EST):")
                logger.info(f"      BetsAPI calls: {self.hourly_total_calls['betsapi']}")
                logger.info(f"      RapidAPI calls: {self.hourly_total_calls['rapidapi']}")
                logger.info("    Daily Totals (EST):")
                logger.info(f"      BetsAPI calls: {self.daily_total_calls['betsapi']}")
                logger.info(f"      RapidAPI calls: {self.daily_total_calls['rapidapi']}")
                logger.info("    Monthly Totals (EST):")
                logger.info(f"      BetsAPI calls: {self.monthly_total_calls['betsapi']}")
                logger.info(f"      RapidAPI calls: {self.monthly_total_calls['rapidapi']}")

                logger.info("  Match Results:")
                logger.info(f"    Total RapidAPI records: {len(rapid_data)}")
                logger.info(f"    Total BetsAPI records: {len(bets_data)}")
                logger.info(f"    Total unique matches: {stats.get('total_matches', 'N/A')}")
                logger.info(f"    RapidAPI-only matches: {stats.get('unmatched_rapid', 'N/A')}")
                logger.info(f"    BetsAPI-only matches: {stats.get('unmatched_bets', 'N/A')}")

                # Log unmatched events if any exist
                if stats.get('unmatched_rapid', 0) > 0:
                    logger.info("\nUnmatched RapidAPI events:")
                    for match in merged_data:
                        if match.get('rapid_data') and not match.get('betsapi_data'):
                            home = match['rapid_data']['raw_event_data'].get('team1', '')
                            away = match['rapid_data']['raw_event_data'].get('team2', '')
                            event_id = match['rapid_data']['raw_event_data'].get('eventId', '')
                            logger.info(f"  {home} vs {away} (EventId: {event_id})")

                if stats.get('unmatched_bets', 0) > 0:
                    logger.info("\nUnmatched BetsAPI events:")
                    for match in merged_data:
                        if match.get('betsapi_data') and not match.get('rapid_data'):
                            home, away = merger.get_player_names_from_record(match['betsapi_data'])
                            bet365_id = match['betsapi_data'].get('bet365_id', '')
                            logger.info(f"  {home} vs {away} (Bet365Id: {bet365_id})")

                # Update with the latest merged data
                global latest_tennis_data
                latest_tennis_data = merged_data

                message = {
                    "type": "match_data",
                    "matches": merged_data
                }
                message_json = json.dumps(message)

                elapsed = time.time() - self.last_fetch_time
                wait_time = max(0, self.fetch_interval - elapsed)
                logger.info(f"Fetch cycle complete. Sleeping for {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time)
                self.last_fetch_time = time.time()

        except asyncio.CancelledError:
            logger.warning("Fetch loop cancelled. Exiting gracefully.")
            self.save_counters()
            raise
        except Exception as e:
            logger.error("Error in TennisBot run loop", exc_info=True)
            self.save_counters()
        finally:
            logger.info("TennisBot run loop has exited.")

    async def run_single_cycle(self) -> None:
        """
        Runs a single data fetch cycle and returns the merged results.
        """
        start_time = time.time()
        logger.info(f"Starting fetch cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Reset cycle counters
        self.reset_cycle_counters()
        
        # Reset date counters if day/hour changed
        est = pytz.timezone('America/New_York')
        now = datetime.now(est)
        if now.date() != self.last_reset_date:
            self.reset_daily_counters()
            self.last_reset_date = now.date()
            
            # If month also changed, reset monthly counters
            if now.month != self.last_reset_month:
                self.reset_monthly_counters()
                self.last_reset_month = now.month
                
        # If hour changed, reset hourly counters
        if now.hour != self.last_reset_hour:
            self.reset_hourly_counters()
            self.last_reset_hour = now.hour
            
        # Step 1: Fetch data from BetsAPI
        logger.info("Fetching prematch data from BetsAPI...")
        bets_data = []
        try:
            bets_data = await self.betsapi_fetcher.get_tennis_data()
            self.current_cycle_calls['betsapi'] = len(bets_data)
            self.hourly_total_calls['betsapi'] += len(bets_data)
            self.daily_total_calls['betsapi'] += len(bets_data)
            self.monthly_total_calls['betsapi'] += len(bets_data)
        except Exception as e:
            logger.error(f"Error fetching from BetsAPI: {e}")
            
        # Step 2: Fetch data from RapidAPI
        logger.info("Fetching in-play odds from RapidAPI...")
        rapid_data = []
        try:
            rapid_data = await self.rapid_fetcher.get_tennis_data()
            self.current_cycle_calls['rapidapi'] = len(rapid_data)
            self.hourly_total_calls['rapidapi'] += len(rapid_data)
            self.daily_total_calls['rapidapi'] += len(rapid_data)
            self.monthly_total_calls['rapidapi'] += len(rapid_data)
        except Exception as e:
            logger.error(f"Error fetching from RapidAPI: {e}")
            
        # Step 3: Merge the data
        logger.info("Merging data from both APIs...")
        merged_data = []
        if bets_data and rapid_data:
            try:
                merger = TennisMerger()
                merged_data = merger.merge(bets_data, rapid_data)
                
                if merged_data and len(merged_data) > 0:
                    sample = merged_data[0]
                    logger.info("Sample match data:")
                    if 'betsapi_data' in sample:
                        logger.info("  BetsAPI Data:")
                    if 'rapid_data' in sample:
                        logger.info("\n  RapidAPI Data:")
                    
                # Print statistics
                rapid_only = 0
                bets_only = 0
                for match in merged_data:
                    if not match.get('betsapi_data'):
                        rapid_only += 1
                    if not match.get('rapid_data'):
                        bets_only += 1
                        
                # Also count matches from each source
                for match in merged_data:
                    match_id = match.get('match_id', 'unknown')
                    
                # Log a message with counts
                logger.info(f"Merger results: {len(merged_data)} total matches " +
                           f"({len(bets_data)} from BetsAPI, {len(rapid_data)} from RapidAPI)")
                
                # Store data globally to make available to FastAPI
                global latest_tennis_data
                latest_tennis_data = merged_data  # Store the merged data directly
                
                # Create response message
                message = {
                    "type": "match_data",
                    "matches": merged_data
                }
                message_json = json.dumps(message)
                
            except Exception as e:
                logger.error(f"Error in data merging: {e}")
                
        # Save updated counters
        self.save_counters()
        
        # Log counter information
        logger.info("API Usage Counters:")
        logger.info("  Current Cycle:")
        logger.info(f"    BetsAPI calls: {self.current_cycle_calls['betsapi']}")
        logger.info(f"    RapidAPI calls: {self.current_cycle_calls['rapidapi']}")
        logger.info("  Hourly Totals (EST):")
        logger.info(f"    BetsAPI calls: {self.hourly_total_calls['betsapi']}")
        logger.info(f"    RapidAPI calls: {self.hourly_total_calls['rapidapi']}")
        logger.info("  Daily Totals (EST):")
        logger.info(f"    BetsAPI calls: {self.daily_total_calls['betsapi']}")
        logger.info(f"    RapidAPI calls: {self.daily_total_calls['rapidapi']}")
        logger.info("  Monthly Totals (EST):")
        logger.info(f"    BetsAPI calls: {self.monthly_total_calls['betsapi']}")
        logger.info(f"    RapidAPI calls: {self.monthly_total_calls['rapidapi']}")
        logger.info("Match Results:")
        logger.info(f"  Total RapidAPI records: {len(rapid_data)}")
        logger.info(f"  Total BetsAPI records: {len(bets_data)}")
        logger.info(f"  Total unique matches: {len(merged_data)}")
        logger.info(f"  RapidAPI-only matches: {rapid_only}")
        logger.info(f"  BetsAPI-only matches: {bets_only}")
        
        # Track the time for this fetch cycle
        self.last_fetch_time = time.time()
        cycle_time = self.last_fetch_time - start_time
        sleep_time = max(0, self.fetch_interval - cycle_time)
        logger.info(f"Fetch cycle complete. Sleeping for {sleep_time:.2f} seconds.")
        
        # Return the merged data
        return merged_data

###############################################################################
# FastAPI app for serving data directly to frontend
###############################################################################
app = FastAPI()

# Add CORS to allow requests from your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only, restrict this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Send the latest data every time it's updated
            global latest_tennis_data
            if latest_tennis_data:
                try:
                    await websocket.send_json({
                        "timestamp": datetime.now().isoformat(),
                        "matches": latest_tennis_data
                    })
                except RuntimeError as e:
                    if "Cannot call 'send' once a close message has been sent" in str(e):
                        logger.info("WebSocket already closed")
                        break
                    raise
            # Wait for a moment to prevent overwhelming the client
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        try:
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.close()
        except Exception as e:
            logger.debug(f"Error during WebSocket cleanup: {e}", exc_info=True)

# REST endpoint alternative
@app.get("/api/tennis")
async def get_tennis_data():
    global latest_tennis_data
    return {
        "timestamp": datetime.now().isoformat(),
        "matches": latest_tennis_data
    }

# New endpoint for raw JSON data
@app.get("/api/tennis/raw")
async def get_raw_tennis_data():
    global latest_tennis_data
    # Return raw JSON with proper Content-Type and pretty printing
    return Response(
        content=json.dumps(
            {
                "timestamp": datetime.now().isoformat(),
                "matches": latest_tennis_data
            }, 
            indent=2
        ),
        media_type="application/json"
    )

# Function to start the API server
def start_api_server():
    try:
        logger.info("Starting API server on port 8000")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logger.error(f"Error starting API server: {e}")

###############################################################################
# Main Entry Point
###############################################################################
async def main() -> None:
    """
    Sets up graceful shutdown handlers, then runs the TennisBot indefinitely.
    """
    pid_file = "/tmp/tennis_bot.pid"
    try:
        with open(pid_file, 'x') as f:
            f.write(str(os.getpid()))
    except FileExistsError:
        try:
            with open(pid_file, 'r') as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)
            logger.error(f"Another instance is already running (PID: {old_pid})")
            return
        except (ProcessLookupError, ValueError):
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown_handler(loop))
    bot = TennisBot()
    api_thread = threading.Thread(target=start_api_server)
    api_thread.daemon = True  # Allow the thread to exit when main thread exits
    api_thread.start()
    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("KeyboardInterrupt or SystemExit received. Shutting down.")

__all__ = ['app']

def format_match_data(matches):
    formatted_data = []
    for match in matches:
        formatted_data.append(f"{match['player1']} vs {match['player2']}\nEvent ID: {match['event_id']}\nBet365 ID: {match['bet365_id']}\nMatch Status: {match['status']}\n")
    return formatted_data

# Example usage
matches = [
    {
        'player1': 'Honami Sodeyama',
        'player2': 'Koharu Niimi',
        'event_id': '6V170761094C13A_1_1',
        'bet365_id': '170761094',
        'status': ''
    },
    {
        'player1': 'Claudia S M Solis',
        'player2': 'Noelia Z Melgar',
        'event_id': '6V170781146C13A_1_1',
        'bet365_id': '170781146',
        'status': ''
    },
    # ... other matches ...
]

formatted_matches = format_match_data(matches)
for match in formatted_matches:
    print(match)
