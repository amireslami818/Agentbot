"""Module for merging tennis data from multiple sources."""

import re
import logging
from typing import List, Dict, Any, Optional
from rapidfuzz import fuzz
from datetime import datetime

logger = logging.getLogger(__name__)

class TennisMerger:
    # Constants for name matching
    PUNCTUATIONS = [",", "-", "'"]  # Periods handled separately
    FUZZ_THRESHOLD = 80  # Similarity score threshold (0-100)

    def __init__(self):
        # Track how many times we resort to fuzzy matching
        self.fuzzy_fallback_count = 0
        # Optionally, store today's date so we can reset daily if desired
        self.last_reset_date = datetime.now().date()

    def normalize_name(self, name: str) -> str:
        """
        Basic normalization for player names:
        - Convert to lowercase
        - Handle initials (j.r. -> jr)
        - Replace punctuation with spaces
        - Collapse multiple spaces
        - Special handling for multiple initials
        """
        name = name.lower()
        # Replace certain punctuation (hyphens, commas, apostrophes) with spaces
        # but keep periods for special handling of initials
        for ch in [",", "-", "'"]:
            name = name.replace(ch, " ")

        # Split on whitespace
        tokens = name.split()
        
        expanded_tokens = []
        for token in tokens:
            # If this chunk has multiple periods (e.g., "j.r."), let's split them out
            if token.count(".") > 1:
                # Example: "j.r." -> split on "." -> ["j", "r", ""]
                subparts = token.split(".")
                # Filter out empty strings and re-add the "." so "j" -> "j."
                for sp in subparts:
                    if sp:  # if not empty
                        expanded_tokens.append(sp + ".")
            else:
                expanded_tokens.append(token)

        # Now we have a list where multiple initials are separated, e.g. ["j.", "r."] instead of "j.r."
        # Next we merge consecutive single-letter initials:
        merged_parts = []
        i = 0
        while i < len(expanded_tokens):
            part = expanded_tokens[i]
            
            # Check if this part is a single-letter+period, e.g. "j."
            if len(part) == 2 and part[1] == "." and part[0].isalpha():
                # Look ahead to see if next is also single-letter+period
                if (i + 1 < len(expanded_tokens)
                    and len(expanded_tokens[i+1]) == 2
                    and expanded_tokens[i+1][1] == "."
                    and expanded_tokens[i+1][0].isalpha()):
                    
                    # Merge them, e.g. "j." + "r." -> "jr"
                    combined = part[0] + expanded_tokens[i+1][0]
                    merged_parts.append(combined)
                    i += 2  # Skip the next token (we merged it)
                else:
                    # Just a single initial on its own, strip the period
                    merged_parts.append(part[0])
                    i += 1
            else:
                # Replace any leftover periods in longer tokens with space
                part = part.replace(".", " ")
                merged_parts.append(part)
                i += 1

        # Join and split again to clean up extra spaces
        result = " ".join(merged_parts)
        result = " ".join(result.split())
        return result

    def fuzzy_match_names(self, name1: str, name2: str, threshold: Optional[int] = None) -> bool:
        """
        Uses RapidFuzz to measure string similarity.
        Uses partial_ratio to better handle abbreviated names (e.g., "N. Djokovic" vs "Novak Djokovic")
        """
        if threshold is None:
            threshold = self.FUZZ_THRESHOLD
            
        norm1 = self.normalize_name(name1)
        norm2 = self.normalize_name(name2)
        
        # Use partial_ratio for better matching of abbreviated names
        ratio = fuzz.partial_ratio(norm1, norm2)
        return ratio >= threshold

    def names_are_equivalent(
        self,
        rapid_home: str,
        rapid_away: str,
        bets_home: str,
        bets_away: str,
        threshold: Optional[int] = None
    ) -> bool:
        """
        Checks if two pairs of names (rapid vs. bets) represent the same match
        by either direct or flipped fuzzy matching.
        """
        if threshold is None:
            threshold = self.FUZZ_THRESHOLD

        # Try direct match (home->home, away->away)
        direct_match = (
            self.fuzzy_match_names(rapid_home, bets_home, threshold) and
            self.fuzzy_match_names(rapid_away, bets_away, threshold)
        )
        
        # Try flipped match (home->away, away->home)
        flipped_match = (
            self.fuzzy_match_names(rapid_home, bets_away, threshold) and
            self.fuzzy_match_names(rapid_away, bets_home, threshold)
        )
        
        return direct_match or flipped_match

    def merge_events_and_odds(self, events: List[Dict], odds: Dict[str, Dict]) -> List[Dict]:
        """
        Merge tennis events with their corresponding odds data.
        For RapidAPI data, odds are in raw_odds_data field.
        """
        merged_data = []
        
        for event in events:
            match_id = event.get("match_id")
            if not match_id:
                continue
                
            # For RapidAPI data
            if "raw_odds_data" in event:
                match_odds = event["raw_odds_data"]
            # For BetsAPI data
            elif match_id in odds:
                match_odds = odds[match_id]
            else:
                match_odds = {}
            
            merged_match = {
                **event,
                "odds": match_odds
            }
            merged_data.append(merged_match)
            
        logger.info(f"Merged {len(merged_data)} matches with their odds")
        return merged_data

    def get_player_names_from_record(self, bets_record: Dict[str, Any]) -> tuple:
        """
        Extract player names from a BetsAPI record, checking both players dict and inplay_event.
        Returns a tuple of (home_player, away_player).
        """
        # First try players dict
        players = bets_record.get("players", {})
        home_name = players.get("home", "")
        away_name = players.get("away", "")

        # If either name is empty, try inplay_event
        if not home_name or not away_name:
            inplay = bets_record.get("inplay_event", {})
            home = inplay.get("home", {})
            away = inplay.get("away", {})
            home_name = home_name or home.get("name", "")
            away_name = away_name or away.get("name", "")

        # Update names consistently throughout the record
        if home_name and away_name:
            self.update_names_in_record(bets_record, home_name, away_name)

        return home_name, away_name

    def update_names_in_record(self, record: Dict[str, Any], home_name: str, away_name: str) -> None:
        """
        Update player names consistently throughout a record's nested data structures.
        """
        # Update BetsAPI inplay_event
        if "inplay_event" in record:
            inplay = record["inplay_event"]
            if "home" in inplay:
                inplay["home"]["name"] = home_name
            if "away" in inplay:
                inplay["away"]["name"] = away_name

        # Update RapidAPI raw_event_data
        if "raw_event_data" in record:
            event_data = record["raw_event_data"]
            event_data["team1"] = home_name
            event_data["team2"] = away_name
            # Also update eventName for consistency
            event_data["eventName"] = f"{home_name} - {away_name}"

        # Update players dict if it exists
        if "players" in record:
            record["players"]["home"] = home_name
            record["players"]["away"] = away_name

    def extract_bet365_id_from_eventid(self, event_id: str) -> Optional[str]:
        """
        Extract bet365_id from RapidAPI's eventId format.
        Example: "6V170281242C13A_1_1" contains bet365_id "170281242"
        Returns None if no valid bet365_id is found.
        """
        if not event_id:
            return None
            
        # Look for a sequence of digits that could be a bet365_id
        matches = re.findall(r'\d{9,}', event_id)
        return matches[0] if matches else None

    def merge(self, 
             prematch_data: List[Dict], 
             live_data: List[Dict]) -> List[Dict]:
        """
        Merge pre-match and live match data
        
        Args:
            prematch_data (List[Dict]): Pre-match data from BetsAPI
            live_data (List[Dict]): Live match data from RapidAPI
            
        Returns:
            List[Dict]: Merged match data
        """
        # Reset merged matches
        self.merged_matches = {}
        
        # Process pre-match data first
        for match in prematch_data:
            if not match:
                continue
                
            # BetsAPI match IDs can be in multiple fields
            bet365_id = str(match.get('bet365_id', ''))
            inplay_id = str(match.get('inplay_event', {}).get('id', ''))
            fi_id = str(match.get('FI', ''))
            
            # Use any valid ID we can find
            match_id = bet365_id or inplay_id or fi_id
            if not match_id:
                continue
                
            self.merged_matches[match_id] = {
                'match_id': match_id,
                'betsapi_data': match
            }

        # Update with live data where available
        for match in live_data:
            if not match or not match.get('raw_event_data'):
                continue
                
            raw_event = match['raw_event_data']
            
            # Try to extract bet365_id from RapidAPI's eventId
            event_id = str(raw_event.get('eventId', ''))
            extracted_bet365_id = self.extract_bet365_id_from_eventid(event_id)
            market_fi = str(raw_event.get('marketFI', ''))
            
            # First try: Look for extracted bet365_id in our merged matches
            if extracted_bet365_id and extracted_bet365_id in self.merged_matches:
                logger.info(f"Found match by extracted bet365_id {extracted_bet365_id} from eventId {event_id}")
                self.merged_matches[extracted_bet365_id]['rapid_data'] = match
                continue
                
            # Second try: Check if marketFI matches any existing IDs
            if market_fi and market_fi in self.merged_matches:
                logger.info(f"Found match by marketFI {market_fi}")
                self.merged_matches[market_fi]['rapid_data'] = match
                continue
            
            # Final try: Fall back to fuzzy name matching
            home_name = str(raw_event.get('team1', ''))
            away_name = str(raw_event.get('team2', ''))
            
            found_match = False
            for existing_id, existing_match in self.merged_matches.items():
                if not existing_match.get('rapid_data'):  # Only look at unmatched BetsAPI events
                    bets_data = existing_match['betsapi_data']
                    bets_home, bets_away = self.get_player_names_from_record(bets_data)
                    
                    if self.names_are_equivalent(home_name, away_name, bets_home, bets_away):
                        logger.info(f"Found match by fuzzy name matching for event {existing_id}")
                        self.merged_matches[existing_id]['rapid_data'] = match
                        found_match = True
                        break
            
            if not found_match:
                # Log detailed information about the unmatched event
                if extracted_bet365_id:
                    logger.info(f"Event with extracted bet365_id {extracted_bet365_id} from eventId {event_id} not found in BetsAPI data")
                else:
                    logger.info(f"Event {event_id} (marketFI: {market_fi}) with players {home_name} vs {away_name} not found in BetsAPI data")
                    
                # Create new entry with just RapidAPI data
                match_id = event_id or market_fi
                if match_id:
                    self.merged_matches[match_id] = {
                        'match_id': match_id,
                        'rapid_data': match,
                        'betsapi_data': None
                    }
        
        return list(self.merged_matches.values())

    def get_match_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the merged data.
        
        Returns:
            Dict with the following keys:
            - total_matches: Total number of matches
            - successful_matches: Number of matches with both BetsAPI and RapidAPI data
            - unmatched_bets: Number of matches with only BetsAPI data
            - unmatched_rapid: Number of matches with only RapidAPI data
        """
        if not hasattr(self, 'merged_matches'):
            self.merged_matches = {}
        
        total_matches = len(self.merged_matches)
        matches_with_both = sum(1 for m in self.merged_matches.values() 
                              if m.get('betsapi_data') and m.get('rapid_data'))
        matches_bets_only = sum(1 for m in self.merged_matches.values() 
                              if m.get('betsapi_data') and not m.get('rapid_data'))
        matches_rapid_only = sum(1 for m in self.merged_matches.values() 
                               if not m.get('betsapi_data') and m.get('rapid_data'))
        
        return {
            'total_matches': total_matches,
            'successful_matches': matches_with_both,
            'unmatched_bets': matches_bets_only,
            'unmatched_rapid': matches_rapid_only
        }

    def get_possible_ids(self, data: Dict[str, Any], fields: List[str]) -> set:
        """
        Extracts possible IDs from the given data by scanning the specified fields.
        """
        ids = set()
        for field in fields:
            value = data.get(field)
            if value:
                # Use regular expression to find numeric substrings
                ids.update(re.findall(r'\d+', str(value)))
        return ids

    def reset_fallback_count_if_new_day(self):
        """
        Resets the fuzzy fallback count if today's date is different from the last reset date.
        """
        today = datetime.now().date()
        if today != self.last_reset_date:
            self.fuzzy_fallback_count = 0
            self.last_reset_date = today

__all__ = ['TennisMerger']

if __name__ == "__main__":
    pass