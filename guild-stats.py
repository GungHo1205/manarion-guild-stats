#!/usr/bin/env python3
"""
Guild Stats Collection and Historical Data Logging Script - ENHANCED OPTIMIZED VERSION
- Tracks guild membership daily to prevent redundant API calls
- Only processes one player per guild per day (since all guild members share building levels)
- Fetches player data from leaderboards to calculate guild Nexus and Study levels
- Tracks daily progress against a baseline, calculating approximate codex usage
- Fetches market prices for all tradeable items
- Appends new hourly data to a historical log for chart visualization
- Handles API failures gracefully by using previous data points
- Enhanced with better caching, concurrent processing, and smart guild selection
"""

import json
import os
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
import concurrent.futures
from collections import defaultdict

# --- Configuration ---
# Replace the existing file constants in guild-stats.py:

# --- Configuration ---
API_BASE_URL = "https://api.manarion.com"
LEADERBOARD_TYPE = "boost_damage"
MAX_WORKERS = 2
API_DELAY = 1.5
BASE_PER_UPGRADE = 0.02
DATA_DIR = "docs"
GUILD_DATA_FILE = os.path.join(DATA_DIR, "guild-data.json")
BASELINE_FILE = os.path.join(DATA_DIR, "daily-baseline.json")
HISTORICAL_FILE = os.path.join(DATA_DIR, "historical-data.json")
# Consolidated cache file - replaces the old separate cache files
DAILY_GUILD_CACHE_FILE = os.path.join(DATA_DIR, "daily-guild-cache.json")

# --- Item Configuration with proper names ---
ITEM_MAPPING = {
    # Resources
    1: "Mana Dust", 7: "Fish", 8: "Wood", 9: "Iron",
    # Essentials
    2: "Elemental Shards", 3: "Codex",
    # Essences
    4: "Fire Essence", 5: "Water Essence", 6: "Nature Essence",
    # Equipment Materials
    10: "Asbestos", 11: "Ironbark", 12: "Fish Scales",
    # Spell Tomes
    13: "Tome of Fire", 14: "Tome of Water", 15: "Tome of Nature", 16: "Tome of Mana Shield",
    # Enchanting Formulas
    17: "Formula: Fire Resistance", 18: "Formula: Water Resistance", 19: "Formula: Nature Resistance",
    20: "Formula: Inferno", 21: "Formula: Tidal Wrath", 22: "Formula: Wildheart",
    23: "Formula: Insight", 24: "Formula: Bountiful Harvest", 25: "Formula: Prosperity",
    26: "Formula: Fortune", 27: "Formula: Growth", 28: "Formula: Vitality",
    # Enchanting Reagents
    29: "Elderwood", 30: "Lodestone", 31: "White Pearl",
    32: "Four-Leaf Clover", 33: "Enchanted Droplet", 34: "Infernal Heart",
    # Orbs/Upgrades
    35: "Orb of Power", 36: "Orb of Chaos", 37: "Orb of Divinity", 50: "Orb of Perfection", 45: "Orb of Legacy",
    46: "Elementium", 47: "Divine Essence",
    # Herbs
    39: "Sunpetal", 40: "Sageroot", 41: "Bloomwell",
    # Special
    44: "Crystallized Mana"
}

ITEM_CATEGORIES = {
    "Essentials": ["Elemental Shards", "Codex"],
    "Resources": ["Fish", "Wood", "Iron"],
    "Spell Tomes": ["Tome of Fire", "Tome of Water", "Tome of Nature", "Tome of Mana Shield"],
    "Orbs/Upgrades": ["Orb of Power", "Orb of Chaos", "Orb of Divinity", "Orb of Perfection", "Orb of Legacy", "Elementium", "Divine Essence"],
    "Herbs": ["Sunpetal", "Sageroot", "Bloomwell"],
    "Enchanting Reagents": ["Fire Essence", "Water Essence", "Nature Essence", "Asbestos", "Ironbark", "Fish Scales", 
                           "Elderwood", "Lodestone", "White Pearl", "Four-Leaf Clover", "Enchanted Droplet", "Infernal Heart"],
    "Enchanting Formulas": ["Formula: Fire Resistance", "Formula: Water Resistance", "Formula: Nature Resistance",
                           "Formula: Inferno", "Formula: Tidal Wrath", "Formula: Wildheart", "Formula: Insight",
                           "Formula: Bountiful Harvest", "Formula: Prosperity", "Formula: Fortune", "Formula: Growth", "Formula: Vitality"],
    "Special": ["Crystallized Mana"]
}

UNTRADEABLE_IDS = {38, 42, 43, 48, 49}

class APIClient:
    """Simplified API client focused on efficient batch processing."""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'GuildStatsTracker/2.2'})

    def get(self, endpoint: str, params: Optional[Dict] = None, retries: int = 3) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(retries):
            try:
                time.sleep(API_DELAY)
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                print(f"API Error on {url} (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    return None

    def get_multiple_players(self, player_names: List[str]) -> Dict[str, Dict]:
        """Fetch multiple players concurrently."""
        results = {}
        
        def fetch_player(name):
            try:
                data = self.get(f"/players/{name}")
                return name, data
            except Exception as e:
                print(f"Error fetching player {name}: {e}")
                return name, None
        
        print(f"Fetching {len(player_names)} players concurrently...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_player, name) for name in player_names]
            for future in concurrent.futures.as_completed(futures):
                try:
                    name, data = future.result()
                    if data:
                        results[name] = data
                except Exception as e:
                    print(f"Error in concurrent player fetch: {e}")
        
        return results

class GuildStatsTracker:
    def __init__(self):
        self.api = APIClient(API_BASE_URL)
        os.makedirs(DATA_DIR, exist_ok=True)
        self.guild_lookup = self._load_guild_lookup()
        # Remove the old cache file references
        self.daily_cache_file = os.path.join(DATA_DIR, "daily-guild-cache.json")
        self._ensure_data_files_exist()

    def _load_consolidated_cache(self) -> Dict:
        """Load the consolidated daily cache with level tracking metadata."""
        try:
            with open(self.daily_cache_file, 'r') as f:
                cache = json.load(f)
                
            # Ensure all required fields exist
            default_cache = {
                "date": None,
                "last_updated": None,
                "last_processed_hour": None,
                "guild_member_mapping": {},
                "processed_guilds_today": [],
                "guild_data": [],
                "processing_stats": {
                    "players_processed": 0,
                    "players_skipped": 0,
                    "guilds_found": 0,
                    "efficiency_percent": 0
                }
            }
            
            # Merge with defaults to ensure all fields exist
            for key, default_value in default_cache.items():
                if key not in cache:
                    cache[key] = default_value
                    
            return cache
            
        except (IOError, json.JSONDecodeError):
            return {
                "date": None,
                "last_updated": None,
                "last_processed_hour": None,
                "guild_member_mapping": {},
                "processed_guilds_today": [],
                "guild_data": [],
                "processing_stats": {
                    "players_processed": 0,
                    "players_skipped": 0,
                    "guilds_found": 0,
                    "efficiency_percent": 0
                }
            }

    def _save_consolidated_cache(self, cache_data: Dict):
        """Save the consolidated daily cache."""
        try:
            with open(self.daily_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except IOError as e:
            print(f"Error saving consolidated cache: {e}")

    def _is_new_day(self, cache_data: Dict) -> bool:
        """Check if it's a new day and we need to refresh guild mappings."""
        stored_date = cache_data.get("date")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return stored_date != today

    def fetch_current_guild_data(self) -> Tuple[List[Dict], bool]:
        """NEW: Fetch guild data directly from guild owners using the direct guild approach."""
        cache_data = self._load_consolidated_cache()
        
        # For level tracking, we need fresh data at least once per hour
        current_hour = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        cached_hour = cache_data.get("last_processed_hour")
        
        # Only reuse cached guild data if it's from the current hour
        if cached_hour == current_hour and cache_data.get("guild_data"):
            print(f"Using cached guild data from hour {current_hour}")
            return cache_data["guild_data"], True
        
        print("Fetching guild data using direct guild approach...")
        
        # Step 1: Load guilds from https://api.manarion.com/guilds
        print("Step 1: Loading guild list from API...")
        guilds_data = self.api.get("/guilds")
        if not guilds_data:
            print("Failed to fetch guild list")
            return [], False
        
        # Step 2: Log relevant details and prepare for sorting
        print("Step 2: Processing guild details...")
        guild_list = []
        for guild in guilds_data:
            guild_info = {
                "ID": guild.get("ID"),
                "OwnerID": guild.get("OwnerID"),
                "Name": guild.get("Name"),
                "Level": guild.get("Level", 0),
                "TotalUpgrades": guild.get("TotalUpgrades", 0)
            }
            guild_list.append(guild_info)
            print(f"  Guild: {guild_info['Name']} | ID: {guild_info['ID']} | Owner: {guild_info['OwnerID']} | Level: {guild_info['Level']} | Upgrades: {guild_info['TotalUpgrades']}")
        
        # Step 3: Sort guilds by priority (TotalUpgrades DESC, Level DESC, ID ASC)
        print("Step 3: Sorting guilds by priority (Upgrades > Level > ID)...")
        guild_list.sort(key=lambda g: (-g["TotalUpgrades"], -g["Level"], g["ID"]))
        
        # Take top 50 guilds
        top_guilds = guild_list[:30]
        print(f"Selected top {len(top_guilds)} guilds for processing")
        
        # Step 4-6: Process each guild owner to calculate levels
        print("Step 4-6: Processing guild owners for level calculations...")
        guild_data = []
        players_processed = 0
        players_skipped = 0
        
        for i, guild_info in enumerate(top_guilds):
            guild_name = guild_info["Name"]
            owner_id = guild_info["OwnerID"]
            
            print(f"  Processing guild {i+1}/{len(top_guilds)}: {guild_name} (Owner ID: {owner_id})")
            
            # Fetch owner's player data using the OwnerID
            player_data = self.api.get(f"/players/{owner_id}")
            
            if not player_data:
                print(f"    Failed to fetch owner data for {guild_name}, checking for cached data...")
                # Try to use cached data if available
                cached_guild = self._get_cached_guild_data(guild_name)
                if cached_guild:
                    print(f"    Using cached data for {guild_name}")
                    guild_data.append(cached_guild)
                else:
                    print(f"    Skipping {guild_name} - no data available")
                    players_skipped += 1
                continue
            
            # Calculate guild levels using the owner's data
            result = self.process_guild_owner_data(guild_name, player_data, guild_info["TotalUpgrades"])
            
            if result:
                # Add guild level to the result
                result["GuildLevel"] = guild_info["Level"]
                result["GuildID"] = guild_info["ID"]
                guild_data.append(result)
                players_processed += 1
                print(f"    {guild_name} -> Nexus: L{result['NexusLevel']}, Study: L{result['StudyLevel']}")
            else:
                print(f"    Failed to process data for {guild_name}")
                players_skipped += 1
        
        print(f"\nProcessing Summary:")
        print(f"   Guild owners processed: {players_processed}")
        print(f"   Guild owners skipped: {players_skipped}")
        print(f"   Total guilds with data: {len(guild_data)}")
        
        # Save the fresh data with current hour tracking
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cache_data = {
            "date": today,
            "guild_data": guild_data,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "last_processed_hour": current_hour,
            "guild_member_mapping": {},  # Not needed with direct approach
            "processed_guilds_today": [g["GuildName"] for g in guild_data],
            "processing_stats": {
                "players_processed": players_processed,
                "players_skipped": players_skipped,
                "guilds_found": len(guild_data),
                "efficiency_percent": 100  # Direct approach is always 100% efficient
            }
        }
        self._save_consolidated_cache(cache_data)
        
        return guild_data, True

    def _get_cached_guild_data(self, guild_name: str) -> Optional[Dict]:
        """Get cached data for a specific guild if available."""
        try:
            cache_data = self._load_consolidated_cache()
            for guild in cache_data.get("guild_data", []):
                if guild.get("GuildName") == guild_name:
                    return guild
            return None
        except:
            return None

    def process_guild_owner_data(self, guild_name: str, player_data: Dict, total_upgrades: int) -> Optional[Dict]:
        """NEW: Process guild owner data to calculate guild levels using ORIGINAL algorithm."""
        try:
            # Get boost data exactly as in the original code
            base_boosts = player_data.get("BaseBoosts", {})
            total_boosts = player_data.get("TotalBoosts", {})
            
            # CRITICAL FIX: Use BaseBoosts["40"] as the equivalent of leaderboard "Score" (upgrades)
            owner_upgrades = base_boosts.get("40", 0)
            
            codex_exp_boost = base_boosts.get("100", 0)
            total_exp_boost = total_boosts.get("100", 0)
            total_damage_percent = total_boosts.get("40", 0) * 100
            
            # Process equipment exactly as in the original code
            equipments = player_data.get("Equipment", {})
            totalEquipmentBoosts = 0
            enchant_boost = 0
            
            for item in range(1, 9):
                try:
                    item_key = str(item)
                    equipment_item = equipments.get(item_key, {})
                    
                    # CRITICAL: Only set enchant_boost for item 5 (enchanted item)
                    if item == 5:
                        enchant_boost = equipment_item.get("Boosts", {}).get("100", 0)
                    
                    # Handle infusions exactly like original code
                    infusions = equipment_item.get("Infusions", {})
                    # In original code, infusions is treated as a number directly
                    if isinstance(infusions, dict):
                        infusions_count = sum(v for v in infusions.values() if isinstance(v, (int, float)))
                    else:
                        infusions_count = infusions if isinstance(infusions, (int, float)) else 0
                    
                    base_boost = equipment_item.get("Boosts", {}).get("40", 0)
                    equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                    totalEquipmentBoosts += equip_percent
                    
                except Exception as e:
                    print(f"      Error processing equipment item {item}: {e}")
                    continue
            
            base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
            
            # CRITICAL FIX: Study room calculation - make sure enchant_boost is properly calculated
            study_level = self.calculate_study_level(total_exp_boost, codex_exp_boost, enchant_boost)
            nexus_level = self.calculate_nexus_level(base_damage_percent, owner_upgrades)
            
            print(f"      {guild_name} - Upgrades: {owner_upgrades}, Nexus: L{nexus_level}, Study: L{study_level}")
            print(f"        Study Debug - Total EXP: {total_exp_boost}, Codex: {codex_exp_boost}, Enchant: {enchant_boost}")
            
            return {
                "GuildName": guild_name, 
                "NexusLevel": nexus_level, 
                "StudyLevel": study_level,
                "TotalUpgrades": total_upgrades,  # Keep guild total upgrades for final sorting
                "GuildLevel": 0  # Will be filled from guild data
            }
            
        except Exception as e:
            print(f"      Error processing guild owner data for {guild_name}: {e}")
            return None

    def _should_process_player(self, player_name: str, cache_data: Dict) -> Tuple[bool, Optional[str]]:
        """Determine if we should process this player based on guild tracking."""
        guild_name = cache_data["guild_member_mapping"].get(player_name)
        
        if not guild_name:
            # Player not in our mapping - process them (might be from new/unknown guild)
            return True, None
        
        # Check if this guild has already been processed today
        if guild_name in cache_data["processed_guilds_today"]:
            return False, guild_name  # Skip - guild already processed
        
        return True, guild_name  # Process - first player from this guild

    def _mark_guild_as_processed(self, guild_name: str, cache_data: Dict):
        """Mark a guild as processed for today."""
        if guild_name not in cache_data["processed_guilds_today"]:
            cache_data["processed_guilds_today"].append(guild_name)
            self._save_consolidated_cache(cache_data)

    def _should_process_player_for_levels(self, player_name: str, cache_data: Dict, processed_this_session: Set[str]) -> Tuple[bool, str]:
        """
        Determine if we should process this player for level tracking.
        Returns (should_process, reason)
        """
        guild_name = cache_data["guild_member_mapping"].get(player_name)
        
        if guild_name:
            # Known guild member
            if guild_name in processed_this_session:
                return False, f"Guild {guild_name} already processed this session"
            else:
                return True, f"First representative of {guild_name} this session"
        else:
            # Unknown player - might be from new guild
            return True, "Unknown player - checking for new/unmapped guild"

    def _ensure_data_files_exist(self):
        """Creates empty placeholder files if they don't exist to prevent frontend errors."""
        empty_files = {
            GUILD_DATA_FILE: {
                "guilds": [], 
                "dustSpending": {"total_codex": 0, "formatted_dust": "0.00", "formatted_price": "N/A"}, 
                "lastUpdated": None, 
                "baselineDate": None
            },
            BASELINE_FILE: {"date": None, "guilds": {}},
            HISTORICAL_FILE: {"guild_history": {}, "item_prices": {}, "item_categories": ITEM_CATEGORIES},
            # New consolidated cache file
            DAILY_GUILD_CACHE_FILE: {
                "date": None,
                "last_updated": None,
                "guild_member_mapping": {},
                "processed_guilds_today": [],
                "guild_data": []
            }
        }
        for path, content in empty_files.items():
            if not os.path.exists(path):
                print(f"Creating placeholder file: {path}")
                with open(path, 'w') as f:
                    json.dump(content, f, indent=2)

    def _load_guild_lookup(self) -> Dict[int, str]:
        print("Loading guild lookup...")
        data = self.api.get("/guilds")
        if not data:
            print("WARNING: Could not load guild lookup. Using cached data if available.")
            try:
                with open(os.path.join(DATA_DIR, "guild_cache.json"), 'r') as f:
                    cached = json.load(f)
                    return cached.get("guild_lookup", {})
            except (IOError, json.JSONDecodeError):
                return {}
        
        guild_lookup = {g.get("ID", 0): g.get("Name", "Unknown") for g in data}
        try:
            with open(os.path.join(DATA_DIR, "guild_cache.json"), 'w') as f:
                json.dump({"guild_lookup": guild_lookup, "last_updated": datetime.now(timezone.utc).isoformat()}, f)
        except IOError:
            pass
        
        return guild_lookup

    def _get_previous_data_point(self, data_type: str = "guild") -> Optional[Dict]:
        """Get the most recent data point for fallback purposes."""
        try:
            if data_type == "guild":
                with open(GUILD_DATA_FILE, 'r') as f:
                    return json.load(f)
            elif data_type == "historical":
                with open(HISTORICAL_FILE, 'r') as f:
                    return json.load(f)
        except (IOError, json.JSONDecodeError):
            return None

    def _get_latest_market_prices(self) -> Dict:
        """Get latest market prices from historical data if API fails."""
        try:
            with open(HISTORICAL_FILE, 'r') as f:
                history = json.load(f)
            
            market_prices = {}
            for item_name, data in history.get('item_prices', {}).items():
                prices = data.get('prices', [])
                if prices:
                    latest = prices[-1]
                    market_prices[item_name] = {"buy": latest['buy'], "sell": latest['sell']}
            
            print(f"Using cached market prices for {len(market_prices)} items")
            return market_prices
        except (IOError, json.JSONDecodeError):
            return {}

    # --- Calculation Formulas (unchanged) ---
    def calculate_nexus_level(self, research_damage_percent, upgrades):
        """Calculate nexus level using the correct formula."""
        if upgrades <= 0:
            return 0
        
        base_pct = 0.02
        multiplier = research_damage_percent / (upgrades * base_pct)
        level = 100 * (multiplier - 1.0)
        return max(0, round(level))

    def calculate_study_level(self, total_exp_boost, codex_boost, enchant_boost):
        """Calculate study level."""
        return max(0, total_exp_boost - codex_boost - enchant_boost)

    def safe_get_infusions_count(self, infusions_data):
        """Safely extract infusion count from infusions data."""
        if isinstance(infusions_data, dict):
            return sum(v for v in infusions_data.values() if isinstance(v, (int, float)))
        elif isinstance(infusions_data, (int, float)):
            return infusions_data
        else:
            return 0

    def process_player(self, player_entry: Dict) -> Optional[Dict]:
        """Process player with corrected formulas matching your working code exactly."""
        player_name = player_entry.get("Name")
        upgrades = player_entry.get("Score", 0)
        if not player_name: 
            return None

        player = self.api.get(f"/players/{player_name}")
        if not player: 
            return None

        guild_id = player.get("GuildID", 0)
        guild_name = self.guild_lookup.get(guild_id)
        if not guild_name or guild_name == "Unknown": 
            return None

        # Get boost data exactly as in your working code
        base_boosts = player.get("BaseBoosts", {})
        total_boosts = player.get("TotalBoosts", {})
        
        codex_exp_boost = base_boosts.get("100", 0)
        total_exp_boost = total_boosts.get("100", 0)
        total_damage_percent = total_boosts.get("40", 0) * 100
        
        # Process equipment exactly as in your working code
        equipments = player.get("Equipment", {})
        totalEquipmentBoosts = 0
        enchant_boost = 0
        
        for item in range(1, 9):
            try:
                item_key = str(item)
                item_data = equipments.get(item_key, {})
                if not item_data:
                    continue
                
                if item == 5:
                    enchant_boost = item_data.get("Boosts", {}).get("100", 0)
                
                infusions_raw = item_data.get("Infusions", {})
                infusions_count = self.safe_get_infusions_count(infusions_raw)
                base_boost = item_data.get("Boosts", {}).get("40", 0)
                equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                totalEquipmentBoosts += equip_percent
                
            except Exception as e:
                print(f"Error processing equipment item {item} for {player_name}: {e}")
                continue
        
        base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
        
        # Calculate levels
        study_level = self.calculate_study_level(total_exp_boost, codex_exp_boost, enchant_boost)
        nexus_level = self.calculate_nexus_level(base_damage_percent, upgrades)
        
        return {
            "GuildName": guild_name, 
            "NexusLevel": nexus_level, 
            "StudyLevel": study_level
        }

    def calculate_codex_cost(self, start_level: int, progress: int) -> int:
        if progress <= 0: return 0
        return sum(range(start_level + 1, start_level + progress + 1))

    # --- Enhanced Data Processing ---
    def is_daily_update_required(self) -> bool:
        """Checks if a full daily refresh is required based on the last update time."""
        try:
            with open(DAILY_GUILD_CACHE_FILE, 'r') as f:
                cache = json.load(f)
            last_updated_str = cache.get("lastUpdated")
            if not last_updated_str:
                return True
            last_updated_time = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            return now.date() > last_updated_time.date()
        except (FileNotFoundError, json.JSONDecodeError):
            return True

    def update_historical_data(self, guilds: List[Dict], market_prices: Dict, timestamp: str):
        try:
            with open(HISTORICAL_FILE, 'r') as f:
                history = json.load(f)
        except (IOError, json.JSONDecodeError):
            history = {"guild_history": {}, "item_prices": {}, "item_categories": ITEM_CATEGORIES}

        if "item_categories" not in history:
            history["item_categories"] = ITEM_CATEGORIES

        # Update guild history only with fresh data
        for guild in guilds:
            name = guild['GuildName']
            if name not in history['guild_history']: 
                history['guild_history'][name] = []
            history['guild_history'][name].append({
                "timestamp": timestamp, 
                "nexus": guild['NexusLevel'], 
                "study": guild['StudyLevel']
            })

        # Update item price history
        for item_name, prices in market_prices.items():
            if item_name not in history['item_prices']: 
                history['item_prices'][item_name] = {"prices": []}
            history['item_prices'][item_name]['prices'].append({
                "timestamp": timestamp, 
                "buy": prices['buy'], 
                "sell": prices['sell']
            })
        
        # Prune old data (older than 30 days)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        for data in history['guild_history'].values():
            data[:] = [d for d in data if d['timestamp'] >= cutoff]
        for data in history['item_prices'].values():
            data['prices'][:] = [p for p in data['prices'] if p['timestamp'] >= cutoff]
            
        with open(HISTORICAL_FILE, 'w') as f:
            json.dump(history, f)
        print("Historical data updated.")

    def fetch_market_prices(self) -> Tuple[Dict, bool]:
        """Fetch market prices with caching fallback."""
        print("Fetching market prices for all items...")
        market_data = self.api.get("/market")
        
        if not market_data:
            print("Market API failed, using previous data...")
            return self._get_latest_market_prices(), False

        prices = {}
        buy_data, sell_data = market_data.get("Buy", {}), market_data.get("Sell", {})
        for item_id, item_name in ITEM_MAPPING.items():
            if item_id in UNTRADEABLE_IDS: continue
            buy_price, sell_price = buy_data.get(str(item_id)), sell_data.get(str(item_id))
            if buy_price and sell_price:
                prices[item_name] = {"buy": buy_price, "sell": sell_price}
        
        print(f"Fetched fresh prices for {len(prices)} items.")
        return prices, True

    def calculate_average_codex_price(self) -> float:
        """Calculate average Codex price from recent historical data."""
        try:
            with open(HISTORICAL_FILE, 'r') as f:
                history = json.load(f)
            
            codex_prices = history.get('item_prices', {}).get('Codex', {}).get('prices', [])
            if not codex_prices:
                return 10000000000  # Default fallback price
            
            # Use last 24 hours of data for average
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            recent_prices = [
                p for p in codex_prices[-24:] 
                if datetime.fromisoformat(p['timestamp'].replace('Z', '+00:00')) >= cutoff
            ]
            
            if not recent_prices:
                recent_prices = codex_prices[-1:]  # Use most recent if no recent data
            
            total_avg = sum((p['buy'] + p['sell']) / 2 for p in recent_prices)
            return total_avg / len(recent_prices)
            
        except (IOError, json.JSONDecodeError, KeyError):
            return 10000000000  # Default fallback price

    def run_update(self):
        """Enhanced main execution method with level tracking validation."""
        start_time = time.time()
        print(f" Starting optimized guild level tracking at {datetime.now(timezone.utc).isoformat()}")
        
        if not self.guild_lookup: 
            print(" No guild lookup available, cannot proceed.")
            return

        timestamp = datetime.now(timezone.utc).isoformat()
        today_str = timestamp.split('T')[0]
        
        # Fetch current data
        current_guilds, guild_data_fresh = self.fetch_current_guild_data()
        market_prices, market_data_fresh = self.fetch_market_prices()
        
        # Validate level tracking data
        if guild_data_fresh and current_guilds:
            print(f"\n Level Tracking Validation:")
            print(f"   Guilds with level data: {len(current_guilds)}")
            
            # Show some examples of tracked levels
            for i, guild in enumerate(current_guilds[:5]):  # Show first 5 guilds
                print(f"   {guild['GuildName']}: Nexus L{guild['NexusLevel']}, Study L{guild['StudyLevel']}")
            
            if len(current_guilds) > 5:
                print(f"   ... and {len(current_guilds) - 5} more guilds")
        
        # Load/create baseline - ALWAYS check for new day, regardless of data freshness
        try:
            with open(BASELINE_FILE, 'r') as f: 
                baseline = json.load(f)
        except (IOError, json.JSONDecodeError): 
            baseline = {"date": None, "guilds": {}}

        # Check for new day and create baseline if needed
        if baseline.get("date") != today_str and current_guilds:
            print(f" New day detected. Creating new baseline for {today_str}.")
            baseline = {
                "date": today_str,
                "created_at": timestamp,  # Add this line
                "guilds": {
                    g["GuildName"]: {
                        "NexusLevel": g["NexusLevel"], 
                        "StudyLevel": g["StudyLevel"]
                    } for g in current_guilds
                }
            }
            with open(BASELINE_FILE, 'w') as f: 
                json.dump(baseline, f, indent=2)
            print(f" Baseline created/updated for {len(current_guilds)} guilds.")
        elif baseline.get("date") == today_str:
            print(f" Using existing baseline from {today_str}.")
        else:
            print(f" No current guild data available for baseline creation.")

        # Only update historical data if we have fresh data
        if guild_data_fresh or market_data_fresh:
            guild_data_for_history = current_guilds if guild_data_fresh else []
            self.update_historical_data(guild_data_for_history, market_prices if market_data_fresh else {}, timestamp)

        # Calculate progress and codex cost (CRITICAL: This is where level changes are tracked!)
        total_codex = 0
        guilds_with_progress = 0
        
        for guild in current_guilds:
            base = baseline.get("guilds", {}).get(guild["GuildName"])
            if base:
                # Calculate actual progress from baseline
                nexus_progress = max(0, guild["NexusLevel"] - base["NexusLevel"])
                study_progress = max(0, guild["StudyLevel"] - base["StudyLevel"])
                
                guild["NexusProgress"] = nexus_progress
                guild["StudyProgress"] = study_progress
                guild["TotalCodexCost"] = (
                    self.calculate_codex_cost(base["NexusLevel"], nexus_progress) +
                    self.calculate_codex_cost(base["StudyLevel"], study_progress)
                )
                total_codex += guild["TotalCodexCost"]
                
                if nexus_progress > 0 or study_progress > 0:
                    guilds_with_progress += 1
            else:
                # New guild - no baseline data yet
                guild["NexusProgress"] = guild["StudyProgress"] = guild["TotalCodexCost"] = 0
        
        # Calculate dust spending using average Codex price
        avg_price = self.calculate_average_codex_price()
        
        dust_spending = {
            "total_codex": total_codex,
            "formatted_dust": self.format_currency(total_codex * avg_price),
            "formatted_price": self.format_currency(avg_price)
        }

        # Save final output with tracking metadata - FIXED SORTING ORDER
        final_data = {
            "guilds": sorted(current_guilds, key=lambda g: (
                -g.get("NexusLevel", 0),      
                -g.get("StudyLevel", 0),      
                -g.get("TotalUpgrades", 0),   
                -g.get("GuildLevel", 0),      
                g.get("GuildID", 999999)      
            )),
            "dustSpending": dust_spending,
            "lastUpdated": timestamp,
            "baselineDate": baseline.get("date"),
            "baselineCreatedAt": baseline.get("created_at"),  # Add this line
            "dataFreshness": {
                "guild_data_fresh": guild_data_fresh,
                "market_data_fresh": market_data_fresh
            },
            "levelTrackingStats": {
                "guilds_tracked": len(current_guilds),
                "guilds_with_progress": guilds_with_progress,
                "total_codex_used": total_codex,
                "baseline_date": baseline.get("date")
            }
        }
        
        with open(GUILD_DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)
        
        execution_time = time.time() - start_time
        
        print(f"\n Level tracking update complete in {execution_time:.2f} seconds!")
        print(f" Final Summary:")
        print(f"   Guilds tracked: {len(current_guilds)}")
        print(f"   Guilds with progress today: {guilds_with_progress}")
        print(f"   Total Codex used today: {total_codex:,}")
        print(f"   Estimated dust spent: {dust_spending['formatted_dust']}")
        print(f"   Performance: {len(current_guilds)/execution_time:.1f} guilds/second")
        print(f"   Fresh data: guilds={guild_data_fresh}, market={market_data_fresh}")
        print(f"   Baseline date: {baseline.get('date')}")
        
        # Load and display processing stats
        cache_data = self._load_consolidated_cache()
        if cache_data.get("processing_stats"):
            stats = cache_data["processing_stats"]
            print(f"   Efficiency: {stats['efficiency_percent']}% (Direct guild approach)")
            if stats['players_skipped'] > 0:
                print(f"   API optimization: {stats['players_skipped']} calls avoided")

    def format_currency(self, amount: float) -> str:
        if amount >= 1e12: return f"{amount / 1e12:.2f}T"
        if amount >= 1e9: return f"{amount / 1e9:.2f}B"
        if amount >= 1e6: return f"{amount / 1e6:.2f}M"
        if amount >= 1e3: return f"{amount / 1e3:.2f}K"
        return f"{amount:.2f}"

if __name__ == "__main__":
    tracker = GuildStatsTracker()
    tracker.run_update()