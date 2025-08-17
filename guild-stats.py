#!/usr/bin/env python3
"""
Guild Stats Collection and Historical Data Logging Script
- Fetches player data from leaderboards to calculate guild Nexus and Study levels.
- Tracks daily progress against a baseline, calculating approximate codex usage.
- Fetches market prices for all tradeable items.
- Appends new hourly data to a historical log for chart visualization.
- Handles API failures gracefully by using previous data points.
"""

import json
import os
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import concurrent.futures

# --- Configuration ---
API_BASE_URL = "https://api.manarion.com"
LEADERBOARD_TYPE = "boost_damage"
MAX_WORKERS = 2
API_DELAY = 2
BASE_PER_UPGRADE = 0.02
DATA_DIR = "docs"
GUILD_DATA_FILE = os.path.join(DATA_DIR, "guild-data.json")
BASELINE_FILE = os.path.join(DATA_DIR, "daily-baseline.json")
HISTORICAL_FILE = os.path.join(DATA_DIR, "historical-data.json")

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
    """Handles API communication with error handling and rate limiting."""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'GuildStatsTracker/2.0'})

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
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                else:
                    return None

class GuildStatsTracker:
    """Orchestrates data collection, processing, and file I/O."""
    def __init__(self):
        self.api = APIClient(API_BASE_URL)
        os.makedirs(DATA_DIR, exist_ok=True)
        self.guild_lookup = self._load_guild_lookup()
        self._ensure_data_files_exist()

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
            HISTORICAL_FILE: {"guild_history": {}, "item_prices": {}, "item_categories": ITEM_CATEGORIES}
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
            # Try to load from previous successful run
            try:
                with open(os.path.join(DATA_DIR, "guild_cache.json"), 'r') as f:
                    cached = json.load(f)
                    return cached.get("guild_lookup", {})
            except (IOError, json.JSONDecodeError):
                return {}
        
        guild_lookup = {g.get("ID", 0): g.get("Name", "Unknown") for g in data}
        # Cache successful guild lookup
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
                    latest = prices[-1]  # Most recent price
                    market_prices[item_name] = {"buy": latest['buy'], "sell": latest['sell']}
            
            print(f"Using cached market prices for {len(market_prices)} items")
            return market_prices
        except (IOError, json.JSONDecodeError):
            return {}

    # --- Calculation Formulas ---
    def calculate_nexus_level(self, research_damage_percent, upgrades):
        """Calculate nexus level using the correct formula."""
        if upgrades <= 0:
            return 0
        
        # Use 0.02 as the base percentage per upgrade (matching your working code)
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
        
        # CRITICAL FIX: Your working code multiplies by 100 here!
        # total_damage_percent = player_data.get("TotalBoosts", {}).get("40", 0) * 100
        total_damage_percent = total_boosts.get("40", 0) * 100
        
        # Process equipment exactly as in your working code
        equipments = player.get("Equipment", {})
        totalEquipmentBoosts = 0  # Using same variable name as working code
        enchant_boost = 0
        
        for item in range(1, 9):
            try:
                item_key = str(item)
                item_data = equipments.get(item_key, {})
                if not item_data:
                    continue
                
                # Get enchant boost from item 5 (exact match to working code)
                if item == 5:
                    enchant_boost = item_data.get("Boosts", {}).get("100", 0)
                
                # Get infusions count
                infusions_raw = item_data.get("Infusions", {})
                infusions_count = self.safe_get_infusions_count(infusions_raw)
                
                # Get base boost
                base_boost = item_data.get("Boosts", {}).get("40", 0)
                
                # EXACT formula from your working code:
                # equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                totalEquipmentBoosts += equip_percent
                
            except Exception as e:
                print(f"Error processing equipment item {item} for {player_name}: {e}")
                continue
        
        # EXACT base damage calculation from your working code:
        # base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
        base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
        
        # Debug output to verify the fix
        print(f"DEBUG FIXED: {player_name}")
        print(f"  total_damage_percent (after *100): {total_damage_percent}")
        print(f"  totalEquipmentBoosts: {totalEquipmentBoosts}")
        print(f"  base_damage_percent: {base_damage_percent}")
        
        # Calculate levels
        study_level = self.calculate_study_level(total_exp_boost, codex_exp_boost, enchant_boost)
        nexus_level = self.calculate_nexus_level(base_damage_percent, upgrades)
        
        print(f"  -> Study: {study_level}, Nexus: {nexus_level}")
        
        return {
            "GuildName": guild_name, 
            "NexusLevel": nexus_level, 
            "StudyLevel": study_level
        }
    def calculate_codex_cost(self, start_level: int, progress: int) -> int:
        if progress <= 0: return 0
        return sum(range(start_level + 1, start_level + progress + 1))

    # --- Data Processing ---
   

    def fetch_current_guild_data(self) -> tuple[List[Dict], bool]:
        """Fetch current guild data, returns (data, is_fresh_data)"""
        entries = []
        for page in range(1, 5):  # Top 5 pages
            lb = self.api.get(f"/leaderboards/{LEADERBOARD_TYPE}", {"page": page})
            if lb and lb.get("Entries"): 
                entries.extend(lb["Entries"])
            else: 
                break
        
        if not entries:
            print("No leaderboard data available, using previous data point...")
            prev_data = self._get_previous_data_point("guild")
            if prev_data and prev_data.get("guilds"):
                # Return previous guild data but strip progress info since it's stale
                guild_data = []
                for guild in prev_data["guilds"]:
                    guild_copy = guild.copy()
                    # Remove progress data since it's from a previous run
                    guild_copy.pop("NexusProgress", None)
                    guild_copy.pop("StudyProgress", None)
                    guild_copy.pop("TotalCodexCost", None)
                    guild_data.append(guild_copy)
                return guild_data, False
            return [], False
        
        print(f"Processing {len(entries)} leaderboard entries...")
        processed_guilds, guild_data = set(), []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.process_player, e) for e in entries]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result and result["GuildName"] not in processed_guilds:
                    processed_guilds.add(result["GuildName"])
                    guild_data.append(result)
        
        print(f"Found fresh data for {len(guild_data)} unique guilds.")
        return guild_data, True

    def update_historical_data(self, guilds: List[Dict], market_prices: Dict, timestamp: str):
        try:
            with open(HISTORICAL_FILE, 'r') as f:
                history = json.load(f)
        except (IOError, json.JSONDecodeError):
            history = {"guild_history": {}, "item_prices": {}, "item_categories": ITEM_CATEGORIES}

        # Add item categories if missing
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

    def fetch_market_prices(self) -> tuple[Dict, bool]:
        """Fetch market prices, returns (prices, is_fresh_data)"""
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
        """Main execution method."""
        print(f"--- Starting update at {datetime.now(timezone.utc).isoformat()} ---")
        if not self.guild_lookup: 
            print("No guild lookup available, cannot proceed.")
            return

        timestamp = datetime.now(timezone.utc).isoformat()
        current_guilds, guild_data_fresh = self.fetch_current_guild_data()
        market_prices, market_data_fresh = self.fetch_market_prices()
        
        # Only update historical data if we have fresh data
        if guild_data_fresh or market_data_fresh:
            # Only add guild data to history if it's fresh
            guild_data_for_history = current_guilds if guild_data_fresh else []
            self.update_historical_data(guild_data_for_history, market_prices if market_data_fresh else {}, timestamp)

        # Load/create baseline
        try:
            with open(BASELINE_FILE, 'r') as f: 
                baseline = json.load(f)
        except (IOError, json.JSONDecodeError): 
            baseline = {"date": None, "guilds": {}}

        today_str = timestamp.split('T')[0]
        if baseline.get("date") != today_str:
            print(f"New day detected. Creating new baseline for {today_str}.")
            baseline = {
                "date": today_str, 
                "guilds": {
                    g["GuildName"]: {
                        "NexusLevel": g["NexusLevel"], 
                        "StudyLevel": g["StudyLevel"]
                    } for g in current_guilds
                }
            }
            with open(BASELINE_FILE, 'w') as f: 
                json.dump(baseline, f, indent=2)

        # Calculate progress and codex cost (only for guilds with baseline data)
        total_codex = 0
        for guild in current_guilds:
            base = baseline.get("guilds", {}).get(guild["GuildName"])
            if base:
                guild["NexusProgress"] = max(0, guild["NexusLevel"] - base["NexusLevel"])
                guild["StudyProgress"] = max(0, guild["StudyLevel"] - base["StudyLevel"])
                guild["TotalCodexCost"] = (
                    self.calculate_codex_cost(base["NexusLevel"], guild["NexusProgress"]) +
                    self.calculate_codex_cost(base["StudyLevel"], guild["StudyProgress"])
                )
                total_codex += guild["TotalCodexCost"]
            else:
                guild["NexusProgress"] = guild["StudyProgress"] = guild["TotalCodexCost"] = 0
        
        # Calculate dust spending using average Codex price
        avg_price = self.calculate_average_codex_price()
        
        dust_spending = {
            "total_codex": total_codex,
            "formatted_dust": self.format_currency(total_codex * avg_price),
            "formatted_price": self.format_currency(avg_price)
        }

        # Save final output
        final_data = {
            "guilds": sorted(current_guilds, key=lambda g: g.get("NexusLevel", 0), reverse=True),
            "dustSpending": dust_spending,
            "lastUpdated": timestamp,
            "baselineDate": baseline.get("date"),
            "dataFreshness": {
                "guild_data_fresh": guild_data_fresh,
                "market_data_fresh": market_data_fresh
            }
        }
        
        with open(GUILD_DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)
        
        print(f"--- Update complete. Fresh data: guilds={guild_data_fresh}, market={market_data_fresh} ---")

    def format_currency(self, amount: float) -> str:
        if amount >= 1e12: return f"{amount / 1e12:.2f}T"
        if amount >= 1e9: return f"{amount / 1e9:.2f}B"
        if amount >= 1e6: return f"{amount / 1e6:.2f}M"
        if amount >= 1e3: return f"{amount / 1e3:.2f}K"
        return f"{amount:.2f}"

if __name__ == "__main__":
    tracker = GuildStatsTracker()
    tracker.run_update()