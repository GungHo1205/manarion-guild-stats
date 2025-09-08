#!/usr/bin/env python3
"""
Guild Stats Collection Script - Pure SQLite Version
No JSON file generation, all data stored in SQLite database

ADDITIONS:
- player_dust_income table to store daily mana dust income for top-100 leaderboard players.
- fetch_leaderboard_and_store_daily_dust: runs once per day and stores per-player/day daily_income.
- Dust calculation uses the same pre-diminish formula:
    final = baseDrop(x) * (1 + baseBoost/100) * (1 + totalBoost/100)
  where x = enemy + 150 (with >150k adjustment matching existing logic).
"""

import json
import os
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import concurrent.futures
from collections import defaultdict
import sqlite3
import sys
import math

class GuildStatsDatabase:
    def __init__(self, db_path: str = "docs/guild-stats.db"):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self._create_tables_if_not_exist()
        return self.conn
        
    def disconnect(self):
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _create_tables_if_not_exist(self):
        """Create tables if they don't exist."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS guild_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            guild_name TEXT NOT NULL,
            guild_id INTEGER,
            guild_level INTEGER DEFAULT 0,
            nexus_level INTEGER NOT NULL,
            study_level INTEGER NOT NULL,
            total_upgrades INTEGER DEFAULT 0,
            nexus_progress INTEGER DEFAULT 0,
            study_progress INTEGER DEFAULT 0,
            codex_cost INTEGER DEFAULT 0,
            baseline_date TEXT,
            data_fresh BOOLEAN DEFAULT 1,
            UNIQUE(timestamp, guild_name)
        );

        CREATE TABLE IF NOT EXISTS daily_baselines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            guild_name TEXT NOT NULL,
            nexus_level INTEGER NOT NULL,
            study_level INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(date, guild_name)
        );

        CREATE TABLE IF NOT EXISTS market_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            item_name TEXT NOT NULL,
            item_id INTEGER,
            buy_price INTEGER NOT NULL,
            sell_price INTEGER NOT NULL,
            average_price INTEGER GENERATED ALWAYS AS ((buy_price + sell_price) / 2) STORED,
            UNIQUE(timestamp, item_name)
        );

        CREATE TABLE IF NOT EXISTS guilds (
            guild_id INTEGER PRIMARY KEY,
            guild_name TEXT NOT NULL UNIQUE,
            owner_id INTEGER,
            last_seen TEXT,
            is_active BOOLEAN DEFAULT 1,
            total_upgrades INTEGER DEFAULT 0,
            guild_level INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS processing_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            execution_time_seconds REAL,
            guilds_processed INTEGER,
            guilds_skipped INTEGER,
            api_calls_made INTEGER,
            data_freshness TEXT,
            errors TEXT,
            baseline_created BOOLEAN DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS player_dust_income (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            player_name TEXT NOT NULL,
            leaderboard_rank INTEGER,
            daily_income REAL NOT NULL,
            UNIQUE(date, player_name)
        );

        CREATE INDEX IF NOT EXISTS idx_player_dust_income_date ON player_dust_income(date);
        CREATE INDEX IF NOT EXISTS idx_player_dust_income_player ON player_dust_income(player_name);
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_name ON guild_snapshots(guild_name);
        CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_prices(timestamp);
        CREATE INDEX IF NOT EXISTS idx_market_item_name ON market_prices(item_name);
        """
        
        self.conn.executescript(schema_sql)
        self.conn.commit()

    def calculate_mana_dust_income(self, player_data: Dict) -> float:
        """
        Replicates the frontend's mana dust calculation logic.
        Uses boost IDs:
        - 101: Base mana dust boost (from total boosts as it includes sigils)
        - 121: Total mana dust multiplier
        """
        try:
            enemy_level = player_data.get("Enemy", 0)
            if not enemy_level:
                return 0.0

            total_boosts = player_data.get("TotalBoosts", {})
            
            # Base multiplier from sigils, etc.
            # ID 101 seems to be for base boosts, but using the total boost value is more accurate
            base_mana_dust_raw = total_boosts.get('101', 0)
            
            # Total mana dust multiplier
            mana_dust_raw = total_boosts.get('121', 0)

            base_factor = 1 + (base_mana_dust_raw / 100) if isinstance(base_mana_dust_raw, (int, float)) else 1
            total_factor = 1 + (mana_dust_raw / 100) if isinstance(mana_dust_raw, (int, float)) else 1

            # Base drop formula from frontend logic
            enemy_base_mana_drop = 0
            if enemy_level > 150000:
                base_at_150k = 0.0001 * (150150**2) + (150150**1.2) + (10 * 150150)
                multiplier = (1.01 ** ((enemy_level - 150000) / 2000))
                enemy_base_mana_drop = multiplier * base_at_150k
            else:
                x = enemy_level + 150
                enemy_base_mana_drop = 0.0001 * (x**2) + (x**1.2) + (10 * x)
            
            mana_drop_after_base_boost = enemy_base_mana_drop * base_factor
            final_drop_per_kill = mana_drop_after_base_boost * total_factor

            return final_drop_per_kill
        except (TypeError, KeyError, AttributeError) as e:
            print(f"Error calculating dust for player {player_data.get('Name', 'N/A')}: {e}")
            return 0.0

    def save_guild_snapshot(self, guilds: List[Dict], timestamp: str, baseline_date: str, data_fresh: bool = True) -> int:
        records = []
        for guild in guilds:
            records.append((
                timestamp, guild['GuildName'], guild.get('GuildID'),
                guild.get('GuildLevel', 0), guild['NexusLevel'], guild['StudyLevel'], 
                guild.get('TotalUpgrades', 0), guild.get('NexusProgress', 0),
                guild.get('StudyProgress', 0), guild.get('TotalCodexCost', 0),
                baseline_date, data_fresh
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO guild_snapshots 
            (timestamp, guild_name, guild_id, guild_level, nexus_level, study_level,
             total_upgrades, nexus_progress, study_progress, codex_cost, baseline_date, data_fresh)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        self.conn.commit()
        return len(records)

    def get_daily_baseline(self, date: str = None) -> Dict:
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
        cursor = self.conn.execute("""
            SELECT guild_name, nexus_level, study_level, created_at
            FROM daily_baselines WHERE date = ?
        """, [date])
        
        guilds = {}
        created_at = None
        for row in cursor:
            guilds[row['guild_name']] = {
                'NexusLevel': row['nexus_level'],
                'StudyLevel': row['study_level']
            }
            if not created_at:
                created_at = row['created_at']
        
        return {'date': date, 'created_at': created_at, 'guilds': guilds}

    def create_daily_baseline(self, guilds: List[Dict], date: str = None) -> str:
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        timestamp = datetime.now(timezone.utc).isoformat()
        records = [(date, g['GuildName'], g['NexusLevel'], g['StudyLevel'], timestamp) for g in guilds]
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO daily_baselines 
            (date, guild_name, nexus_level, study_level, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        self.conn.commit()
        return timestamp

    def is_new_day_baseline_needed(self) -> bool:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cursor = self.conn.execute("SELECT COUNT(*) as count FROM daily_baselines WHERE date = ?", [today])
        return cursor.fetchone()['count'] == 0

    def save_market_prices(self, prices: Dict, timestamp: str) -> int:
        records = [(timestamp, item_name, None, price_data['buy'], price_data['sell']) 
                  for item_name, price_data in prices.items()]
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO market_prices 
            (timestamp, item_name, item_id, buy_price, sell_price)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        self.conn.commit()
        return len(records)

    def save_player_dust_income(self, date: str, records: List[Dict]) -> int:
        """
        Save (or replace) player dust income rows for a given date.
        records: list of dicts {player_name, rank, daily_income}
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        rows = []
        for r in records:
            rows.append((date, timestamp, r['player_name'], r.get('rank'), r['daily_income']))
        self.conn.executemany("""
            INSERT OR REPLACE INTO player_dust_income
            (date, timestamp, player_name, leaderboard_rank, daily_income)
            VALUES (?, ?, ?, ?, ?)
        """, rows)
        self.conn.commit()
        return len(rows)

    def calculate_average_codex_price(self, hours: int = 24) -> float:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor = self.conn.execute("""
            SELECT average_price FROM market_prices 
            WHERE item_name = 'Codex' AND timestamp >= ?
            ORDER BY timestamp DESC
        """, [cutoff])
        
        prices = [row['average_price'] for row in cursor.fetchall()]
        return sum(prices) / len(prices) if prices else 10000000000

    def format_currency(self, amount: float) -> str:
        if amount >= 1e12: return f"{amount / 1e12:.2f}T"
        if amount >= 1e9: return f"{amount / 1e9:.2f}B"
        if amount >= 1e6: return f"{amount / 1e6:.2f}M"
        if amount >= 1e3: return f"{amount / 1e3:.2f}K"
        return f"{amount:.2f}"

# --- Configuration (Updated) ---
API_BASE_URL = "https://api.manarion.com"
MAX_WORKERS = 2
API_DELAY = 2
BASE_PER_UPGRADE = 0.02
DATA_DIR = "docs"
MAX_GUILDS = 30

# Item mapping (unchanged)
ITEM_MAPPING = {
    1: "Mana Dust", 7: "Fish", 8: "Wood", 9: "Iron",
    2: "Elemental Shards", 3: "Codex",
    4: "Fire Essence", 5: "Water Essence", 6: "Nature Essence",
    10: "Asbestos", 11: "Ironbark", 12: "Fish Scales",
    13: "Tome of Fire", 14: "Tome of Water", 15: "Tome of Nature", 16: "Tome of Mana Shield",
    17: "Formula: Fire Resistance", 18: "Formula: Water Resistance", 19: "Formula: Nature Resistance",
    20: "Formula: Inferno", 21: "Formula: Tidal Wrath", 22: "Formula: Wildheart",
    23: "Formula: Insight", 24: "Formula: Bountiful Harvest", 25: "Formula: Prosperity",
    26: "Formula: Fortune", 27: "Formula: Growth", 28: "Formula: Vitality",
    29: "Elderwood", 30: "Lodestone", 31: "White Pearl",
    32: "Four-Leaf Clover", 33: "Enchanted Droplet", 34: "Infernal Heart",
    35: "Orb of Power", 36: "Orb of Chaos", 37: "Orb of Divinity", 50: "Orb of Perfection", 45: "Orb of Legacy",
    46: "Elementium", 47: "Divine Essence",
    39: "Sunpetal", 40: "Sageroot", 41: "Bloomwell",
    44: "Crystallized Mana"
}

UNTRADEABLE_IDS = {38, 42, 43, 48, 49}

class APIClient:
    """API client for Manarion API calls."""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'GuildStatsTracker/4.0-SQLite-Pure'})

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
                    time.sleep(5 * (attempt + 1))
                else:
                    return None

class GuildStatsTracker:
    def __init__(self, db_path: str = "docs/guild-stats.db"):
        self.api = APIClient(API_BASE_URL)
        self.db = GuildStatsDatabase(db_path)
        os.makedirs(DATA_DIR, exist_ok=True)
        self.guild_lookup = {}

    def fetch_guild_data(self) -> tuple[List[Dict], bool]:
        """Fetch guild data using the direct guild approach with SQLite caching."""
        print("Fetching guild data using direct guild approach...")
        
        print("Step 1: Loading guild list from API...")
        guilds_data = self.api.get("/guilds")
        if not guilds_data:
            print("Failed to fetch guild list")
            return [], False
        
        self.guild_lookup = {g.get("ID", 0): g.get("Name", "Unknown") for g in guilds_data}
        
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
        
        guild_list.sort(key=lambda g: (-g["TotalUpgrades"], -g["Level"], g["ID"]))
        top_guilds = guild_list[:MAX_GUILDS]
        print(f"Selected top {len(top_guilds)} guilds for processing")
        
        print("Step 3: Processing guild owners...")
        guild_data = []
        players_processed = 0
        players_skipped = 0
        
        for i, guild_info in enumerate(top_guilds):
            guild_name = guild_info["Name"]
            owner_id = guild_info["OwnerID"]
            
            print(f"  Processing guild {i+1}/{len(top_guilds)}: {guild_name}")
            
            player_data = self.api.get(f"/players/{owner_id}")
            
            if not player_data:
                print(f"    Failed to fetch owner data for {guild_name}")
                players_skipped += 1
                continue
            
            result = self.process_guild_owner_data(guild_name, player_data, guild_info["TotalUpgrades"])
            
            if result:
                result["GuildLevel"] = guild_info["Level"]
                result["GuildID"] = guild_info["ID"]
                guild_data.append(result)
                players_processed += 1
                print(f"    {guild_name} -> Nexus: L{result['NexusLevel']}, Study: L{result['StudyLevel']}")
            else:
                players_skipped += 1
        
        print(f"\nProcessing Summary:")
        print(f"   Guild owners processed: {players_processed}")
        print(f"   Guild owners skipped: {players_skipped}")
        print(f"   Total guilds with data: {len(guild_data)}")
        
        return guild_data, True

    def process_guild_owner_data(self, guild_name: str, player_data: Dict, total_upgrades: int) -> Optional[Dict]:
        """Process guild owner data to calculate guild levels."""
        try:
            base_boosts = player_data.get("BaseBoosts", {})
            total_boosts = player_data.get("TotalBoosts", {})

            codex_exp_boost = base_boosts.get("100", 0)
            total_exp_boost = total_boosts.get("100", 0)

            boost_priority = [30, 31, 32, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]

            base_damage_percent = 0
            owner_upgrades = 0

            for boost_id in boost_priority:
                boost_id_str = str(boost_id)
                owner_upgrades = base_boosts.get(boost_id_str, 0)
                total_boost_percent = total_boosts.get(boost_id_str, 0) * 100
                
                equipments = player_data.get("Equipment", {})
                totalEquipmentBoosts = 0
                
                for item in range(1, 9):
                    try:
                        item_key = str(item)
                        equipment_item = equipments.get(item_key, {})
                        
                        infusions = equipment_item.get("Infusions", {})
                        if isinstance(infusions, dict):
                            infusions_count = sum(v for v in infusions.values() if isinstance(v, (int, float)))
                        else:
                            infusions_count = infusions if isinstance(infusions, (int, float)) else 0
                        
                        base_boost = equipment_item.get("Boosts", {}).get(boost_id_str, 0)
                        equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                        totalEquipmentBoosts += equip_percent
                        
                    except Exception as e:
                        print(f"      Error processing equipment item {item}: {e}")
                        continue
                
                base_damage_percent = total_boost_percent - totalEquipmentBoosts - 100
                
                if base_damage_percent > 0:
                    break

            equipments = player_data.get("Equipment", {})
            enchant_boost = 0
            item_5 = equipments.get("5", {})
            if item_5:
                enchant_boost = item_5.get("Boosts", {}).get("100", 0)
            
            study_level = self.calculate_study_level(total_exp_boost, codex_exp_boost, enchant_boost)
            nexus_level = self.calculate_nexus_level(base_damage_percent, owner_upgrades)
            
            return {
                "GuildName": guild_name, 
                "NexusLevel": nexus_level, 
                "StudyLevel": study_level,
                "TotalUpgrades": total_upgrades
            }
            
        except Exception as e:
            print(f"      Error processing guild owner data for {guild_name}: {e}")
            return None

    def calculate_nexus_level(self, research_damage_percent, upgrades):
        if upgrades <= 0:
            return 0
        
        base_pct = 0.02
        multiplier = research_damage_percent / (upgrades * base_pct)
        level = 100 * (multiplier - 1.0)
        return max(0, round(level))

    def calculate_study_level(self, total_exp: int, codex_exp: int, enchant_exp: int) -> int:
        return max(0, total_exp - codex_exp - enchant_exp)

    def calculate_codex_cost(self, start_level: int, progress: int) -> int:
        if progress <= 0: 
            return 0
        return sum(range(start_level + 1, start_level + progress + 1))

    def fetch_market_prices(self) -> tuple[Dict, bool]:
        print("Fetching market prices...")
        market_data = self.api.get("/market")
        
        if not market_data:
            print("Market API failed, using database cache...")
            with self.db:
                return self.db.calculate_average_codex_price(), False

        prices = {}
        buy_data, sell_data = market_data.get("Buy", {}), market_data.get("Sell", {})
        for item_id, item_name in ITEM_MAPPING.items():
            if item_id in UNTRADEABLE_IDS: 
                continue
            buy_price, sell_price = buy_data.get(str(item_id)), sell_data.get(str(item_id))
            if buy_price and sell_price:
                prices[item_name] = {"buy": buy_price, "sell": sell_price}
        
        print(f"Fetched fresh prices for {len(prices)} items.")
        return prices, True

    def fetch_leaderboard_top100(self, page: int = 1, lb_type: str = "battle") -> Optional[List[Dict]]:
        endpoint = f"/leaderboards/{lb_type}?page={page}"
        data = self.api.get(endpoint)
        if not data:
            print(f"Failed to fetch leaderboard: {lb_type}")
            return None
        return data.get("Entries", [])

    def compute_daily_dust_for_player(self, player_name: str) -> Optional[float]:
        """
        Fetch player data and compute potential daily mana dust income (pre-diminish).
        Returns daily income (mana dust per 24 hours) or None on error.
        """
        try:
            p_data = self.api.get(f"/players/{player_name}")
            if not p_data:
                print(f"  - Failed to fetch player data for {player_name}")
                return None

            per_kill_income = self.db.calculate_mana_dust_income(p_data)
            
            # Assuming 1 kill every 3 seconds
            kills_per_day = (3600 / 3) * 24
            return per_kill_income * kills_per_day
        except Exception as e:
            print(f"  - Error computing dust for {player_name}: {e}")
            return None

    def fetch_leaderboard_and_store_daily_dust(self, force: bool = False):
        """
        Fetches top 100 battle leaderboard, calculates daily dust income, and stores it.
        This function is designed to run only once per UTC day unless forced.
        """
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        cursor = self.db.conn.execute("SELECT COUNT(*) as count FROM player_dust_income WHERE date = ?", [today_str])
        if cursor.fetchone()['count'] > 0 and not force:
            print(f"Daily dust income for {today_str} already exists. Skipping.")
            return

        print("Fetching top 100 'battle' leaderboard for daily dust calculation...")
        leaderboard_entries = self.fetch_leaderboard_top100(lb_type="battle")
        if not leaderboard_entries:
            print("Could not fetch leaderboard. Aborting dust income job.")
            return

        players_to_process = [entry for entry in leaderboard_entries if not entry.get("Banned")]
        print(f"Found {len(players_to_process)} non-banned players to process.")
        
        income_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_player = {
                executor.submit(self.compute_daily_dust_for_player, p['Name']): p 
                for p in players_to_process
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_player)):
                player_entry = future_to_player[future]
                player_name = player_entry['Name']
                print(f"  ({i+1}/{len(players_to_process)}) Processing {player_name}...")
                try:
                    daily_income = future.result()
                    if daily_income is not None:
                        income_results.append({
                            "player_name": player_name,
                            "rank": player_entry['Rank'],
                            "daily_income": daily_income,
                        })
                except Exception as exc:
                    print(f"  - Generated an exception for {player_name}: {exc}")
        
        if income_results:
            self.db.save_player_dust_income(today_str, income_results)
            print(f"Successfully saved daily dust income for {len(income_results)} players.")

    def run_update(self):
        """Main execution method using SQLite database."""
        start_time = time.time()
        timestamp = datetime.now(timezone.utc).isoformat()
        today_str = timestamp.split('T')[0]
        
        print(f"Starting SQLite guild tracking at {timestamp}")
        
        errors = []
        baseline_created = False

        self.db.connect()
        try:
            current_guilds, guild_data_fresh = self.fetch_guild_data()
            market_prices, market_data_fresh = self.fetch_market_prices()
            
            if not current_guilds:
                errors.append("No fresh guild data available")
            
            if self.db.is_new_day_baseline_needed() and current_guilds:
                print(f"New day detected. Creating baseline for {today_str}")
                self.db.create_daily_baseline(current_guilds, today_str)
                baseline_created = True
                print(f"Baseline created for {len(current_guilds)} guilds")
            
            baseline = self.db.get_daily_baseline(today_str)
            baseline_date = baseline.get('date')
            
            for guild in current_guilds:
                base = baseline.get("guilds", {}).get(guild["GuildName"])
                if base:
                    nexus_progress = max(0, guild["NexusLevel"] - base["NexusLevel"])
                    study_progress = max(0, guild["StudyLevel"] - base["StudyLevel"])
                    
                    guild["NexusProgress"] = nexus_progress
                    guild["StudyProgress"] = study_progress
                    guild["TotalCodexCost"] = (
                        self.calculate_codex_cost(base["NexusLevel"], nexus_progress) +
                        self.calculate_codex_cost(base["StudyLevel"], study_progress)
                    )
                else:
                    guild["NexusProgress"] = guild["StudyProgress"] = guild["TotalCodexCost"] = 0
            
            if current_guilds:
                self.db.save_guild_snapshot(current_guilds, timestamp, baseline_date, guild_data_fresh)
            
            if market_prices and market_data_fresh:
                self.db.save_market_prices(market_prices, timestamp)
            
            if current_guilds:
                self.update_guild_metadata(current_guilds, timestamp)

            # --- Trigger Daily Player Dust Income Fetch ---
            self.fetch_leaderboard_and_store_daily_dust()

            execution_time = time.time() - start_time
            self.db.conn.execute("""
                INSERT INTO processing_logs 
                (timestamp, execution_time_seconds, guilds_processed, api_calls_made, data_freshness, errors, baseline_created)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                timestamp, execution_time, len(current_guilds), 
                MAX_GUILDS + 1,
                json.dumps({"guild_data_fresh": guild_data_fresh, "market_data_fresh": market_data_fresh}),
                "; ".join(errors) if errors else None, 
                baseline_created
            ])
            self.db.conn.commit()
            
            print(f"\n=== SQLite Update Complete in {execution_time:.2f}s ===")
        
        except Exception as e:
            print(f"FATAL ERROR in run_update: {e}")
            raise
        finally:
            self.db.disconnect()

    def update_guild_metadata(self, current_guilds: List[Dict], timestamp: str):
        """Update the guild metadata table."""
        guild_records = []
        for guild in current_guilds:
            guild_records.append((
                guild.get('GuildID'),
                guild['GuildName'],
                None,
                timestamp,
                1,
                guild.get('TotalUpgrades', 0),
                guild.get('GuildLevel', 0)
            ))
        
        self.db.conn.executemany("""
            INSERT OR REPLACE INTO guilds 
            (guild_id, guild_name, owner_id, last_seen, is_active, total_upgrades, guild_level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, guild_records)
        self.db.conn.commit()

    def get_database_stats(self) -> Dict:
        """Get database statistics."""
        with self.db:
            stats = {}
            tables = ['guild_snapshots', 'daily_baselines', 'market_prices', 'guilds', 'player_dust_income']
            for table in tables:
                cursor = self.db.conn.execute(f"SELECT COUNT(*) as count FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()['count']
            
            if os.path.exists(self.db.db_path):
                stats['database_size_mb'] = round(os.path.getsize(self.db.db_path) / (1024*1024), 2)
            
            return stats
    
    def get_progress_velocity_report(self, hours: int = 72) -> Dict:
        """Generate progress velocity report for all guilds."""
        print(f"Generating progress velocity report for last {hours} hours...")
        
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        with self.db:
            cursor = self.db.conn.execute("""
                WITH guild_velocity AS (
                    SELECT 
                        guild_name,
                        MIN(nexus_level) as start_nexus,
                        MAX(nexus_level) as end_nexus,
                        MIN(study_level) as start_study,
                        MAX(study_level) as end_study,
                        COUNT(*) as data_points,
                        (julianday(MAX(timestamp)) - julianday(MIN(timestamp))) * 24 as hours_tracked
                    FROM guild_snapshots
                    WHERE timestamp >= ?
                    GROUP BY guild_name
                    HAVING data_points >= 2 AND hours_tracked > 0
                )
                SELECT 
                    guild_name,
                    (end_nexus - start_nexus) as nexus_growth,
                    (end_study - start_study) as study_growth,
                    ROUND((end_nexus - start_nexus) / hours_tracked, 4) as nexus_velocity,
                    ROUND((end_study - start_study) / hours_tracked, 4) as study_velocity,
                    ROUND(((end_nexus - start_nexus) + (end_study - start_study)) / hours_tracked, 4) as total_velocity,
                    data_points,
                    ROUND(hours_tracked, 1) as hours_tracked
                FROM guild_velocity
                ORDER BY total_velocity DESC
                LIMIT 20
            """, [cutoff])
            
            return [dict(row) for row in cursor.fetchall()]

    def generate_weekly_report(self) -> Dict:
        """Generate comprehensive weekly progress report."""
        print("Generating weekly progress report...")
        
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        
        with self.db:
            cursor = self.db.conn.execute("""
                SELECT 
                    COUNT(DISTINCT guild_name) as guilds_tracked,
                    SUM(nexus_progress) as total_nexus_progress,
                    SUM(study_progress) as total_study_progress,
                    SUM(codex_cost) as total_codex_used,
                    AVG(nexus_level) as avg_nexus_level,
                    AVG(study_level) as avg_study_level
                FROM guild_snapshots
                WHERE timestamp >= ?
            """, [week_ago])
            
            weekly_totals = dict(cursor.fetchone())
            
            velocity_report = self.get_progress_velocity_report(hours=168)
            
            cursor = self.db.conn.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(DISTINCT guild_name) as guilds,
                    SUM(nexus_progress) as nexus_progress,
                    SUM(study_progress) as study_progress,
                    SUM(codex_cost) as codex_used
                FROM guild_snapshots
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """, [week_ago])
            
            daily_breakdown = [dict(row) for row in cursor.fetchall()]
            
            return {
                "report_period": "7 days",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "weekly_totals": weekly_totals,
                "top_velocity_guilds": velocity_report[:10],
                "daily_breakdown": daily_breakdown
            }

if __name__ == "__main__":
    print("=== Guild Stats Tracker - Pure SQLite Version ===")
    tracker = GuildStatsTracker()
    tracker.run_update()
