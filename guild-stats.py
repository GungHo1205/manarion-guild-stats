#!/usr/bin/env python3
"""
Guild Stats Collection Script - Pure SQLite Version
No JSON file generation, all data stored in SQLite database
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

        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_name ON guild_snapshots(guild_name);
        CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_prices(timestamp);
        CREATE INDEX IF NOT EXISTS idx_market_item_name ON market_prices(item_name);
        """
        
        self.conn.executescript(schema_sql)
        self.conn.commit()

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
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
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
        
        # Step 1: Load guilds from API
        print("Step 1: Loading guild list from API...")
        guilds_data = self.api.get("/guilds")
        if not guilds_data:
            print("Failed to fetch guild list")
            return [], False
        
        # Update guild lookup
        self.guild_lookup = {g.get("ID", 0): g.get("Name", "Unknown") for g in guilds_data}
        
        # Step 2: Process and sort guilds
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
        
        # Sort by priority and take top guilds
        guild_list.sort(key=lambda g: (-g["TotalUpgrades"], -g["Level"], g["ID"]))
        top_guilds = guild_list[:MAX_GUILDS]
        print(f"Selected top {len(top_guilds)} guilds for processing")
        
        # Step 3: Process guild owners
        print("Step 3: Processing guild owners...")
        guild_data = []
        players_processed = 0
        players_skipped = 0
        
        for i, guild_info in enumerate(top_guilds):
            guild_name = guild_info["Name"]
            owner_id = guild_info["OwnerID"]
            
            print(f"  Processing guild {i+1}/{len(top_guilds)}: {guild_name}")
            
            # Fetch owner's player data
            player_data = self.api.get(f"/players/{owner_id}")
            
            if not player_data:
                print(f"    Failed to fetch owner data for {guild_name}")
                players_skipped += 1
                continue
            
            # Calculate guild levels
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

            # Define boost priority order
            boost_priority = [30, 31, 32, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]

            # Try each boost in priority order until we find a non-zero result
            base_damage_percent = 0
            owner_upgrades = 0

            for boost_id in boost_priority:
                boost_id_str = str(boost_id)
                owner_upgrades = base_boosts.get(boost_id_str, 0)
                total_boost_percent = total_boosts.get(boost_id_str, 0) * 100
                
                # Process equipment for this boost type
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
                
                # If we found a non-zero result, use this boost
                if base_damage_percent > 0:
                    break

            # Keep enchant boost calculation separate for study level
            equipments = player_data.get("Equipment", {})
            enchant_boost = 0
            item_5 = equipments.get("5", {})
            if item_5:
                enchant_boost = item_5.get("Boosts", {}).get("100", 0)
            
            # Calculate levels
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
        """Calculate nexus level using the correct formula."""
        if upgrades <= 0:
            return 0
        
        base_pct = 0.02
        multiplier = research_damage_percent / (upgrades * base_pct)
        level = 100 * (multiplier - 1.0)
        return max(0, round(level))

    def calculate_study_level(self, total_exp: int, codex_exp: int, enchant_exp: int) -> int:
        return max(0, total_exp - codex_exp - enchant_exp)

    def calculate_codex_cost(self, start_level: int, progress: int) -> int:
        """Calculate codex cost for level progression."""
        if progress <= 0: 
            return 0
        return sum(range(start_level + 1, start_level + progress + 1))

    def fetch_market_prices(self) -> tuple[Dict, bool]:
        """Fetch market prices with fallback to database cache."""
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

    def run_update(self):
        """Main execution method using SQLite database."""
        start_time = time.time()
        timestamp = datetime.now(timezone.utc).isoformat()
        today_str = timestamp.split('T')[0]
        
        print(f"Starting SQLite guild tracking at {timestamp}")
        print(f"Target: {MAX_GUILDS} guilds (performance optimized)")
        
        errors = []
        baseline_created = False
        
        try:
            with self.db:
                # Fetch current guild and market data
                current_guilds, guild_data_fresh = self.fetch_guild_data()
                market_prices, market_data_fresh = self.fetch_market_prices()
                
                if not current_guilds:
                    print("No guild data available, using cached data...")
                    errors.append("No fresh guild data available")
                    guild_data_fresh = False
                
                # Handle baseline creation/loading
                if self.db.is_new_day_baseline_needed() and current_guilds:
                    print(f"New day detected. Creating baseline for {today_str}")
                    baseline_created_timestamp = self.db.create_daily_baseline(current_guilds, today_str)
                    baseline_created = True
                    print(f"Baseline created for {len(current_guilds)} guilds")
                
                # Load existing baseline
                baseline = self.db.get_daily_baseline(today_str)
                baseline_date = baseline.get('date')
                
                # Calculate progress against baseline
                total_codex = 0
                guilds_with_progress = 0
                
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
                        total_codex += guild["TotalCodexCost"]
                        
                        if nexus_progress > 0 or study_progress > 0:
                            guilds_with_progress += 1
                    else:
                        guild["NexusProgress"] = guild["StudyProgress"] = guild["TotalCodexCost"] = 0
                
                # Save guild snapshot to database
                if current_guilds:
                    records_saved = self.db.save_guild_snapshot(current_guilds, timestamp, baseline_date, guild_data_fresh)
                    print(f"Saved {records_saved} guild records to database")
                
                # Save market prices to database
                if market_prices and market_data_fresh:
                    prices_saved = self.db.save_market_prices(market_prices, timestamp)
                    print(f"Saved {prices_saved} market price records to database")
                
                # Update guild metadata table
                if current_guilds:
                    self.update_guild_metadata(current_guilds, timestamp)
                
                # Log this processing run
                execution_time = time.time() - start_time
                self.db.conn.execute("""
                    INSERT INTO processing_logs 
                    (timestamp, execution_time_seconds, guilds_processed, guilds_skipped, 
                     api_calls_made, data_freshness, errors, baseline_created)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    timestamp, execution_time, len(current_guilds), 
                    MAX_GUILDS - len(current_guilds), MAX_GUILDS + 1,  # +1 for market API
                    json.dumps({"guild_data_fresh": guild_data_fresh, "market_data_fresh": market_data_fresh}),
                    "; ".join(errors) if errors else None, baseline_created
                ])
                self.db.conn.commit()
                
                # Performance summary
                print(f"\n=== SQLite Update Complete ===")
                print(f"Execution time: {execution_time:.2f} seconds")
                print(f"Guilds tracked: {len(current_guilds)}")
                print(f"Guilds with progress: {guilds_with_progress}")
                print(f"Total Codex used today: {total_codex:,}")
                print(f"Performance: {len(current_guilds)/execution_time:.1f} guilds/second")
                print(f"Fresh data: guilds={guild_data_fresh}, market={market_data_fresh}")
                print(f"Baseline date: {baseline_date}")
                print(f"Baseline created this run: {baseline_created}")
                
                # Show database stats
                stats = self.get_database_stats()
                print(f"Database: {stats.get('database_size_mb', 0)} MB, {stats.get('guild_snapshots_count', 0):,} snapshots")
                
        except Exception as e:
            print(f"Error in run_update: {e}")
            errors.append(str(e))
            raise

    def update_guild_metadata(self, current_guilds: List[Dict], timestamp: str):
        """Update the guild metadata table."""
        guild_records = []
        for guild in current_guilds:
            guild_records.append((
                guild.get('GuildID'),
                guild['GuildName'],
                None,  # owner_id - could be filled later
                timestamp,  # last_seen
                1,     # is_active
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
            tables = ['guild_snapshots', 'daily_baselines', 'market_prices', 'guilds']
            for table in tables:
                cursor = self.db.conn.execute(f"SELECT COUNT(*) as count FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()['count']
            
            if os.path.exists(self.db.db_path):
                stats['database_size_mb'] = round(os.path.getsize(self.db.db_path) / (1024*1024), 2)
            
            return stats

    # === Analytics Features ===
    
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
            # Weekly totals
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
            
            # Top performers
            velocity_report = self.get_progress_velocity_report(hours=168)  # 7 days
            
            # Daily breakdown
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
    print("No JSON files will be generated - all data stored in SQLite database")
    tracker = GuildStatsTracker()
    tracker.run_update()