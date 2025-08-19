#!/usr/bin/env python3
"""
Migration Script: JSON to SQLite
Converts existing JSON data files to SQLite database
"""

import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional

class DatabaseMigrator:
    def __init__(self, db_path: str = "docs/guild-stats.db"):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database and create tables."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.create_tables()
        
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            
    def create_tables(self):
        """Create all database tables and indexes."""
        schema_sql = """
        -- Main guild snapshots table
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

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_name ON guild_snapshots(guild_name);
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_timestamp ON guild_snapshots(guild_name, timestamp);
        CREATE INDEX IF NOT EXISTS idx_baselines_date ON daily_baselines(date);
        CREATE INDEX IF NOT EXISTS idx_baselines_guild_date ON daily_baselines(guild_name, date);
        CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_prices(timestamp);
        CREATE INDEX IF NOT EXISTS idx_market_item_name ON market_prices(item_name);
        CREATE INDEX IF NOT EXISTS idx_market_item_timestamp ON market_prices(item_name, timestamp);

        -- Views
        CREATE VIEW IF NOT EXISTS latest_guild_data AS
        SELECT 
            gs.*,
            g.owner_id,
            g.is_active
        FROM guild_snapshots gs
        JOIN guilds g ON gs.guild_name = g.guild_name
        WHERE gs.timestamp = (
            SELECT MAX(timestamp) 
            FROM guild_snapshots gs2 
            WHERE gs2.guild_name = gs.guild_name
        );
        """
        
        # Execute schema creation
        self.conn.executescript(schema_sql)
        self.conn.commit()
        print("Database schema created successfully")

    def migrate_historical_data(self, historical_file: str = "docs/historical-data.json"):
        """Migrate historical guild and market data from JSON."""
        if not os.path.exists(historical_file):
            print(f"Historical data file {historical_file} not found, skipping...")
            return
            
        print(f"Migrating historical data from {historical_file}...")
        
        with open(historical_file, 'r') as f:
            data = json.load(f)
        
        # Migrate guild history
        guild_history = data.get('guild_history', {})
        guild_records = []
        
        for guild_name, entries in guild_history.items():
            for entry in entries:
                guild_records.append((
                    entry['timestamp'],
                    guild_name,
                    None,  # guild_id - will be filled later
                    0,     # guild_level - will be filled later
                    entry['nexus'],
                    entry['study'],
                    0,     # total_upgrades
                    0,     # nexus_progress
                    0,     # study_progress  
                    0,     # codex_cost
                    None,  # baseline_date
                    1      # data_fresh
                ))
        
        if guild_records:
            self.conn.executemany("""
                INSERT OR REPLACE INTO guild_snapshots 
                (timestamp, guild_name, guild_id, guild_level, nexus_level, study_level, 
                 total_upgrades, nexus_progress, study_progress, codex_cost, baseline_date, data_fresh)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, guild_records)
            print(f"Migrated {len(guild_records)} guild snapshot records")
        
        # Migrate market prices
        item_prices = data.get('item_prices', {})
        price_records = []
        
        for item_name, item_data in item_prices.items():
            prices = item_data.get('prices', [])
            for price in prices:
                price_records.append((
                    price['timestamp'],
                    item_name,
                    None,  # item_id - could be mapped later
                    price['buy'],
                    price['sell']
                ))
        
        if price_records:
            self.conn.executemany("""
                INSERT OR REPLACE INTO market_prices 
                (timestamp, item_name, item_id, buy_price, sell_price)
                VALUES (?, ?, ?, ?, ?)
            """, price_records)
            print(f"Migrated {len(price_records)} market price records")
        
        self.conn.commit()

    def migrate_current_data(self, guild_data_file: str = "docs/guild-data.json"):
        """Migrate current guild data from JSON."""
        if not os.path.exists(guild_data_file):
            print(f"Guild data file {guild_data_file} not found, skipping...")
            return
            
        print(f"Migrating current guild data from {guild_data_file}...")
        
        with open(guild_data_file, 'r') as f:
            data = json.load(f)
        
        guilds = data.get('guilds', [])
        if not guilds:
            print("No guild data found")
            return
            
        # Get the timestamp from the file
        timestamp = data.get('lastUpdated', datetime.now().isoformat())
        baseline_date = data.get('baselineDate')
        
        # Insert guild records
        guild_records = []
        guild_metadata = []
        
        for guild in guilds:
            # Guild snapshot
            guild_records.append((
                timestamp,
                guild['GuildName'],
                guild.get('GuildID'),
                guild.get('GuildLevel', 0),
                guild['NexusLevel'],
                guild['StudyLevel'],
                guild.get('TotalUpgrades', 0),
                guild.get('NexusProgress', 0),
                guild.get('StudyProgress', 0),
                guild.get('TotalCodexCost', 0),
                baseline_date,
                1  # data_fresh
            ))
            
            # Guild metadata
            guild_metadata.append((
                guild.get('GuildID'),
                guild['GuildName'],
                None,  # owner_id - could be filled later
                timestamp,  # last_seen
                1,     # is_active
                guild.get('TotalUpgrades', 0),
                guild.get('GuildLevel', 0)
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO guild_snapshots 
            (timestamp, guild_name, guild_id, guild_level, nexus_level, study_level,
             total_upgrades, nexus_progress, study_progress, codex_cost, baseline_date, data_fresh)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, guild_records)
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO guilds 
            (guild_id, guild_name, owner_id, last_seen, is_active, total_upgrades, guild_level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, guild_metadata)
        
        print(f"Migrated {len(guild_records)} current guild records")
        self.conn.commit()

    def migrate_baselines(self, baseline_file: str = "docs/daily-baseline.json"):
        """Migrate daily baseline data from JSON."""
        if not os.path.exists(baseline_file):
            print(f"Baseline file {baseline_file} not found, skipping...")
            return
            
        print(f"Migrating baseline data from {baseline_file}...")
        
        with open(baseline_file, 'r') as f:
            data = json.load(f)
        
        date = data.get('date')
        created_at = data.get('created_at', data.get('date'))
        guilds = data.get('guilds', {})
        
        if not date or not guilds:
            print("No baseline data found")
            return
        
        baseline_records = []
        for guild_name, levels in guilds.items():
            baseline_records.append((
                date,
                guild_name,
                levels['NexusLevel'],
                levels['StudyLevel'],
                created_at
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO daily_baselines 
            (date, guild_name, nexus_level, study_level, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, baseline_records)
        
        print(f"Migrated {len(baseline_records)} baseline records")
        self.conn.commit()

    def verify_migration(self):
        """Verify migration was successful by showing data counts."""
        print("\n=== Migration Verification ===")
        
        tables = [
            ("guild_snapshots", "Guild snapshots"),
            ("daily_baselines", "Daily baselines"), 
            ("market_prices", "Market prices"),
            ("guilds", "Guild metadata"),
            ("processing_logs", "Processing logs")
        ]
        
        for table, description in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{description}: {count:,} records")
        
        # Show latest data
        print("\n=== Latest Data Sample ===")
        
        # Latest guild snapshots
        latest_guilds = self.conn.execute("""
            SELECT guild_name, nexus_level, study_level, timestamp
            FROM guild_snapshots 
            WHERE timestamp = (SELECT MAX(timestamp) FROM guild_snapshots)
            ORDER BY nexus_level DESC
            LIMIT 5
        """).fetchall()
        
        if latest_guilds:
            print("Top 5 guilds (latest snapshot):")
            for guild_name, nexus, study, timestamp in latest_guilds:
                print(f"  {guild_name}: Nexus {nexus}, Study {study}")
        
        # Date range
        date_range = self.conn.execute("""
            SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest 
            FROM guild_snapshots
        """).fetchone()
        
        if date_range[0]:
            print(f"Data range: {date_range[0]} to {date_range[1]}")

    def create_backup_json(self):
        """Create a backup of existing JSON files before migration."""
        backup_dir = "docs/json_backup"
        os.makedirs(backup_dir, exist_ok=True)
        
        files_to_backup = [
            "docs/guild-data.json",
            "docs/historical-data.json", 
            "docs/daily-baseline.json"
        ]
        
        for file_path in files_to_backup:
            if os.path.exists(file_path):
                backup_path = os.path.join(backup_dir, os.path.basename(file_path))
                os.rename(file_path, backup_path)
                print(f"Backed up {file_path} to {backup_path}")

def main():
    """Run the complete migration process."""
    print("=== Starting JSON to SQLite Migration ===")
    
    migrator = DatabaseMigrator()
    
    try:
        # Create backup of existing JSON files
        print("\n1. Creating backup of existing JSON files...")
        migrator.create_backup_json()
        
        # Connect and create schema
        print("\n2. Connecting to database and creating schema...")
        migrator.connect()
        
        # Migrate data
        print("\n3. Migrating historical data...")
        migrator.migrate_historical_data("docs/json_backup/historical-data.json")
        
        print("\n4. Migrating current guild data...")
        migrator.migrate_current_data("docs/json_backup/guild-data.json")
        
        print("\n5. Migrating baseline data...")
        migrator.migrate_baselines("docs/json_backup/daily-baseline.json")
        
        # Verify migration
        print("\n6. Verifying migration...")
        migrator.verify_migration()
        
        print("\n=== Migration completed successfully! ===")
        print(f"Database created at: {migrator.db_path}")
        print("JSON backups saved in: docs/json_backup/")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        raise
    finally:
        migrator.disconnect()

if __name__ == "__main__":
    main()