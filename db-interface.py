#!/usr/bin/env python3
"""
Database Interface for Guild Stats
Replaces JSON file operations with efficient SQLite queries
"""

import sqlite3
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import os

class GuildStatsDatabase:
    def __init__(self, db_path: str = "docs/guild-stats.db"):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to database with optimized settings."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dict-like access
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
        self.conn.execute("PRAGMA synchronous = NORMAL")  # Better performance
        return self.conn
        
    def disconnect(self):
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    # === Guild Data Operations ===
    
    def save_guild_snapshot(self, guilds: List[Dict], timestamp: str, baseline_date: str, 
                          data_fresh: bool = True) -> int:
        """Save a complete guild snapshot with progress tracking."""
        records = []
        for guild in guilds:
            records.append((
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
                data_fresh
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO guild_snapshots 
            (timestamp, guild_name, guild_id, guild_level, nexus_level, study_level,
             total_upgrades, nexus_progress, study_progress, codex_cost, baseline_date, data_fresh)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        
        self.conn.commit()
        return len(records)

    def get_latest_guild_data(self) -> List[Dict]:
        """Get the most recent guild data for all guilds."""
        cursor = self.conn.execute("""
            SELECT 
                guild_name, guild_id, guild_level, nexus_level, study_level,
                total_upgrades, nexus_progress, study_progress, codex_cost,
                baseline_date, timestamp, data_fresh
            FROM latest_guild_data
            ORDER BY nexus_level DESC, study_level DESC, total_upgrades DESC
        """)
        
        return [dict(row) for row in cursor.fetchall()]

    def get_guild_history(self, guild_names: List[str] = None, hours: int = 24) -> Dict:
        """Get guild level history for charting."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        query = """
            SELECT guild_name, timestamp, nexus_level, study_level
            FROM guild_snapshots 
            WHERE timestamp >= ?
        """
        params = [cutoff]
        
        if guild_names:
            placeholders = ','.join(['?' for _ in guild_names])
            query += f" AND guild_name IN ({placeholders})"
            params.extend(guild_names)
            
        query += " ORDER BY guild_name, timestamp"
        
        cursor = self.conn.execute(query, params)
        
        # Group by guild name
        history = {}
        for row in cursor:
            guild_name = row['guild_name']
            if guild_name not in history:
                history[guild_name] = []
            history[guild_name].append({
                'timestamp': row['timestamp'],
                'nexus': row['nexus_level'],
                'study': row['study_level']
            })
        
        return history

    # === Baseline Operations ===
    
    def get_daily_baseline(self, date: str = None) -> Dict:
        """Get daily baseline for a specific date (defaults to today)."""
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
        cursor = self.conn.execute("""
            SELECT guild_name, nexus_level, study_level, created_at
            FROM daily_baselines
            WHERE date = ?
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
        
        return {
            'date': date,
            'created_at': created_at,
            'guilds': guilds
        }

    def create_daily_baseline(self, guilds: List[Dict], date: str = None) -> str:
        """Create a new daily baseline from current guild levels."""
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        records = []
        for guild in guilds:
            records.append((
                date,
                guild['GuildName'],
                guild['NexusLevel'],
                guild['StudyLevel'],
                timestamp
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO daily_baselines 
            (date, guild_name, nexus_level, study_level, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        
        self.conn.commit()
        print(f"Created baseline for {len(records)} guilds on {date}")
        return timestamp

    def is_new_day_baseline_needed(self) -> bool:
        """Check if we need to create a new daily baseline."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        cursor = self.conn.execute("""
            SELECT COUNT(*) as count FROM daily_baselines WHERE date = ?
        """, [today])
        
        return cursor.fetchone()['count'] == 0

    # === Market Price Operations ===
    
    def save_market_prices(self, prices: Dict, timestamp: str) -> int:
        """Save market prices for all items."""
        records = []
        for item_name, price_data in prices.items():
            records.append((
                timestamp,
                item_name,
                None,  # item_id - could be mapped later
                price_data['buy'],
                price_data['sell']
            ))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO market_prices 
            (timestamp, item_name, item_id, buy_price, sell_price)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        
        self.conn.commit()
        return len(records)

    def get_market_history(self, item_names: List[str] = None, hours: int = 24) -> Dict:
        """Get market price history for charting."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        query = """
            SELECT item_name, timestamp, buy_price, sell_price
            FROM market_prices 
            WHERE timestamp >= ?
        """
        params = [cutoff]
        
        if item_names:
            placeholders = ','.join(['?' for _ in item_names])
            query += f" AND item_name IN ({placeholders})"
            params.extend(item_names)
            
        query += " ORDER BY item_name, timestamp"
        
        cursor = self.conn.execute(query, params)
        
        # Group by item name
        history = {}
        for row in cursor:
            item_name = row['item_name']
            if item_name not in history:
                history[item_name] = {'prices': []}
            history[item_name]['prices'].append({
                'timestamp': row['timestamp'],
                'buy': row['buy_price'],
                'sell': row['sell_price']
            })
        
        return history

    def get_latest_market_prices(self) -> Dict:
        """Get the most recent market prices."""
        cursor = self.conn.execute("""
            SELECT item_name, buy_price, sell_price
            FROM market_prices 
            WHERE timestamp = (SELECT MAX(timestamp) FROM market_prices)
        """)
        
        prices = {}
        for row in cursor:
            prices[row['item_name']] = {
                'buy': row['buy_price'],
                'sell': row['sell_price']
            }
        
        return prices

    def calculate_average_codex_price(self, hours: int = 24) -> float:
        """Calculate average Codex price from recent data."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        cursor = self.conn.execute("""
            SELECT average_price 
            FROM market_prices 
            WHERE item_name = 'Codex' AND timestamp >= ?
            ORDER BY timestamp DESC
        """, [cutoff])
        
        prices = [row['average_price'] for row in cursor.fetchall()]
        
        if not prices:
            return 10000000000  # Default fallback
        
        return