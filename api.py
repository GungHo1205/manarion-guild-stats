#!/usr/bin/env python3
"""
Simple API Server for Guild Stats
Serves data directly from SQLite database, eliminating JSON files
"""

import sqlite3
import json
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import os

class GuildStatsAPI(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.db_path = getattr(self.__class__, 'db_path', 'docs/guild-stats.db')
        super().__init__(*args, **kwargs)
    
    def send_cors_headers(self):
        """Send CORS headers."""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS preflight."""
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests for various endpoints."""
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        print(f"Request: {path}", flush=True)
        print(f"Query params: {query}", flush=True)
        
        try:
            if path == '/api/guild-data':
                self.serve_guild_data()
            elif path == '/api/daily-baseline':
                self.serve_daily_baseline(query.get('date', [None])[0])
            elif path == '/api/historical-data':
                hours = int(query.get('hours', [720])[0])  # Default 30 days
                self.serve_historical_data(hours)
            elif path == '/api/guild-history':
                guild_names = query.get('guilds', [])
                hours = int(query.get('hours', [24])[0])
                self.serve_guild_history(guild_names, hours)
            elif path == '/api/market-prices':
                hours = int(query.get('hours', [24])[0])
                self.serve_market_prices(hours)
            else:
                self.send_error_response(404, "Endpoint not found")
                
        except Exception as e:
            print(f"API Error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            self.send_error_response(500, str(e))
    
    def send_error_response(self, status_code, message):
        """Send error response with CORS headers."""
        self.send_response(status_code)
        self.send_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        error_data = {"error": message, "status": status_code}
        self.wfile.write(json.dumps(error_data).encode('utf-8'))
    
    def serve_guild_data(self):
        """Serve current guild data (replacement for guild-data.json)."""
        print(f"Attempting to connect to database: {self.db_path}", flush=True)
        
        # Check if database file exists
        if not os.path.exists(self.db_path):
            print(f"Database file not found: {self.db_path}", flush=True)
            self.send_error_response(500, f"Database file not found: {self.db_path}")
            return
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                print("Connected to database successfully", flush=True)
                
                # First check if tables exist
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master WHERE type='table' AND name='guild_snapshots'
                """)
                if not cursor.fetchone():
                    print("Table 'guild_snapshots' not found", flush=True)
                    self.send_error_response(500, "Required database tables not found")
                    return
                
                # Get latest guild data
                cursor = conn.execute("""
                    SELECT guild_name, guild_id, guild_level, nexus_level, study_level,
                           total_upgrades, nexus_progress, study_progress, codex_cost,
                           baseline_date, timestamp, data_fresh
                    FROM guild_snapshots 
                    WHERE timestamp = (SELECT MAX(timestamp) FROM guild_snapshots)
                    ORDER BY nexus_level DESC, study_level DESC, total_upgrades DESC
                """)
                
                rows = cursor.fetchall()
                print(f"Found {len(rows)} guild records", flush=True)
                
                guilds = []
                last_timestamp = None
                for row in rows:
                    if not last_timestamp:
                        last_timestamp = row['timestamp']
                    guilds.append({
                        'GuildName': row['guild_name'],
                        'GuildID': row['guild_id'],
                        'GuildLevel': row['guild_level'] or 0,
                        'NexusLevel': row['nexus_level'],
                        'StudyLevel': row['study_level'],
                        'TotalUpgrades': row['total_upgrades'] or 0,
                        'NexusProgress': row['nexus_progress'] or 0,
                        'StudyProgress': row['study_progress'] or 0,
                        'TotalCodexCost': row['codex_cost'] or 0
                    })
                
                if not guilds:
                    print("No guild data found in database", flush=True)
                    self.send_error_response(404, "No guild data found")
                    return
                
                # Calculate aggregates
                total_codex = sum(g['TotalCodexCost'] for g in guilds)
                avg_codex_price = self.get_average_codex_price(conn)
                
                # Get baseline info
                baseline_date = None
                baseline_created_at = None
                data_fresh = False
                
                if rows:
                    # Get baseline info from the first row
                    first_row = rows[0]
                    baseline_date = first_row['baseline_date']
                    data_fresh = bool(first_row['data_fresh'])
                    
                    if baseline_date:
                        cursor = conn.execute("""
                            SELECT created_at FROM daily_baselines 
                            WHERE date = ? LIMIT 1
                        """, [baseline_date])
                        row = cursor.fetchone()
                        if row:
                            baseline_created_at = row['created_at']
                
                response_data = {
                    "guilds": guilds,
                    "dustSpending": {
                        "total_codex": total_codex,
                        "formatted_dust": self.format_currency(total_codex * avg_codex_price),
                        "formatted_price": self.format_currency(avg_codex_price)
                    },
                    "lastUpdated": last_timestamp,
                    "baselineDate": baseline_date,
                    "baselineCreatedAt": baseline_created_at,
                    "dataFreshness": {
                        "guild_data_fresh": data_fresh,
                        "market_data_fresh": True  # Assume fresh for now
                    }
                }
                
                print("Successfully prepared response data", flush=True)
                self.send_json_response(response_data)
                
        except sqlite3.Error as e:
            print(f"Database error in serve_guild_data: {e}", flush=True)
            self.send_error_response(500, f"Database error: {str(e)}")
        except Exception as e:
            print(f"Error in serve_guild_data: {e}", flush=True)
            import traceback
            traceback.print_exc()
            self.send_error_response(500, f"Internal server error: {str(e)}")
    
    def serve_daily_baseline(self, date=None):
        """Serve daily baseline data."""
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                cursor = conn.execute("""
                    SELECT guild_name, nexus_level, study_level, created_at
                    FROM daily_baselines WHERE date = ?
                """, [date])
                
                guilds = {}
                created_at = None
                
                for row in cursor.fetchall():
                    guilds[row['guild_name']] = {
                        'NexusLevel': row['nexus_level'],
                        'StudyLevel': row['study_level']
                    }
                    if not created_at:
                        created_at = row['created_at']
                
                response_data = {
                    'date': date,
                    'created_at': created_at,
                    'guilds': guilds
                }
                
                self.send_json_response(response_data)
                
        except sqlite3.Error as e:
            print(f"Database error in serve_daily_baseline: {e}")
            self.send_error_response(500, "Database error")
        except Exception as e:
            print(f"Error in serve_daily_baseline: {e}")
            self.send_error_response(500, "Internal server error")
    
    def serve_historical_data(self, hours=720):
        """Serve historical data for charts (replacement for historical-data.json)."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Guild history
                cursor = conn.execute("""
                    SELECT guild_name, timestamp, nexus_level, study_level
                    FROM guild_snapshots 
                    WHERE timestamp >= ?
                    ORDER BY guild_name, timestamp
                """, [cutoff])
                
                guild_history = {}
                for row in cursor:
                    guild_name = row['guild_name']
                    if guild_name not in guild_history:
                        guild_history[guild_name] = []
                    guild_history[guild_name].append({
                        'timestamp': row['timestamp'],
                        'nexus': row['nexus_level'],
                        'study': row['study_level']
                    })
                
                # Market history
                cursor = conn.execute("""
                    SELECT item_name, timestamp, buy_price, sell_price
                    FROM market_prices 
                    WHERE timestamp >= ?
                    ORDER BY item_name, timestamp
                """, [cutoff])
                
                item_prices = {}
                for row in cursor:
                    item_name = row['item_name']
                    if item_name not in item_prices:
                        item_prices[item_name] = {'prices': []}
                    item_prices[item_name]['prices'].append({
                        'timestamp': row['timestamp'],
                        'buy': row['buy_price'],
                        'sell': row['sell_price']
                    })
                
                # Item categories (hardcoded as they don't change)
                item_categories = {
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
                
                response_data = {
                    "guild_history": guild_history,
                    "item_prices": item_prices,
                    "item_categories": item_categories
                }
                
                self.send_json_response(response_data)
                
        except sqlite3.Error as e:
            print(f"Database error in serve_historical_data: {e}")
            self.send_error_response(500, "Database error")
        except Exception as e:
            print(f"Error in serve_historical_data: {e}")
            self.send_error_response(500, "Internal server error")
    
    def serve_guild_history(self, guild_names, hours=24):
        """Serve guild history for specific guilds."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                if guild_names:
                    # Use parameterized query for guild names
                    placeholders = ','.join(['?' for _ in guild_names])
                    query = f"""
                        SELECT guild_name, timestamp, nexus_level, study_level
                        FROM guild_snapshots 
                        WHERE timestamp >= ? AND guild_name IN ({placeholders})
                        ORDER BY guild_name, timestamp
                    """
                    params = [cutoff] + guild_names
                else:
                    query = """
                        SELECT guild_name, timestamp, nexus_level, study_level
                        FROM guild_snapshots 
                        WHERE timestamp >= ?
                        ORDER BY guild_name, timestamp
                    """
                    params = [cutoff]
                
                cursor = conn.execute(query, params)
                
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
                
                self.send_json_response(history)
                
        except sqlite3.Error as e:
            print(f"Database error in serve_guild_history: {e}")
            self.send_error_response(500, "Database error")
        except Exception as e:
            print(f"Error in serve_guild_history: {e}")
            self.send_error_response(500, "Internal server error")
    
    def serve_market_prices(self, hours=24):
        """Serve market price history."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                cursor = conn.execute("""
                    SELECT item_name, timestamp, buy_price, sell_price
                    FROM market_prices 
                    WHERE timestamp >= ?
                    ORDER BY item_name, timestamp
                """, [cutoff])
                
                prices = {}
                for row in cursor:
                    item_name = row['item_name']
                    if item_name not in prices:
                        prices[item_name] = {'prices': []}
                    prices[item_name]['prices'].append({
                        'timestamp': row['timestamp'],
                        'buy': row['buy_price'],
                        'sell': row['sell_price']
                    })
                
                self.send_json_response(prices)
                
        except sqlite3.Error as e:
            print(f"Database error in serve_market_prices: {e}")
            self.send_error_response(500, "Database error")
        except Exception as e:
            print(f"Error in serve_market_prices: {e}")
            self.send_error_response(500, "Internal server error")
    
    def get_average_codex_price(self, conn, hours=24):
        """Calculate average Codex price."""
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            
            # First check if market_prices table exists
            cursor = conn.execute("""
                SELECT name FROM sqlite_master WHERE type='table' AND name='market_prices'
            """)
            if not cursor.fetchone():
                print("market_prices table not found, using default price", flush=True)
                return 10000000000
            
            cursor = conn.execute("""
                SELECT buy_price, sell_price FROM market_prices 
                WHERE item_name = 'Codex' AND timestamp >= ?
                ORDER BY timestamp DESC
            """, [cutoff])
            
            rows = cursor.fetchall()
            if not rows:
                print("No recent Codex prices found, using default", flush=True)
                return 10000000000
            
            # Calculate average of buy and sell prices
            prices = []
            for row in rows:
                if row['buy_price'] and row['sell_price']:
                    avg_price = (row['buy_price'] + row['sell_price']) / 2
                    prices.append(avg_price)
            
            return sum(prices) / len(prices) if prices else 10000000000
            
        except Exception as e:
            print(f"Error getting codex price: {e}", flush=True)
            return 10000000000
    
    def format_currency(self, amount):
        """Format currency values."""
        if amount >= 1e12: return f"{amount / 1e12:.2f}T"
        if amount >= 1e9: return f"{amount / 1e9:.2f}B"
        if amount >= 1e6: return f"{amount / 1e6:.2f}M"
        if amount >= 1e3: return f"{amount / 1e3:.2f}K"
        return f"{amount:.2f}"
    
    def send_json_response(self, data):
        """Send JSON response with appropriate headers."""
        self.send_response(200)
        self.send_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

def create_handler(db_path):
    """Create a request handler with the specified database path."""
    class Handler(GuildStatsAPI):
        pass
    Handler.db_path = db_path
    return Handler

def main():
    """Run the API server."""
    import argparse
    parser = argparse.ArgumentParser(description='Guild Stats API Server')
    parser.add_argument('--port', type=int, default=8000, help='Port to run server on')
    parser.add_argument('--db', default='docs/guild-stats.db', help='Database path')
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        return
    
    handler = create_handler(args.db)
    server = HTTPServer(('localhost', args.port), handler)
    print(f"Guild Stats API server running on http://localhost:{args.port}")
    print(f"Database: {args.db}")
    print("\nAvailable endpoints:")
    print("  GET /api/guild-data")
    print("  GET /api/daily-baseline[?date=YYYY-MM-DD]")
    print("  GET /api/historical-data[?hours=720]")
    print("  GET /api/guild-history[?guilds=name1,name2&hours=24]")
    print("  GET /api/market-prices[?hours=24]")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.shutdown()

if __name__ == "__main__":
    main()