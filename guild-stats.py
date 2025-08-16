#!/usr/bin/env python3
"""
Guild Stats Collection Script
Collects guild data, tracks progress, and monitors codex market prices.
"""

import json
import os
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

class GuildStatsCollector:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        self.data_dir = "docs"
        self.guild_data_file = os.path.join(self.data_dir, "guild-data.json")
        self.historical_data_file = os.path.join(self.data_dir, "historical-data.json")
        self.baseline_file = os.path.join(self.data_dir, "daily-baseline.json")
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
    def safe_api_call(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        """Make API call with retry logic and error handling."""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"API call failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        return None

    def get_leaderboard_data(self) -> Optional[List[Dict]]:
        """Get leaderboard data from API."""
        url = f"{self.api_base_url}/leaderboard"  # Replace with actual endpoint
        return self.safe_api_call(url)

    def get_player_data(self, player_name: str) -> Optional[Dict]:
        """Get individual player data from API."""
        url = f"{self.api_base_url}/player/{player_name}"  # Replace with actual endpoint
        return self.safe_api_call(url)

    def get_codex_market_prices(self) -> Optional[Dict]:
        """Get current codex market prices."""
        url = f"{self.api_base_url}/market/codex"  # Replace with actual endpoint
        return self.safe_api_call(url)

    def load_existing_data(self, filename: str) -> Dict:
        """Load existing data file or return empty dict."""
        filepath = os.path.join(self.data_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading {filename}: {e}")
        return {}

    def save_data(self, filename: str, data: Dict):
        """Save data to file."""
        filepath = os.path.join(self.data_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            print(f"Saved {filename}")
        except IOError as e:
            print(f"Error saving {filename}: {e}")

    def calculate_level_from_upgrades(self, upgrades: int, upgrade_type: str) -> int:
        """Calculate level from upgrade count."""
        # Replace with your actual level calculation logic
        # This is just an example
        if upgrade_type == "nexus":
            return min(upgrades // 10, 200)  # Example calculation
        elif upgrade_type == "study":
            return min(upgrades // 15, 200)  # Example calculation
        return 0

    def estimate_codex_cost(self, level_diff: int, upgrade_type: str) -> int:
        """Estimate codex cost for level increases."""
        # Replace with your actual cost calculation logic
        base_cost = 100 if upgrade_type == "nexus" else 150
        return level_diff * base_cost

    def format_currency(self, amount: int) -> str:
        """Format currency with appropriate units."""
        if amount >= 1_000_000_000_000:
            return f"{amount / 1_000_000_000_000:.2f}T"
        elif amount >= 1_000_000_000:
            return f"{amount / 1_000_000_000:.2f}B"
        elif amount >= 1_000_000:
            return f"{amount / 1_000_000:.2f}M"
        elif amount >= 1_000:
            return f"{amount / 1_000:.2f}K"
        else:
            return f"{amount:.2f}"

    def get_or_create_baseline(self) -> Dict:
        """Get existing baseline or create new one."""
        baseline = self.load_existing_data("daily-baseline.json")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Create new baseline if doesn't exist or is from different day
        if not baseline or baseline.get("date") != today:
            print("Creating new daily baseline...")
            baseline = {
                "date": today,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "guilds": {}
            }
            
        return baseline

    def update_historical_data(self, guild_name: str, nexus_level: int, study_level: int, 
                             buy_price: Optional[int] = None, sell_price: Optional[int] = None):
        """Update combined historical data for guilds and market prices."""
        historical_data = self.load_existing_data("historical-data.json")
        
        # Initialize structure if needed
        if "guild_history" not in historical_data:
            historical_data["guild_history"] = {}
        if "market_prices" not in historical_data:
            historical_data["market_prices"] = {"prices": [], "daily_averages": {}}
        
        # Update guild history
        if guild_name not in historical_data["guild_history"]:
            historical_data["guild_history"][guild_name] = []
        
        # Add new guild data point
        new_guild_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "nexus": nexus_level,
            "study": study_level
        }
        
        historical_data["guild_history"][guild_name].append(new_guild_entry)
        
        # Update market prices if provided
        if buy_price is not None and sell_price is not None:
            average_price = (buy_price + sell_price) / 2
            
            new_price_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "buy_price": buy_price,
                "sell_price": sell_price,
                "average_price": int(average_price)
            }
            
            historical_data["market_prices"]["prices"].append(new_price_entry)
            
            # Update daily averages
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if today not in historical_data["market_prices"]["daily_averages"]:
                historical_data["market_prices"]["daily_averages"][today] = {
                    "buy_prices": [],
                    "sell_prices": [],
                    "avg_prices": []
                }
            
            historical_data["market_prices"]["daily_averages"][today]["buy_prices"].append(buy_price)
            historical_data["market_prices"]["daily_averages"][today]["sell_prices"].append(sell_price)
            historical_data["market_prices"]["daily_averages"][today]["avg_prices"].append(average_price)
            
            # Calculate final daily averages
            day_data = historical_data["market_prices"]["daily_averages"][today]
            historical_data["market_prices"]["daily_averages"][today] = {
                "average_price": sum(day_data["avg_prices"]) / len(day_data["avg_prices"]),
                "buy_average": sum(day_data["buy_prices"]) / len(day_data["buy_prices"]),
                "sell_average": sum(day_data["sell_prices"]) / len(day_data["sell_prices"]),
                "sample_count": len(day_data["avg_prices"]),
                "formatted_price": self.format_currency(sum(day_data["avg_prices"]) / len(day_data["avg_prices"]))
            }
        
        # Clean old data (keep 30 days)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
        
        # Clean guild history
        for guild in historical_data["guild_history"]:
            historical_data["guild_history"][guild] = [
                entry for entry in historical_data["guild_history"][guild]
                if datetime.fromisoformat(entry["timestamp"]) >= cutoff_date
            ]
        
        # Clean market prices
        historical_data["market_prices"]["prices"] = [
            entry for entry in historical_data["market_prices"]["prices"]
            if datetime.fromisoformat(entry["timestamp"]) >= cutoff_date
        ]
        
        # Clean daily averages
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
        historical_data["market_prices"]["daily_averages"] = {
            date: data for date, data in historical_data["market_prices"]["daily_averages"].items()
            if date >= cutoff_date_str
        }
        
        # Update metadata
        historical_data["last_updated"] = datetime.now(timezone.utc).isoformat()
        
        self.save_data("historical-data.json", historical_data)

    def get_previous_market_price(self) -> Optional[int]:
        """Get the most recent market price if current API call fails."""
        historical_data = self.load_existing_data("historical-data.json")
        prices = historical_data.get("market_prices", {}).get("prices", [])
        
        if prices:
            return prices[-1]["average_price"]
        return None

    def collect_guild_data(self) -> Dict:
        """Main function to collect and process guild data."""
        print(f"Starting guild data collection at {datetime.now(timezone.utc)}")
        
        # Get baseline data
        baseline = self.get_or_create_baseline()
        
        # Get leaderboard data
        leaderboard_data = self.get_leaderboard_data()
        if not leaderboard_data:
            print("Failed to get leaderboard data")
            return {}
        
        # Process guild data
        guilds = []
        total_codex_spent = 0
        
        # Track which guilds we've seen to avoid duplicates
        seen_guilds = {}
        
        for player_entry in leaderboard_data:
            # Extract player info and get detailed data
            player_name = player_entry.get("name", "")
            if not player_name:
                continue
                
            player_data = self.get_player_data(player_name)
            if not player_data:
                print(f"Failed to get data for player: {player_name}")
                continue
            
            # Extract guild and level info
            guild_name = player_data.get("guild_name", "")
            if not guild_name or guild_name in seen_guilds:
                continue
                
            # Calculate levels from upgrade counts
            nexus_upgrades = player_data.get("nexus_upgrades", 0)
            study_upgrades = player_data.get("study_upgrades", 0)
            
            nexus_level = self.calculate_level_from_upgrades(nexus_upgrades, "nexus")
            study_level = self.calculate_level_from_upgrades(study_upgrades, "study")
            
            # Calculate progress since baseline
            baseline_guild = baseline["guilds"].get(guild_name, {})
            baseline_nexus = baseline_guild.get("NexusLevel", nexus_level)
            baseline_study = baseline_guild.get("StudyLevel", study_level)
            
            nexus_progress = nexus_level - baseline_nexus
            study_progress = study_level - baseline_study
            
            # Estimate codex costs
            nexus_codex_cost = self.estimate_codex_cost(max(0, nexus_progress), "nexus")
            study_codex_cost = self.estimate_codex_cost(max(0, study_progress), "study")
            total_codex_cost = nexus_codex_cost + study_codex_cost
            
            total_codex_spent += total_codex_cost
            
            # Update guild data
            guild_info = {
                "GuildName": guild_name,
                "NexusLevel": nexus_level,
                "StudyLevel": study_level,
                "NexusProgress": nexus_progress,
                "StudyProgress": study_progress,
                "NexusCodexCost": nexus_codex_cost,
                "StudyCodexCost": study_codex_cost,
                "TotalCodexCost": total_codex_cost
            }
            
            guilds.append(guild_info)
            seen_guilds[guild_name] = True
            
            # Update baseline if this is first time seeing this guild
            if guild_name not in baseline["guilds"]:
                baseline["guilds"][guild_name] = {
                    "NexusLevel": nexus_level,
                    "StudyLevel": study_level
                }
        
        # Save updated baseline
        self.save_data("daily-baseline.json", baseline)
        
        # Get market prices
        market_data = self.get_codex_market_prices()
        average_price = None
        buy_price = None
        sell_price = None
        
        if market_data:
            buy_price = market_data.get("buy_price", 0)
            sell_price = market_data.get("sell_price", 0)
            if buy_price and sell_price:
                average_price = (buy_price + sell_price) / 2
                print(f"Updated market prices: Buy={self.format_currency(buy_price)}, Sell={self.format_currency(sell_price)}")
        
        # If market API failed, use previous price
        if average_price is None:
            average_price = self.get_previous_market_price()
            if average_price:
                print(f"Using previous market price: {self.format_currency(average_price)}")
        
        # Update historical data for each guild and market prices
        for guild_info in guilds:
            self.update_historical_data(
                guild_info["GuildName"], 
                guild_info["NexusLevel"], 
                guild_info["StudyLevel"],
                buy_price,
                sell_price
            )
        
        # Calculate dust spending
        dust_spending = {}
        if average_price and total_codex_spent > 0:
            total_dust = int(average_price * total_codex_spent)
            dust_spending = {
                "formatted_dust": self.format_currency(total_dust),
                "formatted_price": self.format_currency(average_price),
                "total_dust": total_dust,
                "average_price": int(average_price),
                "total_codex": total_codex_spent
            }
        
        # Compile final data
        final_data = {
            "guilds": guilds,
            "dustSpending": dust_spending,
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "baselineDate": baseline.get("date", "Unknown"),
            "totalGuilds": len(guilds)
        }
        
        # Save guild data
        self.save_data("guild-data.json", final_data)
        
        print(f"Collection complete: {len(guilds)} guilds processed, {total_codex_spent} total codex spent")
        return final_data

    def create_empty_files_if_missing(self):
        """Create empty data files if they don't exist to prevent errors."""
        files_to_create = {
            "guild-data.json": {
                "guilds": [],
                "dustSpending": {},
                "lastUpdated": datetime.now(timezone.utc).isoformat(),
                "baselineDate": "No baseline yet",
                "totalGuilds": 0
            },
            "historical-data.json": {
                "guild_history": {},
                "market_prices": {
                    "prices": [],
                    "daily_averages": {}
                },
                "last_updated": datetime.now(timezone.utc).isoformat()
            },
            "daily-baseline.json": {
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "guilds": {}
            }
        }
        
        for filename, default_data in files_to_create.items():
            filepath = os.path.join(self.data_dir, filename)
            if not os.path.exists(filepath):
                self.save_data(filename, default_data)
                print(f"Created empty {filename}")

def main():
    """Main execution function."""
    # Replace with your actual API base URL
    API_BASE_URL = "https://your-game-api.com/api/v1"
    
    collector = GuildStatsCollector(API_BASE_URL)
    
    # Ensure all data files exist
    collector.create_empty_files_if_missing()
    
    # Collect data
    try:
        result = collector.collect_guild_data()
        if result:
            print("Data collection successful!")
        else:
            print("Data collection failed!")
            return 1
    except Exception as e:
        print(f"Error during data collection: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())