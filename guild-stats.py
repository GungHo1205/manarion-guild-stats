import requests
import csv
import time
import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('guild_stats.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.manarion.com"
LEADERBOARD_TYPE = "boost_damage"
BASE_PER_UPGRADE = 0.02  # 0.02% per upgrade
MAX_WORKERS = 2  # For concurrent API calls
API_DELAY = 2  # Reduced delay between API calls
CODEX_ITEM_ID = "3"  # Codex item ID in market API

@dataclass
class GuildData:
    name: str
    study_level: int
    nexus_level: int
    study_progress: Optional[int] = None
    nexus_progress: Optional[int] = None
    study_codex_cost: Optional[int] = None
    nexus_codex_cost: Optional[int] = None
    total_codex_cost: Optional[int] = None
    members_count: Optional[int] = None

class MarketDataManager:
    """Manages market price data and averaging"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.market_file = "docs/market-prices.json"
    
    def format_currency(self, amount: float) -> str:
        """Format large numbers as shortened currency (21.00B, 19.49B, etc.)"""
        if amount >= 1_000_000_000_000:  # Trillions
            return f"{amount / 1_000_000_000_000:.2f}T"
        elif amount >= 1_000_000_000:  # Billions
            return f"{amount / 1_000_000_000:.2f}B"
        elif amount >= 1_000_000:  # Millions
            return f"{amount / 1_000_000:.2f}M"
        elif amount >= 1_000:  # Thousands
            return f"{amount / 1_000:.2f}K"
        else:
            return f"{amount:.2f}"
    
    def get_current_market_prices(self) -> Optional[Dict]:
        """Fetch current market prices from API"""
        try:
            market_data = self.api_client.get_market_data()
            if not market_data:
                return None
            
            # Extract codex prices (item ID "3")
            buy_price = market_data.get("Buy", {}).get(CODEX_ITEM_ID)
            sell_price = market_data.get("Sell", {}).get(CODEX_ITEM_ID)
            
            if buy_price is None or sell_price is None:
                logger.warning("Codex prices not found in market data")
                return None
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "buy_price": buy_price,
                "sell_price": sell_price,
                "average_price": (buy_price + sell_price) / 2
            }
            
        except Exception as e:
            logger.error(f"Error fetching market prices: {e}")
            return None
    
    def load_price_history(self) -> Dict:
        """Load historical price data"""
        if os.path.exists(self.market_file):
            try:
                with open(self.market_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading price history: {e}")
        
        return {
            "prices": [],
            "daily_averages": {}
        }
    
    def save_price_data(self, price_data: Dict, history: Dict) -> Dict:
        """Save current price data and update history"""
        # Add current price to history
        history["prices"].append(price_data)
        
        # Calculate daily average
        today = price_data["timestamp"].split('T')[0]
        today_prices = [
            p for p in history["prices"]
            if p["timestamp"].startswith(today)
        ]
        
        if today_prices:
            avg_buy = sum(p["buy_price"] for p in today_prices) / len(today_prices)
            avg_sell = sum(p["sell_price"] for p in today_prices) / len(today_prices)
            daily_avg = (avg_buy + avg_sell) / 2
            
            history["daily_averages"][today] = {
                "average_price": daily_avg,
                "buy_average": avg_buy,
                "sell_average": avg_sell,
                "sample_count": len(today_prices),
                "formatted_price": self.format_currency(daily_avg)
            }
        
        # Keep only last 7 days of detailed prices (for storage efficiency)
        cutoff_date = datetime.now(timezone.utc).timestamp() - (7 * 24 * 60 * 60)
        history["prices"] = [
            p for p in history["prices"]
            if datetime.fromisoformat(p["timestamp"].replace('Z', '+00:00')).timestamp() > cutoff_date
        ]
        
        # Save to file
        os.makedirs("docs", exist_ok=True)
        with open(self.market_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2)
        
        logger.info(f"Updated market prices - Current: {self.format_currency(price_data['sell_price'])}")
        return history
    
    def get_daily_average_price(self) -> Optional[Dict]:
        """Get today's average price"""
        history = self.load_price_history()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        if today in history["daily_averages"]:
            return history["daily_averages"][today]
        
        # If no daily average yet, use current price
        current_price = self.get_current_market_prices()
        if current_price:
            return {
                "average_price": current_price["sell_price"],  # Use sell price as default
                "formatted_price": self.format_currency(current_price["sell_price"]),
                "sample_count": 1
            }
        
        return None

class APIClient:
    """Centralized API client with error handling and rate limiting"""
    
    def __init__(self, base_url: str, delay: float = API_DELAY):
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'GuildStatsBot/1.0'
        })
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with error handling"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            time.sleep(self.delay)
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            return None
    
    def get_leaderboard_page(self, page: int) -> Optional[Dict]:
        return self._make_request(f"/leaderboards/{LEADERBOARD_TYPE}", {"page": page})
    
    def get_guilds(self) -> Optional[List[Dict]]:
        return self._make_request("/guilds")
    
    def get_player_profile(self, name: str) -> Optional[Dict]:
        return self._make_request(f"/players/{name}")
    
    def get_market_data(self) -> Optional[Dict]:
        return self._make_request("/market")

class GuildStatsTracker:
    """Main class for tracking guild statistics"""
    
    def __init__(self):
        self.api_client = APIClient(BASE_URL)
        self.market_manager = MarketDataManager(self.api_client)
        self.guild_lookup = {}  # Cache for guild ID -> name mapping
        self.processed_guilds = set()  # Track already processed guilds
    
    def load_guild_lookup(self) -> bool:
        """Load and cache guild ID to name mapping"""
        logger.info("Loading guild lookup data...")
        guilds_data = self.api_client.get_guilds()
        if not guilds_data:
            logger.error("Failed to load guilds data")
            return False
        
        self.guild_lookup = {guild.get("ID", 0): guild.get("Name", "") 
                           for guild in guilds_data}
        logger.info(f"Loaded {len(self.guild_lookup)} guilds")
        return True
    
    def update_market_prices(self) -> Optional[Dict]:
        """Update market price data"""
        logger.info("Updating market prices...")
        current_prices = self.market_manager.get_current_market_prices()
        
        if current_prices:
            history = self.market_manager.load_price_history()
            updated_history = self.market_manager.save_price_data(current_prices, history)
            return updated_history
        else:
            logger.warning("Failed to update market prices")
            return None
    
    def calculate_nexus_level(self, research_damage_percent: float, upgrades: int) -> Optional[int]:
        """Calculate nexus level from damage percentage and upgrades"""
        if upgrades <= 0:
            return None
        
        multiplier = research_damage_percent / (upgrades * BASE_PER_UPGRADE)
        level = 100 * (multiplier - 1.0)
        return max(0, round(level))  # Ensure non-negative
    
    def calculate_study_room_level(self, total_exp_boost: int, codex_boost: int, enchant_boost: int) -> int:
        """Calculate study room level"""
        return max(0, total_exp_boost - codex_boost - enchant_boost)
    
    def safe_get_infusions_count(self, infusions_data) -> int:
        """Safely extract infusion count from infusions data"""
        if isinstance(infusions_data, dict):
            return sum(v for v in infusions_data.values() if isinstance(v, (int, float)))
        elif isinstance(infusions_data, (int, float)):
            return infusions_data
        return 0
    
    def process_player_data(self, player_name: str, upgrades: int) -> Optional[GuildData]:
        """Process individual player data and extract guild information"""
        try:
            player_data = self.api_client.get_player_profile(player_name)
            if not player_data:
                return None
            
            guild_id = player_data.get("GuildID", 0)
            guild_name = self.guild_lookup.get(guild_id, "Unknown")
            
            # Skip if we already processed this guild
            if guild_name in self.processed_guilds:
                return None
            
            # Extract player stats
            codex_exp_boost = player_data.get("BaseBoosts", {}).get("100", 0)
            total_exp_boost = player_data.get("TotalBoosts", {}).get("100", 0)
            total_damage_percent = player_data.get("TotalBoosts", {}).get("40", 0) * 100
            equipments = player_data.get("Equipment", {})
            
            # Calculate equipment bonuses
            total_equipment_boosts = 0
            enchant_boost = 0
            
            for item in range(1, 9):
                try:
                    if item == 5:  # Enchantment slot
                        enchant_boost = equipments.get(str(item), {}).get("Boosts", {}).get("100", 0)
                    
                    infusions_raw = equipments.get(str(item), {}).get("Infusions", {})
                    infusions_count = self.safe_get_infusions_count(infusions_raw)
                    base_boost = equipments.get(str(item), {}).get("Boosts", {}).get("40", 0)
                    
                    equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                    total_equipment_boosts += equip_percent
                    
                except Exception as e:
                    logger.warning(f"Error processing equipment item {item} for {player_name}: {e}")
                    continue
            
            # Calculate base damage and levels
            base_damage_percent = total_damage_percent - total_equipment_boosts - 100
            study_level = self.calculate_study_room_level(total_exp_boost, codex_exp_boost, enchant_boost)
            nexus_level = self.calculate_nexus_level(base_damage_percent, upgrades)
            
            # Mark guild as processed
            self.processed_guilds.add(guild_name)
            
            return GuildData(
                name=guild_name,
                study_level=study_level,
                nexus_level=nexus_level if nexus_level is not None else 0
            )
            
        except Exception as e:
            logger.error(f"Error processing player {player_name}: {e}")
            return None
    
    def fetch_leaderboard_data(self, max_entries: int = 400) -> List[GuildData]:
        """Fetch and process leaderboard data"""
        results = []
        self.processed_guilds.clear()
        
        # Load guild lookup first
        if not self.load_guild_lookup():
            return results
        
        logger.info("Starting leaderboard data collection...")
        
        page = 1
        entries_processed = 0
        
        while entries_processed < max_entries:
            logger.info(f"Processing leaderboard page {page}...")
            
            leaderboard_data = self.api_client.get_leaderboard_page(page)
            if not leaderboard_data or "Entries" not in leaderboard_data:
                logger.info(f"No more entries on page {page}")
                break
            
            entries = leaderboard_data["Entries"]
            if not entries:
                break
            
            # Process entries with threading for better performance
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                
                for entry in entries:
                    if entries_processed >= max_entries:
                        break
                    
                    player_name = entry.get("Name") or entry.get("name")
                    upgrades = entry.get("Score", 0) or entry.get("score", 0)
                    
                    if not player_name:
                        continue
                    
                    future = executor.submit(self.process_player_data, player_name, upgrades)
                    futures.append(future)
                    entries_processed += 1
                
                # Collect results
                for future in concurrent.futures.as_completed(futures):
                    guild_data = future.result()
                    if guild_data:
                        results.append(guild_data)
                        logger.info(f"Added guild: {guild_data.name}, Study: {guild_data.study_level}, Nexus: {guild_data.nexus_level}")
            
            page += 1
            
            # Break if we have enough unique guilds
            if len(results) >= 50:  # Reasonable number of guilds
                break
        
        logger.info(f"Collected data for {len(results)} unique guilds")
        return results
    
    def load_baseline_data(self) -> Optional[Dict]:
        """Load the daily baseline data if it exists"""
        baseline_file = "docs/daily-baseline.json"
        if os.path.exists(baseline_file):
            try:
                with open(baseline_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading baseline data: {e}")
        return None
    
    def should_update_baseline(self, baseline_data: Optional[Dict], current_timestamp: str) -> Tuple[bool, str]:
        """Determine if baseline should be updated"""
        if not baseline_data:
            return True, "No baseline exists"
        
        today = current_timestamp.split('T')[0]
        baseline_date = baseline_data.get("date", "")
        
        # Update if it's a new day
        if baseline_date != today:
            return True, f"New day: {baseline_date} -> {today}"
        
        # Update if it's midnight run
        current_hour = datetime.now(timezone.utc).hour
        if current_hour == 0:
            return True, "Midnight baseline update"
        
        return False, "Baseline is current"
    
    def calculate_codex_cost(self, current_level: int, levels_gained: int) -> int:
        """Calculate total codex cost for gaining levels"""
        if levels_gained <= 0:
            return 0
        
        total_cost = 0
        for i in range(levels_gained):
            next_level_cost = current_level + 1 + i
            total_cost += next_level_cost
        
        return total_cost
    
    def calculate_daily_progress(self, current_guilds: List[GuildData], baseline_data: Optional[Dict]) -> List[GuildData]:
        """Calculate progress since daily baseline"""
        if not baseline_data:
            return current_guilds
        
        baseline_guilds = baseline_data.get("guilds", {})
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        baseline_date = baseline_data.get("date", "")
        
        # If baseline is not from today, don't show progress
        if baseline_date != today:
            for guild in current_guilds:
                guild.study_progress = None
                guild.nexus_progress = None
                guild.study_codex_cost = None
                guild.nexus_codex_cost = None
                guild.total_codex_cost = None
            return current_guilds
        
        # Calculate progress for each guild
        for guild in current_guilds:
            if guild.name in baseline_guilds:
                baseline = baseline_guilds[guild.name]
                study_progress = guild.study_level - baseline["StudyLevel"]
                nexus_progress = guild.nexus_level - baseline["NexusLevel"]
                
                guild.study_progress = study_progress
                guild.nexus_progress = nexus_progress
                
                # Calculate codex costs
                guild.study_codex_cost = self.calculate_codex_cost(baseline["StudyLevel"], study_progress)
                guild.nexus_codex_cost = self.calculate_codex_cost(baseline["NexusLevel"], nexus_progress)
                guild.total_codex_cost = guild.study_codex_cost + guild.nexus_codex_cost
            else:
                # New guild not in baseline
                guild.study_progress = None
                guild.nexus_progress = None
                guild.study_codex_cost = None
                guild.nexus_codex_cost = None
                guild.total_codex_cost = None
        
        return current_guilds
    
    def save_baseline_data(self, guilds_data: List[GuildData], timestamp: str) -> Dict:
        """Save current data as the daily baseline"""
        baseline_data = {
            "date": timestamp.split('T')[0],
            "timestamp": timestamp,
            "guilds": {}
        }
        
        for guild in guilds_data:
            baseline_data["guilds"][guild.name] = {
                "StudyLevel": guild.study_level,
                "NexusLevel": guild.nexus_level
            }
        
        os.makedirs("docs", exist_ok=True)
        with open("docs/daily-baseline.json", 'w', encoding='utf-8') as f:
            json.dump(baseline_data, f, indent=2)
        
        logger.info(f"Updated daily baseline for {len(guilds_data)} guilds")
        return baseline_data
    
    def guild_data_to_dict(self, guild: GuildData) -> Dict:
        """Convert GuildData to dictionary"""
        return {
            "GuildName": guild.name,
            "StudyLevel": guild.study_level,
            "NexusLevel": guild.nexus_level,
            "StudyProgress": guild.study_progress,
            "NexusProgress": guild.nexus_progress,
            "StudyCodexCost": guild.study_codex_cost,
            "NexusCodexCost": guild.nexus_codex_cost,
            "TotalCodexCost": guild.total_codex_cost
        }
    
    def calculate_dust_spending(self, guilds_data: List[GuildData], price_info: Optional[Dict]) -> Dict:
        """Calculate total dust spending on codex"""
        total_codex_used = sum(g.total_codex_cost or 0 for g in guilds_data)
        
        if not price_info or total_codex_used == 0:
            return {
                "total_dust_spent": 0,
                "formatted_dust": "0",
                "codex_used": total_codex_used,
                "average_price": 0,
                "formatted_price": "No price data"
            }
        
        total_dust = total_codex_used * price_info["average_price"]
        
        return {
            "total_dust_spent": total_dust,
            "formatted_dust": self.market_manager.format_currency(total_dust),
            "codex_used": total_codex_used,
            "average_price": price_info["average_price"],
            "formatted_price": price_info["formatted_price"]
        }
    
    def save_results(self, guilds_data: List[GuildData], baseline_data: Optional[Dict], timestamp: str, dust_data: Dict):
        """Save results to files"""
        os.makedirs("docs", exist_ok=True)
        
        # Convert to dictionaries
        guild_dicts = [self.guild_data_to_dict(guild) for guild in guilds_data]
        
        # Save CSV (for backward compatibility)
        if guild_dicts:
            with open("guild_stats.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=guild_dicts[0].keys())
                writer.writeheader()
                writer.writerows(guild_dicts)
        
        # Prepare data for JSON
        json_data = {
            "lastUpdated": timestamp,
            "baselineDate": baseline_data.get("date", None) if baseline_data else None,
            "baselineTimestamp": baseline_data.get("timestamp", None) if baseline_data else None,
            "dustSpending": dust_data,
            "guilds": guild_dicts
        }
        
        # Save JSON for website
        with open("docs/guild-data.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)
        
        logger.info("Files saved successfully")
    
    def print_summary(self, guilds_data: List[GuildData], baseline_data: Optional[Dict], timestamp: str, dust_data: Dict):
        """Print progress summary"""
        if baseline_data and baseline_data.get("date") == timestamp.split('T')[0]:
            total_study_gains = sum(g.study_progress or 0 for g in guilds_data)
            total_nexus_gains = sum(g.nexus_progress or 0 for g in guilds_data)
            total_levels_gained = total_study_gains + total_nexus_gains
            
            logger.info(f"Daily levels gained: {total_levels_gained} (Study: +{total_study_gains}, Nexus: +{total_nexus_gains})")
            logger.info(f"Estimated codex used: {dust_data['codex_used']:,}")
            logger.info(f"Estimated dust spent: {dust_data['formatted_dust']} (at {dust_data['formatted_price']} per codex)")
    
    def run(self):
        """Main execution method"""
        logger.info("Starting guild stats collection...")
        
        current_time = datetime.now(timezone.utc)
        timestamp = current_time.isoformat()
        
        try:
            # Update market prices first
            self.update_market_prices()
            
            # Fetch guild data
            guilds_data = self.fetch_leaderboard_data()
            
            if not guilds_data:
                logger.error("No guild data collected")
                return
            
            # Sort by nexus level
            guilds_data.sort(key=lambda x: x.nexus_level, reverse=True)
            
            # Load baseline data for progress calculation
            baseline_data = self.load_baseline_data()
            
            # Check if we should update the baseline
            should_update, reason = self.should_update_baseline(baseline_data, timestamp)
            
            if should_update:
                logger.info(f"Updating baseline: {reason}")
                baseline_data = self.save_baseline_data(guilds_data, timestamp)
            else:
                logger.info(f"Keeping existing baseline: {reason}")
            
            # Calculate daily progress
            guilds_with_progress = self.calculate_daily_progress(guilds_data, baseline_data)
            
            # Calculate dust spending
            price_info = self.market_manager.get_daily_average_price()
            dust_data = self.calculate_dust_spending(guilds_with_progress, price_info)
            
            # Save results
            self.save_results(guilds_with_progress, baseline_data, timestamp, dust_data)
            
            # Print summary
            self.print_summary(guilds_with_progress, baseline_data, timestamp, dust_data)
            
            logger.info(f"Successfully processed {len(guilds_data)} guilds")
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            raise

def main():
    """Entry point"""
    tracker = GuildStatsTracker()
    tracker.run()

if __name__ == "__main__":
    main()