#!/usr/bin/env python3
"""
Mock Data Generation Server for Guild Stats Testing
- Generates realistic mock data that matches guild-stats.py output exactly
- Creates complete historical data for testing charts and functionality
- Only runs locally for testing, never in production
"""
import os
import json
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict

# --- Configuration matching guild-stats.py ---
DATA_DIR = "docs"
GUILD_DATA_FILE = os.path.join(DATA_DIR, "guild-data.json")
BASELINE_FILE = os.path.join(DATA_DIR, "daily-baseline.json")
HISTORICAL_FILE = os.path.join(DATA_DIR, "historical-data.json")

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
    35: "Orb of Power", 36: "Orb of Chaos", 37: "Orb of Divinity", 45: "Orb of Legacy",
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
    "Orbs/Upgrades": ["Orb of Power", "Orb of Chaos", "Orb of Divinity", "Orb of Legacy", "Elementium", "Divine Essence"],
    "Herbs": ["Sunpetal", "Sageroot", "Bloomwell"],
    "Enchanting Reagents": ["Fire Essence", "Water Essence", "Nature Essence", "Asbestos", "Ironbark", "Fish Scales", 
                           "Elderwood", "Lodestone", "White Pearl", "Four-Leaf Clover", "Enchanted Droplet", "Infernal Heart"],
    "Enchanting Formulas": ["Formula: Fire Resistance", "Formula: Water Resistance", "Formula: Nature Resistance",
                           "Formula: Inferno", "Formula: Tidal Wrath", "Formula: Wildheart", "Formula: Insight",
                           "Formula: Bountiful Harvest", "Formula: Prosperity", "Formula: Fortune", "Formula: Growth", "Formula: Vitality"],
    "Special": ["Crystallized Mana"]
}

UNTRADEABLE_IDS = {38, 42, 43, 48, 49}

class MockDataGenerator:
    def __init__(self):
        self.BASE_PER_UPGRADE = 0.02
        os.makedirs(DATA_DIR, exist_ok=True)

    def calculate_codex_cost(self, start_level: int, progress: int) -> int:
        if progress <= 0: return 0
        return sum(range(start_level + 1, start_level + progress + 1))

    def format_currency(self, amount: float) -> str:
        if amount >= 1e12: return f"{amount / 1e12:.2f}T"
        if amount >= 1e9: return f"{amount / 1e9:.2f}B"
        if amount >= 1e6: return f"{amount / 1e6:.2f}M"
        if amount >= 1e3: return f"{amount / 1e3:.2f}K"
        return f"{amount:.2f}"

    def generate_guild_data(self) -> List[Dict]:
        """Generate realistic guild data matching actual API responses."""
        guild_names = [
            "Phoenix Legends", "Dragon Warriors", "Shadow Hunters", "Mystic Order",
            "Iron Brotherhood", "Storm Riders", "Void Seekers", "Crystal Guard",
            "Fire Keepers", "Wind Walkers", "Earth Shapers", "Wave Masters",
            "Thunder Clan", "Frost Giants", "Ember Guild", "Moonlight Society"
        ]
        
        guilds = []
        for i, name in enumerate(guild_names):
            # Generate realistic levels with some variation
            base_nexus = 580 + random.randint(-80, 120)
            base_study = 420 + random.randint(-60, 100)
            
            guilds.append({
                "GuildName": name,
                "NexusLevel": base_nexus,
                "StudyLevel": base_study
            })
        
        return sorted(guilds, key=lambda x: x["NexusLevel"], reverse=True)

    def generate_historical_data(self, current_guilds: List[Dict], hours_back: int = 72) -> Dict:
        """Generate comprehensive historical data for guilds and market prices."""
        now = datetime.now(timezone.utc)
        history = {"guild_history": {}, "item_prices": {}, "item_categories": ITEM_CATEGORIES}
        
        # Generate guild progression history
        for guild in current_guilds:
            name = guild["GuildName"]
            history["guild_history"][name] = []
            
            current_nexus = guild["NexusLevel"]
            current_study = guild["StudyLevel"]
            
            for i in range(hours_back):
                timestamp = (now - timedelta(hours=i)).isoformat()
                
                # Simulate realistic backward progression (levels decrease going back in time)
                nexus_decline = int(i * random.uniform(0.6, 1.4))
                study_decline = int(i * random.uniform(0.4, 1.2))
                
                historical_nexus = max(0, current_nexus - nexus_decline)
                historical_study = max(0, current_study - study_decline)
                
                history["guild_history"][name].insert(0, {
                    "timestamp": timestamp,
                    "nexus": historical_nexus,
                    "study": historical_study
                })
        
        # Generate market price history for all tradeable items
        tradeable_items = {k: v for k, v in ITEM_MAPPING.items() if k not in UNTRADEABLE_IDS}
        
        # Define realistic base prices
        base_prices = {
            "Codex": 10000000000,  # 10B base price for Codex
            "Mana Dust": 50000000,  # 50M
            "Elemental Shards": 75000000,  # 75M
            "Orb of Power": 5000000000,  # 5B
            "Orb of Chaos": 8000000000,  # 8B
            "Orb of Divinity": 15000000000,  # 15B
            "Orb of Legacy": 12000000000,  # 12B
            "Elementium": 2000000000,  # 2B
            "Divine Essence": 3000000000,  # 3B
            "Crystallized Mana": 500000000,  # 500M
            # Resources
            "Fish": 100000,
            "Wood": 150000,
            "Iron": 200000,
            # Spell Tomes
            "Tome of Fire": 10000000,
            "Tome of Water": 10000000,
            "Tome of Nature": 10000000,
            "Tome of Mana Shield": 15000000,
            # Herbs
            "Sunpetal": 5000000,
            "Sageroot": 7000000,
            "Bloomwell": 12000000,
        }
        
        for item_id, item_name in tradeable_items.items():
            base_price = base_prices.get(item_name, random.randint(500000, 100000000))
            history["item_prices"][item_name] = {"prices": []}
            
            for i in range(hours_back):
                timestamp = (now - timedelta(hours=i)).isoformat()
                
                # Simulate realistic price fluctuations
                price_variation = 1 + random.uniform(-0.15, 0.15)  # ±15% variation
                time_trend = 1 + (random.uniform(-0.002, 0.002) * i)  # Small random trend
                
                current_price = int(base_price * price_variation * time_trend)
                buy_price = current_price
                sell_price = int(current_price * random.uniform(1.02, 1.12))  # 2-12% spread
                
                history["item_prices"][item_name]["prices"].insert(0, {
                    "timestamp": timestamp,
                    "buy": buy_price,
                    "sell": sell_price
                })
        
        return history

    def generate_baseline(self, guilds: List[Dict]) -> Dict:
        """Generate baseline data for daily progress calculation."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        baseline_guilds = {}
        for guild in guilds:
            # Baseline should be lower than current levels to show positive progress
            baseline_guilds[guild["GuildName"]] = {
                "NexusLevel": max(0, guild["NexusLevel"] - random.randint(2, 10)),
                "StudyLevel": max(0, guild["StudyLevel"] - random.randint(1, 8))
            }
        
        return {
            "date": today,
            "guilds": baseline_guilds
        }

    def calculate_average_codex_price(self, history: Dict) -> float:
        """Calculate realistic average Codex price from historical data."""
        codex_prices = history.get('item_prices', {}).get('Codex', {}).get('prices', [])
        if not codex_prices:
            return 10000000000
        
        # Use last 24 price points for average
        recent_prices = codex_prices[-24:] if len(codex_prices) >= 24 else codex_prices
        total_avg = sum((p['buy'] + p['sell']) / 2 for p in recent_prices)
        return total_avg / len(recent_prices)

    def generate_mock_data(self):
        """Generate all mock data files matching guild-stats.py output format."""
        print(" Generating mock data for local testing...")
        
        # Generate current guild data
        current_guilds = self.generate_guild_data()
        print(f"Generated {len(current_guilds)} mock guilds")
        
        # Generate historical data (72 hours of data points)
        historical_data = self.generate_historical_data(current_guilds, 72)
        
        # Save historical data first
        with open(HISTORICAL_FILE, 'w') as f:
            json.dump(historical_data, f, indent=2)
        print("Generated historical data for charts")
        
        # Generate baseline
        baseline = self.generate_baseline(current_guilds)
        with open(BASELINE_FILE, 'w') as f:
            json.dump(baseline, f, indent=2)
        print("Generated baseline data")
        
        # Calculate progress and codex costs
        total_codex = 0
        for guild in current_guilds:
            base = baseline["guilds"][guild["GuildName"]]
            guild["NexusProgress"] = guild["NexusLevel"] - base["NexusLevel"]
            guild["StudyProgress"] = guild["StudyLevel"] - base["StudyLevel"]
            guild["TotalCodexCost"] = (
                self.calculate_codex_cost(base["NexusLevel"], guild["NexusProgress"]) +
                self.calculate_codex_cost(base["StudyLevel"], guild["StudyProgress"])
            )
            total_codex += guild["TotalCodexCost"]
        
        # Calculate dust spending using average Codex price
        avg_price = self.calculate_average_codex_price(historical_data)
        
        dust_spending = {
            "total_codex": total_codex,
            "formatted_dust": self.format_currency(total_codex * avg_price),
            "formatted_price": self.format_currency(avg_price)
        }
        
        # Generate final guild data file
        final_data = {
            "guilds": current_guilds,  # Already sorted by NexusLevel
            "dustSpending": dust_spending,
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "baselineDate": baseline["date"],
            "dataFreshness": {
                "guild_data_fresh": True,
                "market_data_fresh": True
            }
        }
        
        with open(GUILD_DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)
        
        print(" Mock data generation complete!")
        print(f" Dashboard: {len(current_guilds)} guilds with progress tracking")
        print(f" Historical: {len(historical_data['guild_history'])} guild histories")
        print(f" Market: {len(historical_data['item_prices'])} item price histories")
        print(f" Total Codex Used: {total_codex:,}")
        print(f" Dust Spent: {dust_spending['formatted_dust']}")
        print(f" Item Categories: {len(historical_data['item_categories'])} categories")
        print("\n Open docs/index.html to view the mock dashboard!")

def main():
    """Generate mock data for local testing only."""
    # Safety check to prevent running in production
    if os.environ.get('GITHUB_ACTIONS') or os.environ.get('CI'):
        print(" Mock data generation disabled in CI/production environment")
        return
    
    generator = MockDataGenerator()
    generator.generate_mock_data()

if __name__ == "__main__":
    main()