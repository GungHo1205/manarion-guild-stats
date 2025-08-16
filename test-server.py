#!/usr/bin/env python3
"""
Windows-compatible script to set up a local testing environment.
- Creates a 'staging' directory with a visual banner.
- Generates mock combined historical data for both guild and market data.
- Creates proper baseline files to prevent errors.
"""

import os
import shutil
import sys
import json
import random
from datetime import datetime, timedelta, timezone

def safe_print(message):
    """Print with Unicode fallback for Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        message = message.replace('🧪', '[TEST]').replace('📁', '[FOLDER]').replace('📄', '[FILE]')
        message = message.replace('✅', '[OK]').replace('⚠️', '[WARN]').replace('❌', '[ERROR]')
        message = message.replace('🎨', '[STYLE]').replace('🚀', '[ROCKET]').replace('🌐', '[WEB]')
        message = message.replace('🔍', '[SEARCH]').replace('🚧', '[CONSTRUCTION]').replace('📈', '[CHART]')
        message = message.replace('💰', '[MONEY]').replace('📊', '[DATA]')
        print(message)

def create_empty_baseline_files():
    """Creates minimal baseline files that prevent errors in production."""
    safe_print("\n📊 Creating production-safe baseline files...")
    
    # Minimal guild data structure
    minimal_guild_data = {
        "guilds": [],
        "dustSpending": {
            "formatted_dust": "0",
            "formatted_price": "0",
            "total_dust": 0,
            "average_price": 0
        },
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "baselineDate": datetime.now(timezone.utc).strftime("%Y-%m-%d")
    }
    
    # Minimal combined historical data
    minimal_historical_data = {
        "guild_history": {},
        "market_prices": {
            "prices": [],
            "daily_averages": {}
        },
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    
    # Minimal baseline structure
    minimal_baseline = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "guilds": {}
    }
    
    # Create files in docs directory for production
    docs_dir = "docs"
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)
        safe_print(f"📁 Created '{docs_dir}' directory")
    
    files_to_create = [
        ("guild-data.json", minimal_guild_data),
        ("historical-data.json", minimal_historical_data),
        ("daily-baseline.json", minimal_baseline)
    ]
    
    for filename, data in files_to_create:
        file_path = os.path.join(docs_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        safe_print(f"✅ Created minimal '{filename}' in docs/")

def create_baseline_files():
    """Creates proper baseline files for testing."""
    safe_print("\n📊 Creating baseline files...")
    
    # Create some realistic guild names and levels
    mock_guilds = {
        "Elite Dragons": {"StudyLevel": 150, "NexusLevel": 120},
        "Shadow Legends": {"StudyLevel": 145, "NexusLevel": 115},
        "Phoenix Rising": {"StudyLevel": 140, "NexusLevel": 110},
        "Storm Riders": {"StudyLevel": 135, "NexusLevel": 105},
        "Mystic Warriors": {"StudyLevel": 130, "NexusLevel": 100},
        "Iron Wolves": {"StudyLevel": 125, "NexusLevel": 95},
        "Crystal Guardians": {"StudyLevel": 120, "NexusLevel": 90},
        "Thunder Hawks": {"StudyLevel": 115, "NexusLevel": 85},
        "Void Seekers": {"StudyLevel": 110, "NexusLevel": 80},
        "Flame Bringers": {"StudyLevel": 105, "NexusLevel": 75}
    }
    
    baseline_data = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "guilds": mock_guilds
    }
    
    # Create guild-data.json (current data file)
    guild_data = {
        "guilds": [
            {
                "GuildName": name,
                "NexusLevel": levels["NexusLevel"] + random.randint(0, 5),
                "StudyLevel": levels["StudyLevel"] + random.randint(0, 5),
                "NexusProgress": random.randint(0, 3),
                "StudyProgress": random.randint(0, 4),
                "NexusCodexCost": random.randint(100, 500),
                "StudyCodexCost": random.randint(150, 600),
                "TotalCodexCost": random.randint(250, 1100)
            }
            for name, levels in mock_guilds.items()
        ],
        "dustSpending": {
            "formatted_dust": "2.5B",
            "formatted_price": "16.8B",
            "total_dust": 2500000000,
            "average_price": 16800000000
        },
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "baselineDate": datetime.now(timezone.utc).strftime("%Y-%m-%d")
    }
    
    # Create baseline file in staging
    staging_dir = "staging"
    if os.path.exists(staging_dir):
        baseline_file_path = os.path.join(staging_dir, "daily-baseline.json")
        with open(baseline_file_path, 'w', encoding='utf-8') as f:
            json.dump(baseline_data, f, indent=2)
        
        # Create guild data file
        guild_data_path = os.path.join(staging_dir, "guild-data.json")
        with open(guild_data_path, 'w', encoding='utf-8') as f:
            json.dump(guild_data, f, indent=2)
        
        safe_print(f"✅ Baseline files created in '{staging_dir}/'")

def generate_combined_historical_data():
    """Generates combined historical data for both guilds and market prices."""
    safe_print("\n📈 Generating combined historical data for charts...")
    
    # Mock guilds for testing
    mock_guilds = {
        "Elite Dragons": {"StudyLevel": 150, "NexusLevel": 120},
        "Shadow Legends": {"StudyLevel": 145, "NexusLevel": 115},
        "Phoenix Rising": {"StudyLevel": 140, "NexusLevel": 110},
        "Storm Riders": {"StudyLevel": 135, "NexusLevel": 105},
        "Mystic Warriors": {"StudyLevel": 130, "NexusLevel": 100},
        "Iron Wolves": {"StudyLevel": 125, "NexusLevel": 95},
        "Crystal Guardians": {"StudyLevel": 120, "NexusLevel": 90},
        "Thunder Hawks": {"StudyLevel": 115, "NexusLevel": 85},
        "Void Seekers": {"StudyLevel": 110, "NexusLevel": 80},
        "Flame Bringers": {"StudyLevel": 105, "NexusLevel": 75}
    }

    now = datetime.now(timezone.utc)
    total_hours = 30 * 24  # 30 days of hourly data
    
    # Initialize the combined historical data structure
    historical_data = {
        "guild_history": {},
        "market_prices": {
            "prices": [],
            "daily_averages": {}
        },
        "last_updated": now.isoformat()
    }

    # Generate guild history data
    for guild_name, levels in mock_guilds.items():
        historical_data["guild_history"][guild_name] = []
        
        # Start with levels slightly lower than the baseline
        current_nexus = levels["NexusLevel"] - random.randint(15, 30)
        current_study = levels["StudyLevel"] - random.randint(15, 30)

        for i in range(total_hours):
            # Go back in time hour by hour
            timestamp = now - timedelta(hours=total_hours - i)
            
            # Occasionally increase the level to show progression
            if random.random() < 0.1:  # 10% chance to level up each hour
                current_nexus += 1
            if random.random() < 0.15:  # 15% chance
                current_study += 1

            entry = {
                "timestamp": timestamp.isoformat(),
                "nexus": max(0, current_nexus),
                "study": max(0, current_study)
            }
            historical_data["guild_history"][guild_name].append(entry)

    # Generate market price data
    daily_averages = {}
    
    # Starting prices (realistic codex market values)
    base_buy_price = 15_000_000_000  # 15B
    base_sell_price = 18_000_000_000  # 18B
    
    current_buy = base_buy_price
    current_sell = base_sell_price
    
    for i in range(total_hours):
        # Go back in time hour by hour
        timestamp = now - timedelta(hours=total_hours - i)
        
        # Add some realistic market volatility
        # Buy price fluctuates ±5% from base with some trending
        buy_volatility = random.uniform(-0.05, 0.05)
        trend_factor = 0.1 * random.random() * (i / total_hours)  # Slight upward trend over time
        current_buy = base_buy_price * (1 + buy_volatility + trend_factor)
        
        # Sell price is always higher than buy price, with similar volatility
        sell_volatility = random.uniform(-0.05, 0.05)
        current_sell = base_sell_price * (1 + sell_volatility + trend_factor)
        
        # Ensure sell > buy
        if current_sell <= current_buy:
            current_sell = current_buy * 1.1
        
        average_price = (current_buy + current_sell) / 2
        
        price_entry = {
            "timestamp": timestamp.isoformat(),
            "buy_price": int(current_buy),
            "sell_price": int(current_sell),
            "average_price": int(average_price)
        }
        historical_data["market_prices"]["prices"].append(price_entry)
        
        # Calculate daily averages
        date_str = timestamp.strftime("%Y-%m-%d")
        if date_str not in daily_averages:
            daily_averages[date_str] = {
                "buy_prices": [],
                "sell_prices": [],
                "avg_prices": []
            }
        
        daily_averages[date_str]["buy_prices"].append(current_buy)
        daily_averages[date_str]["sell_prices"].append(current_sell)
        daily_averages[date_str]["avg_prices"].append(average_price)
    
    # Calculate final daily averages
    def format_currency(amount):
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
    
    final_daily_averages = {}
    for date_str, day_data in daily_averages.items():
        avg_buy = sum(day_data["buy_prices"]) / len(day_data["buy_prices"])
        avg_sell = sum(day_data["sell_prices"]) / len(day_data["sell_prices"])
        daily_avg = sum(day_data["avg_prices"]) / len(day_data["avg_prices"])
        
        final_daily_averages[date_str] = {
            "average_price": daily_avg,
            "buy_average": avg_buy,
            "sell_average": avg_sell,
            "sample_count": len(day_data["avg_prices"]),
            "formatted_price": format_currency(daily_avg)
        }
    
    historical_data["market_prices"]["daily_averages"] = final_daily_averages

    # Save the combined historical data to staging directory only
    staging_dir = "staging"
    if os.path.exists(staging_dir):
        historical_file_path = os.path.join(staging_dir, "historical-data.json")
        with open(historical_file_path, 'w', encoding='utf-8') as f:
            json.dump(historical_data, f, indent=2)
        safe_print(f"✅ Combined 'historical-data.json' created in '{staging_dir}/' with:")
        safe_print(f"   - {len(historical_data['guild_history'])} guilds with historical data")
        safe_print(f"   - {len(historical_data['market_prices']['prices'])} market price points")
        safe_print(f"   - {len(final_daily_averages)} days of price averages")

def main():
    """Main function to run the staging setup."""
    safe_print("🧪 Testing Staging Setup")
    safe_print("========================")
    
    # First create empty baseline files for production safety
    create_empty_baseline_files()
    
    # Create staging directory
    staging_dir = "staging"
    safe_print(f"📁 Creating '{staging_dir}' directory...")
    if not os.path.exists(staging_dir):
        os.makedirs(staging_dir)
    
    # Create baseline files for testing
    create_baseline_files()
    
    # Copy any existing docs files to staging (excluding the ones we're about to generate)
    docs_dir = "docs"
    if os.path.exists(docs_dir):
        try:
            for filename in os.listdir(docs_dir):
                if filename not in ['historical-data.json', 'daily-baseline.json', 'guild-data.json']:
                    src = os.path.join(docs_dir, filename)
                    dst = os.path.join(staging_dir, filename)
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)
            safe_print(f"✅ Copied existing files from '{docs_dir}' to '{staging_dir}'")
        except Exception as e:
            safe_print(f"⚠️ Error copying files: {e}")
    
    # Generate combined mock data for testing
    generate_combined_historical_data()

    # Copy index.html and modify it for staging
    index_src = "docs/index.html"
    index_staging = os.path.join(staging_dir, "index.html")
    
    if os.path.exists(index_src):
        safe_print("\n🎨 Adding staging visual indicators...")
        shutil.copy2(index_src, index_staging)
        
        try:
            with open(index_staging, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Modify title
            content = content.replace('<title>Guild Stats Dashboard</title>', '<title>[STAGING] Guild Stats Dashboard</title>')
            
            # Add staging CSS variables
            css_addition = '''        --staging-bg: linear-gradient(45deg, #ff6b35, #f7931e);
        --staging-text: #ffffff;
        --staging-border: #ff6b35;'''
            content = content.replace('--gradient-3: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);', 
                                    '--gradient-3: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);' + css_addition)
            
            # Add staging banner
            banner_html = '''        <div style="background: var(--staging-bg); margin: -40px -25px 20px -25px; padding: 12px 0; border-radius: 15px 15px 0 0; text-align: center; border: 2px solid var(--staging-border);">
          <span style="color: var(--staging-text); font-size: 1rem; font-weight: 700; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">🚧 STAGING ENVIRONMENT - TEST DATA 🚧</span>
        </div>'''
            content = content.replace('<header class="header">', '<header class="header">\n' + banner_html)
            
            with open(index_staging, 'w', encoding='utf-8') as f:
                f.write(content)
            safe_print("✅ Staging modifications applied to 'index.html'")
            
        except Exception as e:
            safe_print(f"❌ Error modifying index.html: {e}")
            return False
    else:
        safe_print(f"❌ No 'index.html' found in root directory")
        return False

    safe_print("\n🚀 Test Setup Complete!")
    safe_print("========================")
    safe_print("🌐 To test locally:")
    safe_print("1. Run a local server: python -m http.server 8000")
    safe_print("2. Visit: http://localhost:8000 (production version with empty data)")
    safe_print("3. Visit: http://localhost:8000/staging/ (staging version with test data)")
    safe_print("\n   Production version (docs/) now has:")
    safe_print("   - Empty but valid JSON files to prevent errors")
    safe_print("   - Safe deployment-ready structure")
    safe_print("   - Combined historical data structure")
    safe_print("\n   Staging version (staging/) has:")
    safe_print("   - Complete guild data with progress tracking")
    safe_print("   - Guild Progress Charts with 30 days of mock data")
    safe_print("   - Codex Price Charts with realistic market fluctuations")
    safe_print("   - Hourly/Daily interval switching")
    safe_print("   - All time ranges (1D, 3D, 7D, 14D, 30D) functional")
    safe_print("   - Current price display with daily change calculations")
    safe_print("\n   Chart features:")
    safe_print("   - Smart interval switching (hourly < 7 days, daily > 6 days)")
    safe_print("   - Manual interval override controls")
    safe_print("   - Buy/Sell/Average price lines for codex")
    safe_print("   - Proper currency formatting (B/T notation)")
    safe_print("   - Combined data structure for efficiency")
    safe_print("\n   Production safety:")
    safe_print("   - docs/ contains minimal valid files")
    safe_print("   - No errors when deployed without real data")
    safe_print("   - Ready for your actual API integration")
    safe_print("   - Efficient single JSON file for historical data")
    
    return True

if __name__ == "__main__":
    if not main():
        sys.exit(1)