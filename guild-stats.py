import requests
import csv
import time
import json
import os
from datetime import datetime, timezone

BASE_URL = "https://api.manarion.com"
LEADERBOARD_TYPE = "boost_damage"

BASE_PER_UPGRADE = 0.02  # 0.02% per upgrade

def get_leaderboard_page(page):
    url = f"{BASE_URL}/leaderboards/{LEADERBOARD_TYPE}?page={page}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def get_guilds_page():
    url = f"{BASE_URL}/guilds"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def get_player_profile(name):
    url = f"{BASE_URL}/players/{name}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def calculate_nexus_level(research_damage_percent, upgrades):
    base_pct = 0.02  # 0.02 percent per upgrade (not decimal)
    if upgrades <= 0:
        return None
    multiplier = research_damage_percent / (upgrades * base_pct)
    L = 100 * (multiplier - 1.0)
    return round(L)

def calculate_study_room_level(total_exp_boost, codex_boost, enchant_boost):
    return total_exp_boost - codex_boost - enchant_boost

def safe_get_infusions_count(infusions_data):
    """Safely extract infusion count from infusions data"""
    if isinstance(infusions_data, dict):
        # If it's a dict, sum up all infusion values
        return sum(v for v in infusions_data.values() if isinstance(v, (int, float)))
    elif isinstance(infusions_data, (int, float)):
        # If it's already a number, return it
        return infusions_data
    else:
        # If it's something else, return 0
        return 0

def load_baseline_data():
    """Load the daily baseline data if it exists"""
    baseline_file = "docs/daily-baseline.json"
    if os.path.exists(baseline_file):
        try:
            with open(baseline_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def save_baseline_data(guilds_data, timestamp):
    """Save current data as the daily baseline"""
    baseline_data = {
        "date": timestamp.split('T')[0],  # Just the date part
        "timestamp": timestamp,
        "guilds": {}
    }
    
    for guild in guilds_data:
        guild_name = guild["GuildName"]
        baseline_data["guilds"][guild_name] = {
            "StudyLevel": guild["StudyLevel"],
            "NexusLevel": guild["NexusLevel"]
        }
    
    os.makedirs("docs", exist_ok=True)
    with open("docs/daily-baseline.json", 'w', encoding='utf-8') as f:
        json.dump(baseline_data, f, indent=2)
    
    print(f"Saved baseline data for {len(guilds_data)} guilds")

def is_midnight_run():
    """Check if this is the midnight baseline run"""
    current_hour = datetime.now(timezone.utc).hour
    return current_hour == 0

def calculate_daily_progress(current_guilds, baseline_data):
    """Calculate progress since daily baseline"""
    if not baseline_data:
        return current_guilds
    
    baseline_guilds = baseline_data.get("guilds", {})
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    baseline_date = baseline_data.get("date", "")
    
    # If baseline is not from today, don't show progress
    if baseline_date != today:
        for guild in current_guilds:
            guild["StudyProgress"] = None
            guild["NexusProgress"] = None
        return current_guilds
    
    # Calculate progress for each guild
    for guild in current_guilds:
        guild_name = guild["GuildName"]
        if guild_name in baseline_guilds:
            baseline = baseline_guilds[guild_name]
            guild["StudyProgress"] = guild["StudyLevel"] - baseline["StudyLevel"]
            guild["NexusProgress"] = guild["NexusLevel"] - baseline["NexusLevel"]
        else:
            # New guild not in baseline
            guild["StudyProgress"] = None
            guild["NexusProgress"] = None
    
    return current_guilds

def main():
    results = []
    current_time = datetime.now(timezone.utc)
    timestamp = current_time.isoformat()

    for page in range(1, 2):  # STOP at page 2 (so only page 1)
        try:
            leaderboard_data = get_leaderboard_page(page)
        except Exception as e:
            print(f"Error getting leaderboard page {page}: {e}")
            break
            
        if "Entries" not in leaderboard_data:
            print(f"No 'Entries' in page {page}")
            break
            
        x = 0
        for entry in leaderboard_data["Entries"]:
            x += 1
            print(f"Processing entry {x}")
            if x == 200:
                break
                
            player_name = entry.get("Name") or entry.get("name")
            upgrades = entry.get("Score", 0) or entry.get("score", 0)
            if not player_name:
                continue

            try:
                player_data = get_player_profile(player_name)
                time.sleep(1)
            except Exception as e:
                print(f"Error getting profile for {player_name}: {e}")
                continue

            if not player_data:
                continue

            guild_id = player_data.get("GuildID", 0)
            guild_name = ""
            
            try:
                guild_data = get_guilds_page()
                for guild in guild_data:
                    if guild_id == guild.get("ID", 0):
                        guild_name = guild.get("Name", "")
                        break
            except Exception as e:
                print(f"Error getting guild data: {e}")
                guild_name = "Unknown"

            codex_exp_boost = player_data.get("BaseBoosts", {}).get("100", 0)
            total_exp_boost = player_data.get("TotalBoosts", {}).get("100", 0)
            total_damage_percent = player_data.get("TotalBoosts", {}).get("40", 0) * 100
            equipments = player_data.get("Equipment", {})
            
            totalEquipmentBoosts = 0
            enchant_boost = 0
            
            for item in range(1, 9):
                try:
                    if item == 5:
                        enchant_boost = equipments.get(str(item), {}).get("Boosts", {}).get("100", 0)
                    
                    infusions_raw = equipments.get(str(item), {}).get("Infusions", {})
                    infusions_count = safe_get_infusions_count(infusions_raw)
                    
                    base_boost = equipments.get(str(item), {}).get("Boosts", {}).get("40", 0)
                    
                    equip_percent = (base_boost * (1 + 0.05 * infusions_count)) / 50
                    totalEquipmentBoosts += equip_percent
                    
                except Exception as e:
                    print(f"Error processing equipment item {item} for {player_name}: {e}")
                    continue
            
            base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
            
            # Derive Nexus level from formula
            study_level = calculate_study_room_level(total_exp_boost, codex_exp_boost, enchant_boost)
            nexus_level = calculate_nexus_level(base_damage_percent, upgrades)
            
            # Only add unique guild names
            if not any(r["GuildName"] == guild_name for r in results):
                results.append({
                    "GuildName": guild_name or "Unknown",
                    "StudyLevel": study_level,
                    "NexusLevel": nexus_level if nexus_level is not None else 0,
                })
                print(f"Added guild: {guild_name}, Study: {study_level}, Nexus: {nexus_level}")

    if results:
        results.sort(key=lambda x: x["NexusLevel"], reverse=True)
        print(f"Final results: {len(results)} guilds")
        
        # Load baseline data for progress calculation
        baseline_data = load_baseline_data()
        
        # If this is midnight (00:00 UTC), save current data as baseline
        if is_midnight_run():
            print("Midnight run detected - saving baseline data")
            save_baseline_data(results, timestamp)
            baseline_data = load_baseline_data()  # Reload the fresh baseline
        
        # Calculate daily progress
        results_with_progress = calculate_daily_progress(results, baseline_data)
        
        # Ensure docs directory exists
        os.makedirs("docs", exist_ok=True)
        
        # Save CSV (for backward compatibility)
        with open("guild_stats.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        
        # Prepare data for JSON (including progress)
        json_data = {
            "lastUpdated": timestamp,
            "baselineDate": baseline_data.get("date", None) if baseline_data else None,
            "baselineTimestamp": baseline_data.get("timestamp", None) if baseline_data else None,
            "guilds": results_with_progress
        }
        
        # Save JSON for website
        with open("docs/guild-data.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)
        
        print("Files saved successfully")
        
        # Print progress summary
        if baseline_data and baseline_data.get("date") == timestamp.split('T')[0]:
            total_study_gains = sum(g.get("StudyProgress", 0) or 0 for g in results_with_progress)
            total_nexus_gains = sum(g.get("NexusProgress", 0) or 0 for g in results_with_progress)
            print(f"Daily progress: Study +{total_study_gains}, Nexus +{total_nexus_gains}")
        
    else:
        print("No results to save.")

if __name__ == "__main__":
    main()