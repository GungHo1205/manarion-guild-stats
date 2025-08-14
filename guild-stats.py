import requests
import csv
import time

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

def main():
    results = []

    for page in range(1, 2):  # STOP at page 3
        leaderboard_data = get_leaderboard_page(page)
        if "Entries" not in leaderboard_data:
            print(f"No 'Entries' in page {page}")
            break
        x = 0
        for entry in leaderboard_data["Entries"]:
            x +=1
            print(x)
            if(x == 200):
                break
            player_name = entry.get("Name") or entry.get("name")
            upgrades = entry.get("Score", 0) or entry.get("score", 0)
            if not player_name:
                continue

            player_data = get_player_profile(player_name)
            time.sleep(3)

            if not player_data:
                continue

            guild_id = player_data.get("GuildID", 0)
            guild_data = get_guilds_page()
            for guild in guild_data:
                if(guild_id == guild.get("ID", 0)):
                    guild_name = guild.get("Name", "")
                    break
            codex_exp_boost = player_data.get("BaseBoosts", {}).get("100", 0)
            total_exp_boost = player_data.get("TotalBoosts", {}).get("100", 0)
            total_damage_percent = player_data.get("TotalBoosts", {}).get("40", 0) * 100
            equipments = player_data.get("Equipment", {})
            totalEquipmentBoosts = 0
            for item in range(1,9):
                if(item == 5):
                    enchant_boost = equipments.get(str(item), {}).get("Boosts", {}).get("100", 0)
                infusions = equipments.get(str(item), {}).get("Infusions", {})
                base_boost = equipments.get(str(item), {}).get("Boosts", {}).get("40", 0)
                equip_percent = (base_boost * (1 + 0.05 * infusions)) / 50
                totalEquipmentBoosts += equip_percent
            base_damage_percent = total_damage_percent - totalEquipmentBoosts - 100
            
            # Derive Nexus level from formula
            study_level = calculate_study_room_level(total_exp_boost, codex_exp_boost, enchant_boost)
            nexus_level = calculate_nexus_level(base_damage_percent, upgrades)
            # print(results)
            if not any(r["GuildName"] == guild_name for r in results):
                results.append({
                    "GuildName": guild_name or "",
                    "StudyLevel": study_level,
                    "NexusLevel": nexus_level,
                })

    if results:
        results.sort(key=lambda x: x["NexusLevel"], reverse=True)
        print(results)
        with open("guild_stats.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print("CSV saved as guild_stats.csv")
    else:
        print("No results to save.")

if __name__ == "__main__":
    main()
