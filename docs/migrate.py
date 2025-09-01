import json
import sqlite3
import os

def populate_database():
    """
    Populates the guild-stats.db with historical data from historical-data.json.
    """
    # Define file paths
    db_file = 'guild-stats.db'
    json_file = 'historical-data.json'

    # Check if files exist
    if not os.path.exists(db_file):
        print(f"Error: Database file '{db_file}' not found.")
        return
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' not found.")
        return

    # --- 1. Read Historical Data ---
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        historical_data = data.get('guild_history', {})
        if not historical_data:
            print("Warning: 'guild_history' key not found in JSON or is empty.")
            return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file}'.")
        return
    except Exception as e:
        print(f"An error occurred while reading the JSON file: {e}")
        return

    # --- 2. Connect to Database ---
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to the database: {e}")
        return

    inserted_count = 0
    skipped_count = 0

    # --- 3. & 4. Check for Existing Data and Insert Missing Data ---
    print("Starting database update...")
    for guild_name, records in historical_data.items():
        for record in records:
            timestamp = record.get('timestamp')
            nexus_level = record.get('nexus')
            study_level = record.get('study')

            # Basic validation
            if not all([timestamp, nexus_level is not None, study_level is not None]):
                print(f"Skipping malformed record for guild '{guild_name}': {record}")
                skipped_count += 1
                continue

            try:
                # Check if the record already exists
                cursor.execute(
                    "SELECT 1 FROM guild_snapshots WHERE guild_name = ? AND timestamp = ?",
                    (guild_name, timestamp)
                )
                exists = cursor.fetchone()

                # If it doesn't exist, insert it
                if not exists:
                    cursor.execute(
                        "INSERT INTO guild_snapshots (timestamp, guild_name, nexus_level, study_level) VALUES (?, ?, ?, ?)",
                        (timestamp, guild_name, nexus_level, study_level)
                    )
                    inserted_count += 1

            except sqlite3.Error as e:
                print(f"A database error occurred for guild '{guild_name}' at '{timestamp}': {e}")
                skipped_count += 1


    # --- 5. Commit changes and close the connection ---
    try:
        conn.commit()
        print("\nUpdate complete.")
        print(f"Successfully inserted {inserted_count} new records.")
        if skipped_count > 0:
            print(f"Skipped {skipped_count} malformed or problematic records.")
    except sqlite3.Error as e:
        print(f"Error committing changes to the database: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    # This block allows the script to be run from the command line.
    # In this environment, the function is called directly.
    # To run this, you would save it as a .py file and execute `python your_script_name.py`
    # in a terminal where the .db and .json files are present.
    populate_database()

