#---Update Training Data with results

import pandas as pd
from supabase import create_client, Client
import traceback

# -------------------------
# Supabase Setup
# -------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)

# -------------------------
# Step 1: Load rows needing results
# -------------------------
response = supabase.table("training_data").select("*").or_(
    "home_score.is.null,away_score.is.null,ou_result.is.null,run_line_winner.is.null,ha_winner.is.null"
).execute()

rows = response.data
if not rows:
    print("✅ No rows found needing results.")
else:
    print(f"🔍 Found {len(rows)} rows needing results.")

# -------------------------
# Step 2: Team Table Mapping
# -------------------------
team_to_table = {
    "Angels": "angels_games",
    "Arizona": "arizona_games",
    "Athletics": "athletics_games",
    "Atlanta": "atlanta_games",
    "Baltimore": "baltimore_games",
    "Boston": "boston_games",
    "Cincinnati": "cincinnati_games",
    "Cleveland": "cleveland_games",
    "Colorado": "colorado_games",
    "Cubs": "cubs_games",
    "Detroit": "detroit_games",
    "Dodgers": "dodgers_games",
    "Houston": "houston_games",
    "Kansas City": "kansas_city_games",
    "Mets": "mets_games",
    "Miami": "miami_games",
    "Milwaukee": "milwaukee_games",
    "Minnesota": "minnesota_games",
    "Yankees": "yankees_games",
    "Oakland": "oakland_games",
    "Philadelphia": "philadelphia_games",
    "Pittsburgh": "pittsburgh_games",
    "San Diego": "san_diego_games",
    "San Francisco": "san_francisco_games",
    "Seattle": "seattle_games",
    "ST Louis": "st_louis_games",
    "Tampa Bay": "tampa_bay_games",
    "Texas": "texas_games",
    "Toronto": "toronto_games",
    "Washington": "washington_games",
    "White Sox": "white_sox_games"
}

updated_rows = []

# -------------------------
# Step 3: Process Each Row
# -------------------------
for row in rows:
    home_team = row["home_team"]
    table_name = team_to_table.get(home_team)

    if not table_name:
        print(f"❌ No mapping for {home_team}")
        continue

    try:
        team_games = supabase.table(table_name).select("join_table_string, team_score, opponent_score").execute().data
        game = next((g for g in team_games if g["join_table_string"] == row["unique_id"]), None)

        if not game:
            print(f"⚠️ No data found in {table_name} for {row['unique_id']}")
            continue

        home_score = game.get("team_score")
        away_score = game.get("opponent_score")

        if home_score is None or away_score is None:
            print(f"⚠️ Scores missing for {row['unique_id']} in {table_name}")
            continue

        # Derived results
        ou_result = 1 if home_score + away_score >= row["o_u_line"] else 0
        run_line = row["home_rl"]
        run_line_winner = 1 if (run_line < 0 and home_score > away_score + abs(run_line)) or \
                                (run_line > 0 and home_score + run_line > away_score) else 0
        ha_winner = 1 if home_score > away_score else 0

        update_payload = {
            "home_score": home_score,
            "away_score": away_score,
            "ou_result": ou_result,
            "run_line_winner": run_line_winner,
            "ha_winner": ha_winner
        }

        supabase.table("training_data").update(update_payload).eq("unique_id", row["unique_id"]).execute()
        updated_rows.append(row["unique_id"])

    except Exception as e:
        print(f"❌ Error accessing {table_name} for {row['unique_id']}: {e}")
        traceback.print_exc()

print(f"✅ Updated {len(updated_rows)} rows with results.") 
