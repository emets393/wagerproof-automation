# ---- Import Input Value View Data into Training Data ----

import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# ----------------------------- Supabase Setup -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)

# ----------------------------- Delete Today's Rows from Training Data -----------------------------
today_str = datetime.today().strftime('%Y-%m-%d')
print(f"🧹 Deleting existing rows in training_data for {today_str}...")
supabase.table("training_data").delete().eq("date", today_str).execute()

# ----------------------------- Load Input View -----------------------------
print("📥 Loading data from input_values_view...")
df_input_raw = supabase.table("input_values_view").select("*").execute().data
df_input = pd.DataFrame(df_input_raw)

if df_input.empty:
    print("⚠️ input_values_view returned no rows. Exiting.")
    exit()

# ----------------------------- Prepare Rows -----------------------------
training_rows = []

for _, row in df_input.iterrows():
    home_team = row["home_team"].lower()
    unique_id = row["unique_id"]
    team_table = f"{home_team}_games"

    try:
        game_data = supabase.table(team_table).select("*").eq("unique_id", unique_id).execute().data
    except Exception:
        game_data = []

    home_score = away_score = ou_result = run_line_winner = ha_winner = None

    if game_data:
        game = game_data[0]
        home_score = game.get("team_score")
        away_score = game.get("opponent_score")

        try:
            if pd.notnull(home_score) and pd.notnull(away_score):
                ou_result = int((home_score + away_score) >= row["o_u_line"])
                run_line_winner = int((home_score - away_score) > -row["home_rl"])
                ha_winner = int(home_score > away_score)
        except Exception:
            pass

    row_dict = row.drop(labels=["excel_date"]).to_dict()

    # Clean up for Supabase compatibility
    for key, value in row_dict.items():
        if pd.isna(value):
            row_dict[key] = None
        elif isinstance(value, float) and value.is_integer():
            row_dict[key] = int(value)

    row_dict.update({
        "home_score": home_score,
        "away_score": away_score,
        "ou_result": ou_result,
        "run_line_winner": run_line_winner,
        "ha_winner": ha_winner,
        "data_source": "input values"
    })

    training_rows.append(row_dict)

# ----------------------------- Insert New Rows -----------------------------
if training_rows:
    supabase.table("training_data").insert(training_rows).execute()
    print(f"✅ Inserted {len(training_rows)} new rows into training_data.")
else:
    print("⚠️ No new rows to insert.")