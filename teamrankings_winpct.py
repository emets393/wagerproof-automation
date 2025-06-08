import pandas as pd
from supabase import create_client, Client

# -----------------------------
# Supabase Setup
# -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)
TABLE_NAME = "teamrankings_win_pct"

# -----------------------------
# Fetch team name mapping
# -----------------------------
mapping_data = supabase.table("MLB_Teams").select("TeamRankingsName", "short_name").execute().data
mapping_df = pd.DataFrame(mapping_data)
team_map = dict(zip(mapping_df["TeamRankingsName"], mapping_df["short_name"]))

# -----------------------------
# Scrape 2025 Win % from TeamRankings
# -----------------------------
url = "https://www.teamrankings.com/mlb/stat/win-pct-all-games"
df = pd.read_html(url)[0]

# Select only Team name and 2025 column (index 1 and 2)
df = df.iloc[:, [1, 2]].copy()
df.columns = ["TeamRankingsName", "win_pct"]

# Map TeamRankingsName to short_name
df["team"] = df["TeamRankingsName"].map(team_map)

# Keep only rows where mapping worked and keep only cleaned columns
df = df.dropna(subset=["team"])[["team", "win_pct"]]

# Convert win_pct to numeric (just in case)
df["win_pct"] = pd.to_numeric(df["win_pct"], errors="coerce")

# -----------------------------
# Upload to Supabase
# -----------------------------
# Delete existing records
supabase.table(TABLE_NAME).delete().neq("team", "").execute()

# Insert new data
records = df.to_dict(orient="records")
supabase.table(TABLE_NAME).insert(records).execute()

print("✅ Uploaded 2025 win_pct data to Supabase.")

