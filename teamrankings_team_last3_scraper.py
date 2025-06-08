import pandas as pd
from supabase import create_client, Client

# -----------------------------
# Supabase Setup
# -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)
TABLE_NAME = "teamrankings_team_last3"

# -----------------------------
# Fetch team mapping
# -----------------------------
mapping_data = supabase.table("MLB_Teams").select("TeamRankingsName", "short_name").execute().data
mapping_df = pd.DataFrame(mapping_data)
team_map = dict(zip(mapping_df["TeamRankingsName"], mapping_df["short_name"]))

# -----------------------------
# Scrape OPS Last 3 data
# -----------------------------
df = pd.read_html("https://www.teamrankings.com/mlb/stat/on-base-plus-slugging-pct")[0]
df = df[["Team", "Last 3"]].copy()
df.columns = ["Team", "team_last_3"]

# Map to short name
df["team"] = df["Team"].map(team_map)
df = df.dropna(subset=["team"])  # Drop rows where team couldn't be mapped

# Only keep short name and OPS
final_df = df[["team", "team_last_3"]].copy()

# -----------------------------
# Upload to Supabase
# -----------------------------
# Clear existing data
supabase.table(TABLE_NAME).delete().neq("team", "").execute()

# Insert new data
records = final_df.to_dict(orient="records")
supabase.table(TABLE_NAME).insert(records).execute()

print("✅ Uploaded team Last 3 data with short names to Supabase.")