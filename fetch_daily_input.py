import requests
from datetime import datetime
import os
from supabase import create_client, Client

# -----------------------------
# SUPABASE SETUP
# -----------------------------
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# -----------------------------
# DATE SETUP
# -----------------------------
today = datetime.today().strftime('%Y-%m-%d')

# -----------------------------
# TEAM NORMALIZATION
# -----------------------------
def normalize_team_name(name):
    return name.strip().lower().replace("é", "e")

# -----------------------------
# TEAM MAPPING FROM SUPABASE
# -----------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    normalize_team_name(team["full_name"]): {
        "short": team["short_name"],
        "number": team["team_number"]
    }
    for team in team_map_resp.data
}

# -----------------------------
# FETCH TODAY'S MLB GAMES
# -----------------------------
mlb_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&expand=schedule.teams"
res = requests.get(mlb_url)
data = res.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

# -----------------------------
# DELETE EXISTING ROWS FOR TODAY
# -----------------------------
supabase.table("input_values").delete().eq("date", today).execute()

# -----------------------------
# INSERT NEW GAMES
# -----------------------------
for game in games:
    game_pk = game.get("gamePk")
    home_full = game['teams']['home']['team']['name']
    away_full = game['teams']['away']['team']['name']

    home_data = team_map.get(normalize_team_name(home_full), {"short": home_full, "number": None})
    away_data = team_map.get(normalize_team_name(away_full), {"short": away_full, "number": None})

    unique_id = f"{home_data['short']}_{away_data['short']}_{game_pk}"

    row = {
        "date": today,
        "home_team": home_data["short"],
        "home_team_number": home_data["number"],
        "away_team": away_data["short"],
        "away_team_number": away_data["number"],
        "game_pk": game_pk,
        "unique_id": unique_id
    }

    supabase.table("input_values").insert(row).execute()
    print(f"✅ Inserted: {home_data['short']} vs {away_data['short']} ({unique_id})")



