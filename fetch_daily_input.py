# fetch_daily_input.py

import requests
from datetime import datetime
import os
import pytz
from supabase import create_client, Client

# Supabase setup
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# Time setup
eastern = pytz.timezone("US/Eastern")
today_et = datetime.now(eastern)
today = today_et.strftime('%Y-%m-%d')

# Normalize function
def normalize_team_name(name):
    return name.strip().lower().replace("é", "e")

# Fetch team mapping
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    normalize_team_name(team["full_name"]): {
        "short": team["short_name"],
        "number": team["team_number"]
    }
    for team in team_map_resp.data
}

# Fetch game data
mlb_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&expand=schedule.teams"
res = requests.get(mlb_url)
data = res.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

# Delete old rows
supabase.table("input_values").delete().eq("game_date", today).execute()

# Insert input values
for game in games:
    game_pk = game.get("gamePk")
    home_full = game['teams']['home']['team']['name']
    away_full = game['teams']['away']['team']['name']
    game_time_utc = game.get("gameDate")

    dt_utc = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
    dt_et = dt_utc.astimezone(eastern)
    game_date = dt_et.strftime('%Y-%m-%d')
    start_time = dt_et.strftime('%H:%M')

    home_data = team_map.get(normalize_team_name(home_full), {"short": home_full, "number": None})
    away_data = team_map.get(normalize_team_name(away_full), {"short": away_full, "number": None})

    unique_id = f"{game_date}-{home_data['short']}_{away_data['short']}_{start_time}"

    row = {
        "unique_id": unique_id,
        "game_date": game_date,
        "start_time_et": dt_et.strftime('%Y-%m-%d %H:%M:%S'),
        "home_team": home_data["short"],
        "home_team_number": home_data["number"],
        "away_team": away_data["short"],
        "away_team_number": away_data["number"]
    }

    supabase.table("input_values").insert(row).execute()
    print(f"⬆️ Input inserted for: {unique_id}")




