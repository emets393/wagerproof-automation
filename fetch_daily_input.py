import requests
from datetime import datetime
import os
from supabase import create_client, Client
import pandas as pd

# Initialize Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# Fetch team mapping (full name -> short name, number)
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    t["full_name"]: {
        "short": t["short_name"],
        "number": t["team_number"]
    }
    for t in team_map_resp.data
}

# Fetch today's games from MLB API
mlb_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=linescore"
res = requests.get(mlb_url)
data = res.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

# Delete old records for today
supabase.table("input_values").delete().eq("date", today).execute()

# Insert updated records
for game in games:
    game_pk = game.get("gamePk")
    home_full = game['teams']['home']['team']['name']
    away_full = game['teams']['away']['team']['name']

    home_data = team_map.get(home_full, {"short": home_full, "number": None})
    away_data = team_map.get(away_full, {"short": away_full, "number": None})

    unique_id = f"{home_data['short']}{away_data['short']}{game_pk}"

    row = {
        "date": today,
        "home_team": home_data["short"],
        "home_team_number": home_data["number"],
        "away_team": away_data["short"],
        "away_team_number": away_data["number"],
        "unique_id": unique_id,
        "game_pk": game_pk
    }

    supabase.table("input_values").insert(row).execute()

