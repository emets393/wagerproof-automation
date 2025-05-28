import requests
from datetime import datetime
import os
from supabase import create_client, Client

# Initialize Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# Fetch games
mlb_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"
res = requests.get(mlb_url)
data = res.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

# Format and insert
for game in games:
    home = game['teams']['home']['team']['name']
    away = game['teams']['away']['team']['name']
    game_data = {
        "date": today,
        "home_team": home,
        "away_team": away
    }
    supabase.table("input_values").insert(game_data).execute()
