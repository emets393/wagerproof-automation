# fetch_betting_odds.py

import requests
import os
from datetime import datetime
import pytz
from supabase import create_client, Client

# Supabase setup
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# Time setup
eastern = pytz.timezone("US/Eastern")
today_et = datetime.now(eastern)
today = today_et.strftime("%Y-%m-%d")

# Fetch odds
url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
params = {
    "apiKey": os.getenv("ODDS_API_KEY"),
    "regions": "us",
    "markets": "h2h,spreads,totals",
    "dateFormat": "iso",
    "oddsFormat": "american"
}
response = requests.get(url, params=params)
games = response.json()

for game in games:
    try:
        home = game["home_team"]
        away = game["away_team"]
        commence_time_utc = datetime.fromisoformat(game["commence_time"])
        commence_time_et = commence_time_utc.astimezone(eastern)
        game_date = commence_time_et.strftime('%Y-%m-%d')
        start_time = commence_time_et.strftime('%H:%M')
        unique_id = f"{game_date}-{home}_{away}_{start_time}"

        # Extract odds
        markets = game.get("bookmakers", [])[0].get("markets", []) if game.get("bookmakers") else []
        odds_data = {
            "unique_id": unique_id,
            "game_date": game_date,
            "home_team": home,
            "away_team": away,
            "start_time_et": commence_time_et.strftime('%Y-%m-%d %H:%M:%S')
        }

        for market in markets:
            if market["key"] == "h2h":
                outcomes = market["outcomes"]
                for o in outcomes:
                    if o["name"] == home:
                        odds_data["home_ml"] = o["price"]
                    elif o["name"] == away:
                        odds_data["away_ml"] = o["price"]
            elif market["key"] == "spreads":
                for o in market["outcomes"]:
                    if o["name"] == home:
                        odds_data["home_spread"] = o["point"]
                    elif o["name"] == away:
                        odds_data["away_spread"] = o["point"]
            elif market["key"] == "totals":
                for o in market["outcomes"]:
                    if o["name"] == "Over":
                        odds_data["over_line"] = o["point"]
                    elif o["name"] == "Under":
                        odds_data["under_line"] = o["point"]

        supabase.table("mlb_betting_lines").upsert(odds_data, on_conflict=["unique_id"]).execute()
        print(f"⬆️ Betting odds inserted for: {unique_id}")

    except Exception as e:
        print(f"⚠️ Failed to insert game: {e}")











