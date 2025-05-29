import requests
from datetime import datetime
import os
from supabase import create_client, Client

# -----------------------------
# CONFIGURATION
# -----------------------------
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# -----------------------------
# FETCH FROM API
# -----------------------------
url = "https://odds.p.rapidapi.com/v4/sports/baseball_mlb/odds"
querystring = {
    "regions": "us",
    "oddsFormat": "decimal",
    "markets": "h2h,spreads,totals",
    "dateFormat": "iso",
    "date": today
}

headers = {
    "x-rapidapi-host": "odds.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code != 200:
    print(f"Error fetching data: {response.status_code}")
    print(response.text)
    exit()

try:
    games = response.json()
except Exception as e:
    print("Failed to parse JSON response:", e)
    exit()

# -----------------------------
# FORMAT & STORE IN SUPABASE
# -----------------------------
for game in games:
    if not isinstance(game, dict):
        print("Skipping invalid game object:", game)
        continue

    game_id = game.get("id")  # Unique game identifier
    commence_time = game.get("commence_time")
    home_team = game.get("home_team")
    away_team = game.get("away_team")
    bookmakers = game.get("bookmakers", [])

    home_ml = away_ml = home_spread = away_spread = spread_point = total = None
    home_spread_odds = away_spread_odds = over_odds = under_odds = None

    for bookmaker in bookmakers:
        for market in bookmaker.get("markets", []):
            if market["key"] == "h2h":
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    if outcome["name"] == home_team:
                        home_ml = outcome["price"]
                    elif outcome["name"] == away_team:
                        away_ml = outcome["price"]

            elif market["key"] == "spreads":
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    if outcome["name"] == home_team:
                        home_spread = outcome["point"]
                        home_spread_odds = outcome["price"]
                    elif outcome["name"] == away_team:
                        away_spread = outcome["point"]
                        away_spread_odds = outcome["price"]
                    spread_point = outcome.get("point")

            elif market["key"] == "totals":
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    if outcome["name"].lower() == "over":
                        total = outcome["point"]
                        over_odds = outcome["price"]
                    elif outcome["name"].lower() == "under":
                        under_odds = outcome["price"]

    row = {
        "date": today,
        "game_id": game_id,
        "home_team": home_team,
        "away_team": away_team,
        "home_ml": home_ml,
        "away_ml": away_ml,
        "home_spread": home_spread,
        "home_spread_odds": home_spread_odds,
        "away_spread": away_spread,
        "away_spread_odds": away_spread_odds,
        "total": total,
        "over_odds": over_odds,
        "under_odds": under_odds,
        "commence_time": commence_time,
        "first_fetch": today,
        "last_fetch": today
    }

    # Upsert into Supabase table `mlb_betting_lines`
    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["game_id"]).execute()



