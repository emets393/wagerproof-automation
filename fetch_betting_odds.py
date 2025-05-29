import requests
import os
from datetime import datetime
from supabase import create_client, Client

# -----------------------------
# Configuration
# -----------------------------
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
SPORT = "baseball_mlb"
SPORTSBOOK = "draftkings"
REGIONS = "us"
MARKETS = "spreads,h2h,totals"
import requests
from datetime import datetime
import os
from supabase import create_client, Client

# --------------------------
# Setup
# --------------------------
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
odds_api_key = os.getenv("ODDS_API_KEY")
supabase: Client = create_client(url, key)

# --------------------------
# Fetch team name mapping
# --------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    t["full_name"]: {
        "short": t["short_name"],
        "number": t["team_number"]
    }
    for t in team_map_resp.data
}

# --------------------------
# Get today's date
# --------------------------
today = datetime.today().strftime('%Y-%m-%d')

# --------------------------
# Fetch odds from API
# --------------------------
odds_url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
params = {
    "regions": "us",
    "markets": "spreads,h2h,totals",
    "oddsFormat": "american",
    "dateFormat": "iso",
    "apiKey": odds_api_key
}

response = requests.get(odds_url, params=params)
if response.status_code != 200:
    raise Exception(f"Failed to fetch odds: {response.status_code} {response.text}")

odds_data = response.json()

# --------------------------
# Insert odds into Supabase
# --------------------------
for game in odds_data:
    try:
        home_team_full = game["home_team"]
        away_team_full = game["away_team"]
        game_time = game["commence_time"]
        game_pk = game["id"]  # Use API game id as unique game ID

        # Map to short name and number
        home = team_map.get(home_team_full, {"short": home_team_full, "number": None})
        away = team_map.get(away_team_full, {"short": away_team_full, "number": None})

        # Extract odds (from first bookmaker only, adjust as needed)
        bookmaker = game.get("bookmakers", [])[0] if game.get("bookmakers") else {}
        markets = bookmaker.get("markets", []) if bookmaker else []

        spread, moneyline, total = None, None, None
        for market in markets:
            if market["key"] == "spreads":
                spread = market["outcomes"]
            elif market["key"] == "h2h":
                moneyline = market["outcomes"]
            elif market["key"] == "totals":
                total = market["outcomes"]

        def get_outcome_price(outcomes, team_name=None):
            if not outcomes:
                return None
            if team_name:
                for o in outcomes:
                    if o.get("name") == team_name:
                        return o.get("price")
            return outcomes[0].get("price")  # fallback

        row = {
            "game_pk": game_pk,
            "date": today,
            "game_time": game_time,
            "home_team": home["short"],
            "home_team_number": home["number"],
            "away_team": away["short"],
            "away_team_number": away["number"],
            "home_spread": next((o["point"] for o in spread if o["name"] == home_team_full), None) if spread else None,
            "home_spread_odds": get_outcome_price(spread, home_team_full),
            "away_spread": next((o["point"] for o in spread if o["name"] == away_team_full), None) if spread else None,
            "away_spread_odds": get_outcome_price(spread, away_team_full),
            "home_moneyline": get_outcome_price(moneyline, home_team_full),
            "away_moneyline": get_outcome_price(moneyline, away_team_full),
            "over_under_total": next((o["point"] for o in total if o["name"] == "Over"), None) if total else None,
        }

        supabase.table("betting_lines").upsert(row, on_conflict=["game_pk"]).execute()

    except Exception as e:
        print(f"Error processing game: {e}")

