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
ODDS_URL = f"https://odds.p.rapidapi.com/v4/sports/{SPORT}/odds"

# Supabase setup
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# -----------------------------
# Fetch team mapping
# -----------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    t["full_name"]: {"short": t["short_name"], "number": t["team_number"]}
    for t in team_map_resp.data
}

# -----------------------------
# Fetch odds data
# -----------------------------
response = requests.get(
    f"{ODDS_URL}?regions={REGIONS}&markets={MARKETS}&oddsFormat=american&dateFormat=iso",
    headers={
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "odds.p.rapidapi.com"
    }
)

odds_data = response.json()

# -----------------------------
# Process and Insert Odds
# -----------------------------
for game in odds_data:
    teams = game.get("teams", [])
    home_team = game.get("home_team", "")
    away_team = [t for t in teams if t != home_team][0] if len(teams) == 2 else ""

    bookmaker = next((b for b in game.get("bookmakers", []) if b["key"] == SPORTSBOOK), None)
    if not bookmaker:
        continue

    game_id = str(game["id"])
    game_date = game["commence_time"][:10]

    # Get mapped names
    home_info = team_map.get(home_team, {"short": home_team, "number": None})
    away_info = team_map.get(away_team, {"short": away_team, "number": None})

    # Initialize values
    home_spread = home_spread_odds = away_spread = away_spread_odds = None
    home_moneyline = away_moneyline = over_under_total = over_odds = under_odds = None

    for market in bookmaker["markets"]:
        if market["key"] == "spreads":
            for outcome in market["outcomes"]:
                if outcome["name"] == home_team:
                    home_spread = outcome.get("point")
                    home_spread_odds = outcome.get("price")
                elif outcome["name"] == away_team:
                    away_spread = outcome.get("point")
                    away_spread_odds = outcome.get("price")
        elif market["key"] == "h2h":
            for outcome in market["outcomes"]:
                if outcome["name"] == home_team:
                    home_moneyline = outcome.get("price")
                elif outcome["name"] == away_team:
                    away_moneyline = outcome.get("price")
        elif market["key"] == "totals":
            for outcome in market["outcomes"]:
                over_under_total = outcome.get("point")
                if outcome["name"].lower() == "over":
                    over_odds = outcome.get("price")
                elif outcome["name"].lower() == "under":
                    under_odds = outcome.get("price")

    row = {
        "game_id": game_id,
        "game_date": game_date,
        "home_team": home_info["short"],
        "home_team_number": home_info["number"],
        "away_team": away_info["short"],
        "away_team_number": away_info["number"],
        "sportsbook": SPORTSBOOK,

        "home_spread": home_spread,
        "home_spread_odds": home_spread_odds,
        "away_spread": away_spread,
        "away_spread_odds": away_spread_odds,
        "home_moneyline": home_moneyline,
        "away_moneyline": away_moneyline,
        "over_under_total": over_under_total,
        "over_odds": over_odds,
        "under_odds": under_odds
    }

    # Insert first odds if not exists
    existing_first = supabase.table("mlb_betting_odds").select("id").eq("game_id", game_id).eq("odds_type", "first").eq("sportsbook", SPORTSBOOK).execute()
    if not existing_first.data:
        supabase.table("mlb_betting_odds").insert({**row, "odds_type": "first"}).execute()

    # Upsert last odds
    supabase.table("mlb_betting_odds").upsert({**row, "odds_type": "last"}, on_conflict=["game_id", "odds_type", "sportsbook"]).execute()
