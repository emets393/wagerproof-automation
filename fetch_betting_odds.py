import os
import requests
from datetime import datetime
from supabase import create_client, Client

# Initialize Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Today's date and current UTC time
today = datetime.utcnow().strftime('%Y-%m-%d')
now = datetime.utcnow().isoformat()

# Fetch team mapping from mlb_teams
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {
    t["full_name"]: {
        "short": t["short_name"],
        "number": t["team_number"]
    }
    for t in team_map_resp.data
}

# Helper to extract odds
def get_outcome_price(outcomes, team_name):
    for o in outcomes:
        if o["name"] == team_name:
            return o.get("price")
    return None

def get_outcome_point(outcomes, team_name):
    for o in outcomes:
        if o["name"] == team_name:
            return o.get("point")
    return None

# Fetch betting data
headers = {
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "odds.p.rapidapi.com"
}
params = {
    "regions": "us",
    "oddsFormat": "american",
    "markets": "spreads,h2h,totals",
    "dateFormat": "iso"
}
resp = requests.get("https://odds.p.rapidapi.com/v4/sports/baseball_mlb/odds", headers=headers, params=params)
games = resp.json()

for game in games:
    game_pk = game.get("id")
    game_time = game.get("commence_time", "")[:19]
    teams = game.get("teams", [])
    home_full = game.get("home_team")
    away_full = [team for team in teams if team != home_full][0]

    home = team_map.get(home_full, {"short": home_full, "number": None})
    away = team_map.get(away_full, {"short": away_full, "number": None})

    # Get markets
    bookmakers = game.get("bookmakers", [])
    spread = moneyline = total = []

    for bm in bookmakers:
        for market in bm.get("markets", []):
            if market["key"] == "spreads":
                spread = market["outcomes"]
            elif market["key"] == "h2h":
                moneyline = market["outcomes"]
            elif market["key"] == "totals":
                total = market["outcomes"]

    # Check if already exists
    existing = supabase.table("mlb_betting_lines").select("game_pk").eq("game_pk", game_pk).execute()

    row = {
        "game_pk": game_pk,
        "date": today,
        "game_time": game_time,
        "home_team": home["short"],
        "home_team_number": home["number"],
        "away_team": away["short"],
        "away_team_number": away["number"],
        "home_spread": get_outcome_point(spread, home_full),
        "home_spread_odds": get_outcome_price(spread, home_full),
        "away_spread": get_outcome_point(spread, away_full),
        "away_spread_odds": get_outcome_price(spread, away_full),
        "home_moneyline": get_outcome_price(moneyline, home_full),
        "away_moneyline": get_outcome_price(moneyline, away_full),
        "over_under_total": get_outcome_point(total, "Over"),
        "last_fetch_time": now
    }

    if not existing.data:
        row["first_fetch_time"] = now

    # Upsert by game_pk
    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["game_pk"]).execute()


