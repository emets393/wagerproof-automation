from supabase import create_client, Client
import requests
from datetime import datetime
import os

# -----------------------------
# SUPABASE SETUP
# -----------------------------
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(url, key)

# -----------------------------
# FETCH TEAM MAPPING
# -----------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {t["full_name"]: {"short": t["short_name"], "number": t["team_number"]} for t in team_map_resp.data}

# -----------------------------
# RAPIDAPI CONFIG
# -----------------------------
today = datetime.today().strftime('%Y-%m-%d')
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
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY")
}

# -----------------------------
# FETCH ODDS DATA
# -----------------------------
response = requests.get(url, headers=headers, params=querystring)
games = response.json()

# -----------------------------
# PROCESS GAMES
# -----------------------------
records = []
for game in games:
    home_team = game.get("home_team")
    away_team = game.get("away_team")
    game_id = game.get("id")
    commence_time = game.get("commence_time")

    # Map short names and numbers
    home_info = team_map.get(home_team, {"short": home_team, "number": None})
    away_info = team_map.get(away_team, {"short": away_team, "number": None})

    # Create row template
    row = {
        "game_id": game_id,
        "commence_time": commence_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_team_short": home_info["short"],
        "away_team_short": away_info["short"],
        "home_team_number": home_info["number"],
        "away_team_number": away_info["number"],
        "first_fetched": datetime.utcnow().isoformat(),
        "last_fetched": datetime.utcnow().isoformat(),
    }

    # Extract DraftKings data
    dk = next((b for b in game["bookmakers"] if b["key"] == "draftkings"), None)
    if dk:
        for market in dk.get("markets", []):
            if market["key"] == "h2h":
                for outcome in market["outcomes"]:
                    if outcome["name"] == home_team:
                        row["home_moneyline"] = outcome["price"]
                    elif outcome["name"] == away_team:
                        row["away_moneyline"] = outcome["price"]
            elif market["key"] == "spreads":
                for outcome in market["outcomes"]:
                    if outcome["name"] == home_team:
                        row["home_spread"] = outcome["point"]
                        row["home_spread_odds"] = outcome["price"]
                    elif outcome["name"] == away_team:
                        row["away_spread"] = outcome["point"]
                        row["away_spread_odds"] = outcome["price"]
            elif market["key"] == "totals":
                for outcome in market["outcomes"]:
                    if outcome["name"] == "Over":
                        row["total_line"] = outcome["point"]
                        row["over_odds"] = outcome["price"]
                    elif outcome["name"] == "Under":
                        row["under_odds"] = outcome["price"]

    records.append(row)

# -----------------------------
# UPSERT TO SUPABASE
# -----------------------------
for row in records:
    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["game_id"]).execute()




