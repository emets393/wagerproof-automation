# fetch_betting_odds.py

import os
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client
import pytz

# -----------------------------
# SUPABASE SETUP
# -----------------------------
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    raise Exception("Supabase environment variables are not set.")

supabase: Client = create_client(supabase_url, supabase_key)

# -----------------------------
# API SETUP
# -----------------------------
today = datetime.today().strftime('%Y-%m-%d')
url = "https://odds.p.rapidapi.com/v4/sports/baseball_mlb/odds"

querystring = {
    "regions": "us",
    "oddsFormat": "american",
    "markets": "h2h,spreads,totals",
    "dateFormat": "iso",
    "date": today
}

headers = {
    "x-rapidapi-host": "odds.p.rapidapi.com",
    "x-rapidapi-key": os.environ.get("RAPIDAPI_KEY")
}

response = requests.get(url, headers=headers, params=querystring)
if response.status_code != 200:
    raise Exception(f"API Error {response.status_code}: {response.text}")

games = response.json()
print(f"✅ Fetched {len(games)} games for {today}")

# -----------------------------
# TEAM MAP
# -----------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name").execute()
team_map = {team["full_name"]: team["short_name"] for team in team_map_resp.data}

# -----------------------------
# TIME FILTER
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
today_et = now_et.date()
cutoff_time = now_et.astimezone(pytz.utc) + timedelta(hours=1)

# -----------------------------
# PROCESS GAMES
# -----------------------------
for game in games:
    commence_time = datetime.fromisoformat(game["commence_time"]).replace(tzinfo=pytz.utc)
    commence_et = commence_time.astimezone(eastern)

    if commence_et.date() != today_et:
        continue
    if commence_time <= cutoff_time:
        continue

    home_full = game["home_team"]
    away_full = game["away_team"]
    home_team = team_map.get(home_full, home_full)
    away_team = team_map.get(away_full, away_full)

    start_time_utc = commence_time.strftime('%H:%M')
    game_date = commence_time.strftime('%Y-%m-%d')
    unique_id = f"{game_date}-{home_team}_{away_team}_{start_time_utc}"

    bookmaker = next((b for b in game["bookmakers"] if b["key"] == "draftkings"), None)
    if not bookmaker:
        continue

    def extract(market):
        return next((m["outcomes"] for m in bookmaker.get("markets", []) if m["key"] == market), [])

    def find(outcomes, name):
        return next((o for o in outcomes if o["name"] == name), {})

    spreads = extract("spreads")
    h2h = extract("h2h")
    totals = extract("totals")

    row = {
        "unique_id": unique_id,
        "game_date": game_date,
        "home_team": home_team,
        "away_team": away_team,
        "start_time_utc": commence_time.isoformat(),
        "home_spread": find(spreads, home_full).get("point"),
        "home_spread_odds": find(spreads, home_full).get("price"),
        "away_spread": find(spreads, away_full).get("point"),
        "away_spread_odds": find(spreads, away_full).get("price"),
        "home_moneyline": find(h2h, home_full).get("price"),
        "away_moneyline": find(h2h, away_full).get("price"),
        "over_point": find(totals, "Over").get("point"),
        "over_price": find(totals, "Over").get("price"),
        "under_point": find(totals, "Under").get("point"),
        "under_price": find(totals, "Under").get("price"),
        "fetched_at": datetime.utcnow().isoformat()
    }

    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["unique_id"]).execute()
    print(f"⬆️ Inserted betting odds for: {unique_id}")










