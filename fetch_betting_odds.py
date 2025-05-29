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
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name, team_number").execute()
team_map = {team["full_name"]: team for team in team_map_resp.data}

# -----------------------------
# TIME FILTER SETUP
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
today_et = now_et.date()
cutoff_time = now_et.astimezone(pytz.utc) + timedelta(hours=1)

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def extract_market(bookmaker, market_key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market.get("outcomes", [])
    return []

def find_outcome(outcomes, name):
    return next((o for o in outcomes if o["name"] == name), {})

def find_ou_outcome(outcomes, ou_type):
    return next((o for o in outcomes if o["name"].lower() == ou_type), {})

def odds_changed(existing, new):
    fields = [
        "home_moneyline", "away_moneyline",
        "home_spread", "home_spread_odds",
        "away_spread", "away_spread_odds",
        "over_price", "over_point",
        "under_price", "under_point"
    ]
    return any(existing.get(f) != new.get(f) for f in fields)

# -----------------------------
# PROCESS EACH GAME
# -----------------------------
for game in games:
    commence_time = datetime.fromisoformat(game["commence_time"]).replace(tzinfo=pytz.utc)
    commence_et = commence_time.astimezone(eastern)
    if commence_et.date() != today_et:
        continue
    if commence_time <= cutoff_time:
        continue

    game_id = game["id"]
    home_team_long = game["home_team"]
    away_team_long = game["away_team"]

    if home_team_long not in team_map or away_team_long not in team_map:
        print(f"❌ Missing team map: {home_team_long} vs {away_team_long}")
        continue

    home_team = team_map[home_team_long]["short_name"]
    away_team = team_map[away_team_long]["short_name"]
    home_team_number = team_map[home_team_long]["team_number"]
    away_team_number = team_map[away_team_long]["team_number"]

    bookmaker = next((b for b in game["bookmakers"] if b["key"] == "draftkings"), None)
    if not bookmaker:
        print(f"⚠️ No DraftKings odds for {home_team} vs {away_team}")
        continue

    spreads = extract_market(bookmaker, "spreads")
    h2h = extract_market(bookmaker, "h2h")
    totals = extract_market(bookmaker, "totals")

    home_spread_data = find_outcome(spreads, home_team_long)
    away_spread_data = find_outcome(spreads, away_team_long)
    home_ml_data = find_outcome(h2h, home_team_long)
    away_ml_data = find_outcome(h2h, away_team_long)

    over_data = find_ou_outcome(totals, "over")
    under_data = find_ou_outcome(totals, "under")

    now_iso = datetime.utcnow().isoformat()

    row = {
        "game_id": game_id,
        "game_time": commence_et.isoformat(),
        "home_team": home_team,
        "away_team": away_team,
        "home_team_number": home_team_number,
        "away_team_number": away_team_number,
        "home_spread": home_spread_data.get("point"),
        "home_spread_odds": home_spread_data.get("price"),
        "home_moneyline": home_ml_data.get("price"),
        "away_spread": away_spread_data.get("point"),
        "away_spread_odds": away_spread_data.get("price"),
        "away_moneyline": away_ml_data.get("price"),
        "over_point": over_data.get("point"),
        "over_price": over_data.get("price"),
        "under_point": under_data.get("point"),
        "under_price": under_data.get("price"),
        "fetched_at": now_iso
    }

    # --- Upsert into live odds table ---
    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["game_id"]).execute()

    # --- Insert into history table ---
    history_rows = supabase.table("mlb_betting_lines_history").select("*").eq("game_id", game_id).order("fetched_at").execute().data
    if not history_rows:
        row["first_fetched"] = now_iso
        row["last_fetched"] = None
        supabase.table("mlb_betting_lines_history").insert(row).execute()
        print(f"🆕 First fetch history: {home_team} vs {away_team}")
    else:
        last_row = history_rows[-1]
        if odds_changed(last_row, row):
            row["first_fetched"] = None
            row["last_fetched"] = now_iso
            supabase.table("mlb_betting_lines_history").insert(row).execute()
            print(f"🔁 Added last fetch history: {home_team} vs {away_team}")
        else:
            print(f"⏸ No update needed for: {home_team} vs {away_team}")









