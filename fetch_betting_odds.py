import os
import requests
from datetime import datetime
from supabase import create_client, Client

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
# HELPER FUNCTIONS
# -----------------------------
def extract_market(bookmaker, market_key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market.get("outcomes", [])
    return []

def find_outcome(outcomes, team_name):
    return next((o for o in outcomes if o["name"] == team_name), {})

def odds_changed(existing, new):
    fields = [
        "home_moneyline", "away_moneyline",
        "home_spread", "home_spread_odds",
        "away_spread", "away_spread_odds",
        "over_under_total"
    ]
    return any(existing.get(f) != new.get(f) for f in fields)

# -----------------------------
# PROCESS EACH GAME
# -----------------------------
for game in games:
    game_id = game["id"]
    game_time = game["commence_time"]
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
    ou_total = totals[0]["point"] if totals else None

    now_iso = datetime.utcnow().isoformat()

    row = {
        "game_id": game_id,
        "game_time": game_time,
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
        "over_under_total": ou_total,
        "first_fetched": now_iso,
        "last_fetched": now_iso,
    }

    # --- Upsert into live odds table ---
    supabase.table("mlb_betting_lines").upsert(row, on_conflict=["game_id"]).execute()

    # --- Handle history table ---
    existing_history = supabase.table("mlb_betting_lines_history").select("*").eq("game_id", game_id).execute().data
    if not existing_history:
        supabase.table("mlb_betting_lines_history").insert(row).execute()
        print(f"🆕 Added to history: {home_team} vs {away_team}")
    else:
        existing_row = existing_history[0]
        if odds_changed(existing_row, row):
            updated_row = row.copy()
            updated_row["first_fetched"] = existing_row["first_fetched"]
            supabase.table("mlb_betting_lines_history").upsert(updated_row, on_conflict=["game_id"]).execute()
            print(f"🔁 Updated history (odds changed): {home_team} vs {away_team}")
        else:
            print(f"⏸ No update needed for: {home_team} vs {away_team}")





