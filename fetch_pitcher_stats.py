# fetch_pitcher_stats.py

import os
import requests
from datetime import datetime
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
# TIME & DATE SETUP
# -----------------------------
eastern = pytz.timezone("US/Eastern")
today_et = datetime.now(eastern).date()
today = today_et.strftime('%Y-%m-%d')

# -----------------------------
# FETCH PITCHER STATS
# -----------------------------
url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher(note,stats(type=season,group=pitching))"
response = requests.get(url)
data = response.json()
games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

# -----------------------------
# TEAM MAPPING
# -----------------------------
team_map_resp = supabase.table("mlb_teams").select("full_name, short_name").execute()
team_map = {team["full_name"].lower(): team["short_name"] for team in team_map_resp.data}

# -----------------------------
# PROCESS GAMES
# -----------------------------
for game in games:
    game_date = today
    game_time_utc = game.get("gameDate")
    start_hour_minute = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00")).strftime('%H:%M')

    home_team = game["teams"]["home"]["team"]["name"].lower()
    away_team = game["teams"]["away"]["team"]["name"].lower()

    home_team_short = team_map.get(home_team, home_team)
    away_team_short = team_map.get(away_team, away_team)

    unique_id = f"{game_date}-{home_team_short}_{away_team_short}_{start_hour_minute}"

    def extract_pitcher_data(team_key):
        pitcher = game["teams"][team_key].get("probablePitcher")
        if not pitcher:
            return None, None, None

        stats = pitcher.get("stats", [])
        season_stats = next((s for s in stats if s.get("type", {}).get("displayName") == "season"), {})
        splits = season_stats.get("splits", [{}])[0]

        era = splits.get("era")
        whip = splits.get("whip")
        hand = pitcher.get("pitchHand", {}).get("description")
        return era, whip, hand

    home_era, home_whip, home_hand = extract_pitcher_data("home")
    away_era, away_whip, away_hand = extract_pitcher_data("away")

    row = {
        "unique_id": unique_id,
        "game_date": game_date,
        "home_team": home_team_short,
        "away_team": away_team_short,
        "start_time_utc": game_time_utc,
        "home_pitcher_era": home_era,
        "home_pitcher_whip": home_whip,
        "home_pitcher_hand": home_hand,
        "away_pitcher_era": away_era,
        "away_pitcher_whip": away_whip,
        "away_pitcher_hand": away_hand
    }

    supabase.table("pitcher_stats").upsert(row, on_conflict=["unique_id"]).execute()
    print(f"⬆️ Inserted pitcher stats for: {unique_id}")

