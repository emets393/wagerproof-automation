import os
import requests
from datetime import datetime
from supabase import create_client, Client
import pytz
import unicodedata

# -----------------------------
# SUPABASE SETUP
# -----------------------------
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# -----------------------------
# TIME SETUP
# -----------------------------
eastern = pytz.timezone("US/Eastern")
today_et = datetime.now(eastern)
today = today_et.strftime('%Y-%m-%d')

# -----------------------------
# TEAM MAPPING
# -----------------------------
team_name_map = {
    "Arizona Diamondbacks": "Arizona", "Atlanta Braves": "Atlanta", "Baltimore Orioles": "Baltimore",
    "Boston Red Sox": "Boston", "Chicago Cubs": "Cubs", "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Cincinnati", "Cleveland Guardians": "Cleveland", "Colorado Rockies": "Colorado",
    "Detroit Tigers": "Detroit", "Houston Astros": "Houston", "Kansas City Royals": "Kansas City",
    "Los Angeles Angels": "Angels", "Los Angeles Dodgers": "Dodgers", "Miami Marlins": "Miami",
    "Milwaukee Brewers": "Milwaukee", "Minnesota Twins": "Minnesota", "New York Mets": "Mets",
    "New York Yankees": "Yankees", "Oakland Athletics": "Oakland", "Philadelphia Phillies": "Philadelphia",
    "Pittsburgh Pirates": "Pittsburgh", "San Diego Padres": "San Diego", "San Francisco Giants": "San Francisco",
    "Seattle Mariners": "Seattle", "St. Louis Cardinals": "ST Louis", "Tampa Bay Rays": "Tampa Bay",
    "Texas Rangers": "Texas", "Toronto Blue Jays": "Toronto", "Washington Nationals": "Washington"
}

def remove_accents(text):
    if not isinstance(text, str):
        return text
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

def get_pitcher_stats_and_hand(pitcher_id):
    url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}?hydrate=stats(group=[pitching],type=[season])"
    try:
        res = requests.get(url).json()
        person = res['people'][0]

        # Get ERA/WHIP
        stats = person.get('stats', [])
        if stats and 'splits' in stats[0] and stats[0]['splits']:
            pitching_stats = stats[0]['splits'][0]['stat']
            era = pitching_stats.get('era', None)
            whip = pitching_stats.get('whip', None)
        else:
            era, whip = None, None

        # Get throwing hand
        hand = person.get('pitchHand', {}).get('code')
        handedness = 1 if hand == 'R' else 2 if hand == 'L' else None

    except:
        era, whip, handedness = None, None, None

    return era, whip, handedness

# -----------------------------
# FETCH TODAY'S GAMES
# -----------------------------
schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher(note,stats),linescore,team"
response = requests.get(schedule_url)
data = response.json()
games = data['dates'][0]['games'] if data['dates'] else []

# -----------------------------
# PROCESS GAMES
# -----------------------------
for game in games:
    game_time_utc = datetime.fromisoformat(game["gameDate"].replace("Z", "+00:00"))
    game_time_et = game_time_utc.astimezone(eastern)
    start_time = game_time_et.strftime('%H:%M')

    away_team_full = game['teams']['away']['team']['name']
    home_team_full = game['teams']['home']['team']['name']
    away_team = team_name_map.get(away_team_full, away_team_full)
    home_team = team_name_map.get(home_team_full, home_team_full)

    unique_id = f"{today}-{home_team}_{away_team}_{start_time}"

    away_info = game['teams']['away'].get('probablePitcher')
    home_info = game['teams']['home'].get('probablePitcher')

    away_pitcher_name = remove_accents(away_info['fullName']) if away_info else 'TBD'
    home_pitcher_name = remove_accents(home_info['fullName']) if home_info else 'TBD'

    away_era, away_whip, away_hand = get_pitcher_stats_and_hand(away_info['id']) if away_info else (None, None, None)
    home_era, home_whip, home_hand = get_pitcher_stats_and_hand(home_info['id']) if home_info else (None, None, None)

    row = {
        "unique_id": unique_id,
        "game_date": today,
        "start_time_et": game_time_et.strftime('%Y-%m-%d %H:%M:%S'),
        "start_time_utc": game_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
        "home_team": home_team,
        "away_team": away_team,
        "home_pitcher": home_pitcher_name,
        "away_pitcher": away_pitcher_name,
        "home_pitcher_era": home_era,
        "home_pitcher_whip": home_whip,
        "home_pitcher_hand": home_hand,
        "away_pitcher_era": away_era,
        "away_pitcher_whip": away_whip,
        "away_pitcher_hand": away_hand
    }

    supabase.table("pitcher_stats").upsert(row, on_conflict=["unique_id"]).execute()
    print(f"✅ Inserted: {unique_id}")



