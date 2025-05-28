import requests
import pandas as pd
from datetime import datetime, date
import unicodedata
from supabase import create_client, Client
import os

# -----------------------------
# SUPABASE SETUP
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# CONFIG
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

# -----------------------------
# FUNCTIONS
# -----------------------------
def get_pitcher_stats_and_hand(pitcher_id):
    url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}?hydrate=stats(group=[pitching],type=[season])"
    try:
        res = requests.get(url).json()
        person = res['people'][0]

        stats = person.get('stats', [])
        if stats and 'splits' in stats[0] and stats[0]['splits']:
            pitching_stats = stats[0]['splits'][0]['stat']
            era = pitching_stats.get('era', 'N/A')
            whip = pitching_stats.get('whip', 'N/A')
        else:
            era, whip = 'N/A', 'N/A'

        hand = person.get('pitchHand', {}).get('code', 'N')
        handedness = 1 if hand == 'R' else 2 if hand == 'L' else 'N/A'

    except:
        era, whip, handedness = 'N/A', 'N/A', 'N/A'

    return era, whip, handedness

def remove_accents(text):
    if not isinstance(text, str):
        return text
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

# -----------------------------
# STEP 1: Delete previous entries for today
# -----------------------------
today_iso = date.today().isoformat()
existing_rows = supabase.table("pitcher_stats").select("id, Date").execute().data
ids_to_delete = [row['id'] for row in existing_rows if row['Date'].startswith(today_iso)]

for row_id in ids_to_delete:
    supabase.table("pitcher_stats").delete().eq("id", row_id).execute()

# -----------------------------
# STEP 2: Fetch today's games
# -----------------------------
today = datetime.today().strftime('%Y-%m-%d')
schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher(note,stats),linescore,team"
response = requests.get(schedule_url)
data = response.json()
games = data['dates'][0]['games'] if data['dates'] else []

# -----------------------------
# STEP 3: Build pitcher data
# -----------------------------
results = []

for game in games:
    away_team = game['teams']['away']['team']['name']
    home_team = game['teams']['home']['team']['name']

    away_info = game['teams']['away'].get('probablePitcher')
    home_info = game['teams']['home'].get('probablePitcher')

    away_name = away_info['fullName'] if away_info else 'TBD'
    home_name = home_info['fullName'] if home_info else 'TBD'

    if away_info:
        away_era, away_whip, away_hand = get_pitcher_stats_and_hand(away_info['id'])
    else:
        away_era, away_whip, away_hand = 'N/A', 'N/A', 'N/A'

    if home_info:
        home_era, home_whip, home_hand = get_pitcher_stats_and_hand(home_info['id'])
    else:
        home_era, home_whip, home_hand = 'N/A', 'N/A', 'N/A'

    results.append({
        'Date': today,
        'Away Team': team_name_map.get(away_team, away_team),
        'Away Pitcher': remove_accents(away_name),
        'Away ERA': away_era,
        'Away WHIP': away_whip,
        'Away Handedness': away_hand,
        'Home Team': team_name_map.get(home_team, home_team),
        'Home Pitcher': remove_accents(home_name),
        'Home ERA': home_era,
        'Home WHIP': home_whip,
        'Home Handedness': home_hand
    })

# -----------------------------
# STEP 4: Insert into Supabase
# -----------------------------
if results:
    supabase.table("pitcher_stats").insert(results).execute()
