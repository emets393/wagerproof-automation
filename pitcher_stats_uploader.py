#----Pitching stats inputed into supabase

import requests
import pandas as pd
from datetime import datetime, timedelta
import unicodedata
from collections import Counter
from supabase import create_client, Client

# -----------------------------
# Supabase Config
# -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)

# -----------------------------
# Team Map
# -----------------------------
team_name_map = {
    "Arizona Diamondbacks": "Arizona", "Atlanta Braves": "Atlanta", "Baltimore Orioles": "Baltimore",
    "Boston Red Sox": "Boston", "Chicago Cubs": "Cubs", "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Cincinnati", "Cleveland Guardians": "Cleveland", "Colorado Rockies": "Colorado",
    "Detroit Tigers": "Detroit", "Houston Astros": "Houston", "Kansas City Royals": "Kansas City",
    "Los Angeles Angels": "Angels", "Los Angeles Dodgers": "Dodgers", "Miami Marlins": "Miami",
    "Milwaukee Brewers": "Milwaukee", "Minnesota Twins": "Minnesota", "New York Mets": "Mets",
    "New York Yankees": "Yankees", "Athletics": "Athletics", "Philadelphia Phillies": "Philadelphia",
    "Pittsburgh Pirates": "Pittsburgh", "San Diego Padres": "San Diego", "San Francisco Giants": "San Francisco",
    "Seattle Mariners": "Seattle", "St. Louis Cardinals": "ST Louis", "Tampa Bay Rays": "Tampa Bay",
    "Texas Rangers": "Texas", "Toronto Blue Jays": "Toronto", "Washington Nationals": "Washington"
}

# -----------------------------
# Helpers
# -----------------------------
def remove_accents(text):
    if not isinstance(text, str):
        return text
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

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

# -----------------------------
# Pitcher ID Management
# -----------------------------
response = supabase.table("pitcher_ids").select("*").execute()
existing_pitchers = {row['pitcher_name']: row['pitcher_id'] for row in response.data}

def get_or_create_pitcher_id(pitcher_name):
    global existing_pitchers
    if pitcher_name in existing_pitchers:
        return existing_pitchers[pitcher_name]
    next_id = max(existing_pitchers.values(), default=1000) + 1
    existing_pitchers[pitcher_name] = next_id
    supabase.table("pitcher_ids").insert({
        "pitcher_name": pitcher_name,
        "pitcher_id": next_id
    }).execute()
    return next_id

# -----------------------------
# Fetch Today's Games
# -----------------------------
today = datetime.today()
today_str = today.strftime('%Y-%m-%d')
excel_date = (today - datetime(1899, 12, 30)).days

schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today_str}&hydrate=probablePitcher(note,stats),linescore,team"
response = requests.get(schedule_url)
data = response.json()
all_games = data['dates'][0]['games'] if data['dates'] else []
games = []

for g in all_games:
    official_date = g.get("officialDate")
    if official_date == today.strftime('%Y-%m-%d'):
        games.append(g)



# -----------------------------
# Parse Games
# -----------------------------
results = []
matchup_seen = Counter()

for game in games:
    away_team = game['teams']['away']['team']['name']
    home_team = game['teams']['home']['team']['name']

    away_info = game['teams']['away'].get('probablePitcher')
    home_info = game['teams']['home'].get('probablePitcher')

    away_name = away_info['fullName'] if away_info else 'TBD'
    home_name = home_info['fullName'] if home_info else 'TBD'

    away_pitcher_name = remove_accents(away_name)
    home_pitcher_name = remove_accents(home_name)

    away_pitcher_id = get_or_create_pitcher_id(away_pitcher_name)
    home_pitcher_id = get_or_create_pitcher_id(home_pitcher_name)

    if away_info:
        away_era, away_whip, away_hand = get_pitcher_stats_and_hand(away_info['id'])
    else:
        away_era, away_whip, away_hand = 'N/A', 'N/A', 'N/A'

    if home_info:
        home_era, home_whip, home_hand = get_pitcher_stats_and_hand(home_info['id'])
    else:
        home_era, home_whip, home_hand = 'N/A', 'N/A', 'N/A'

    away_team_short = team_name_map.get(away_team, away_team)
    home_team_short = team_name_map.get(home_team, home_team)
    matchup_key = f"{home_team_short}_{away_team_short}"

    matchup_seen[matchup_key] += 1
    doubleheader_game = matchup_seen[matchup_key]
    unique_id = f"{home_team_short}{away_team_short}{excel_date}{doubleheader_game}"

    start_time_utc = datetime.fromisoformat(game['gameDate'].replace('Z', '+00:00'))
    start_time_et = (start_time_utc - timedelta(hours=4)).strftime('%#I:%M %p')

    results.append({
        'unique_id': unique_id,
        'date': today_str,
        'start_time_et': start_time_et,
        'away_team': away_team_short,
        'away_pitcher': away_pitcher_name,
        'away_pitcher_id': away_pitcher_id,
        'away_era': away_era,
        'away_whip': away_whip,
        'home_team': home_team_short,
        'home_pitcher': home_pitcher_name,
        'home_pitcher_id': home_pitcher_id,
        'home_era': home_era,
        'home_whip': home_whip,
        'away_handedness': away_hand,
        'home_handedness': home_hand,
        'doubleheader_game': doubleheader_game
    })

# -----------------------------
# Upload to Supabase
# -----------------------------
# -----------------------------
# Upload to Supabase
# -----------------------------
df = pd.DataFrame(results)

print(f"🧹 Deleting today's data from `pitching_data_today`...")
try:
    # only delete rows where date == today_str
    supabase.table("pitching_data_today") \
        .delete() \
        .eq("date", today_str) \
        .execute()
    print("✅ Old data deleted.")
except Exception as e:
    print(f"⚠️ Warning: Failed to delete old data (continuing anyway): {e}")

print(f"⬆️ Uploading {len(df)} rows...")
supabase.table("pitching_data_today") \
    .insert(df.to_dict("records")) \
    .execute()
print("✅ Done.")
