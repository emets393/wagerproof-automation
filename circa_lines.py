# ------ Circa Lines for Sharp Betting Page ------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
import math
from datetime import datetime
from zoneinfo import ZoneInfo
from supabase import create_client, Client

# ---------- Supabase Setup ----------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)
TABLE_NAME = "circa_lines"


# ---------- Team Name Mapping ----------
team_name_map = {
    "Arizona Diamondbacks": "Arizona", "Atlanta Braves": "Atlanta",
    "Baltimore Orioles": "Baltimore", "Boston Red Sox": "Boston",
    "Chicago Cubs": "Cubs", "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Cincinnati", "Cleveland Guardians": "Cleveland",
    "Colorado Rockies": "Colorado", "Detroit Tigers": "Detroit",
    "Houston Astros": "Houston", "Kansas City Royals": "Kansas City",
    "Los Angeles Angels": "Angels", "Los Angeles Dodgers": "Dodgers",
    "Miami Marlins": "Miami", "Milwaukee Brewers": "Milwaukee",
    "Minnesota Twins": "Minnesota", "New York Mets": "Mets",
    "New York Yankees": "Yankees", "Athletics": "Athletics",
    "Philadelphia Phillies": "Philadelphia", "Pittsburgh Pirates": "Pittsburgh",
    "San Diego Padres": "San Diego", "San Francisco Giants": "San Francisco",
    "Seattle Mariners": "Seattle", "St. Louis Cardinals": "ST Louis",
    "Tampa Bay Rays": "Tampa Bay", "Texas Rangers": "Texas",
    "Toronto Blue Jays": "Toronto", "Washington Nationals": "Washington","ST Louis Cardinals": "ST Louis"
}

# ---------- Selenium Setup ----------
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

driver.get("https://data.vsin.com/betting-splits/?bookid=circa")
driver.implicitly_wait(10)
html = driver.page_source
driver.quit()

# ---------- Parse Table & Filter by Today’s Date ----------
soup = BeautifulSoup(html, 'html.parser')
eastern = ZoneInfo("America/New_York")
target_date = datetime.now(tz=eastern).date()
print(f"🎯 Target date (NY time): {target_date}")

# Map DOM positions to dates
date_pattern = re.compile(r'^[A-Za-z]+,?\s?[A-Za-z]{3}\s?\d{1,2}$')
date_map = {}
elements = list(soup.find_all())
for i, tag in enumerate(elements):
    text = tag.get_text(strip=True)
    if re.match(date_pattern, text):
        parts = re.split(r",\s*|\s+", text)
        if len(parts) >= 2:
            md = parts[-2] + " " + parts[-1]
            try:
                parsed = datetime.strptime(md, "%b %d").replace(year=target_date.year).date()
                date_map[i] = parsed
            except:
                pass

# Collect only today’s rows
table = soup.find("table", class_="freezetable")
rows = table.find_all("tr")
flat = elements
filtered = []
last_date = None
for row in rows[1:]:
    idx = flat.index(row)
    applicable = [v for k, v in date_map.items() if k < idx]
    if applicable:
        last_date = applicable[-1]
    if last_date != target_date:
        continue
    cols = [td.get_text(strip=True).replace('\xa0',' ') for td in row.find_all("td")]
    if len(cols) >= 10:
        filtered.append(cols[:10])

df = pd.DataFrame(filtered, columns=[
    "Matchup","Money","Handle","Bets","Total",
    "Handle_1","Bets_2","RL","Handle_3","Bets_4"
])

# ---------- Double-header & Matchup Cleanup ----------
df['raw_matchup'] = df['Matchup'].str.replace('History','',regex=False).str.strip()
df['game_number'] = (
    df['raw_matchup'].str.extract(r'\[GM\s*(\d)\]',expand=False)
        .fillna('1').astype(int)
)
df['clean_matchup'] = df['raw_matchup'].str.replace(r'\[GM\s*\d\]','',regex=True).str.strip()

def split_matchup(m):
    for full in team_name_map:
        if m.startswith(full):
            return pd.Series([full, m[len(full):].strip()])
    return pd.Series([None,None])

df[['Away_Full','Home_Full']] = df['clean_matchup'].apply(split_matchup)
df['Away_Team'] = df['Away_Full'].map(team_name_map)
df['Home_Team'] = df['Home_Full'].map(team_name_map)
df = df.dropna(subset=['Away_Team','Home_Team'])

# ---------- Robust Money Split ----------
def split_money(val):
    # normalize various dash chars to hyphen
    val = re.sub(r'[–—‑−]', '-', val)
    # keep only digits and signs
    pure = re.sub(r'[^\d+-]', '', val)
    # look for delimiter sign after first char
    idx_plus = pure.find('+',1)
    idx_minus = pure.rfind('-',1)
    idx = max(idx_plus, idx_minus)
    if idx > 0:
        try:
            return pd.Series([int(pure[:idx]), int(pure[idx:])])
        except:
            pass
    # fallback: split last 3 digits
    if len(pure) >= 6:
        try:
            return pd.Series([int(pure[:-3]), int(pure[-3:])])
        except:
            pass
    print(f"⚠️ Could not parse money from {val!r} → pure:{pure!r}")
    return pd.Series([None,None])

df[['Money_Away','Money_Home']] = df['Money'].apply(split_money)

# ---------- Percent & Run Line Splits ----------
def split_pct(val):
    parts = re.findall(r'\d+%?', val)
    clean = [p.rstrip('%') for p in parts]
    return pd.Series(clean[:2]) if len(clean)>=2 else pd.Series([None,None])

df[['Handle_Away','Handle_Home']]       = df['Handle'].apply(split_pct)
df[['Bets_Away','Bets_Home']]           = df['Bets'].apply(split_pct)
df[['RL_Handle_Away','RL_Handle_Home']] = df['Handle_3'].apply(split_pct)
df[['RL_Bets_Away','RL_Bets_Home']]     = df['Bets_4'].apply(split_pct)
df[['Total_Over_Handle','Total_Under_Handle']] = df['Handle_1'].apply(split_pct)
df[['Total_Over_Bets','Total_Under_Bets']]     = df['Bets_2'].apply(split_pct)

df[['RL_Away','RL_Home']] = df['RL'].apply(
    lambda v: pd.Series(re.findall(r'[+-]?\d+\.?\d*', v)[:2])
)

# ---------- Convert to Numeric ----------
for c in [
    'Handle_Away','Handle_Home','Bets_Away','Bets_Home',
    'RL_Handle_Away','RL_Handle_Home','RL_Bets_Away','RL_Bets_Home',
    'Total_Over_Handle','Total_Under_Handle','Total_Over_Bets','Total_Under_Bets'
]:
    df[c] = pd.to_numeric(df[c], errors='coerce') / 100

for c in ['Money_Away','Money_Home','RL_Away','RL_Home']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df['Money_Away'] = df['Money_Away'].round(0).astype("Int64")
df['Money_Home'] = df['Money_Home'].round(0).astype("Int64")

# ---------- Date & Unique ID ----------
df['date'] = target_date.strftime('%Y-%m-%d')
df['excel_date'] = (target_date - datetime(1899,12,30).date()).days
df['unique_id'] = (
    df['Home_Team'] + df['Away_Team'] +
    df['excel_date'].astype(str) +
    df['game_number'].astype(str)
)

# ---------- Prediction & Strength Logic ----------
df['circa_ml_prediction'] = df.apply(
    lambda r: r['Home_Team'] if r['Handle_Home'] > r['Handle_Away'] else r['Away_Team'], axis=1
)
df['circa_rl_prediction'] = df.apply(
    lambda r: r['Home_Team'] if r['RL_Handle_Home'] > r['RL_Handle_Away'] else r['Away_Team'], axis=1
)
df['circa_total_prediction'] = df.apply(
    lambda r: 'Over' if r['Total_Over_Handle'] > r['Total_Under_Handle'] else 'Under', axis=1
)

def strength_label(diff, threshold=0.30):
    return "Strong" if abs(diff) >= threshold else "Basic"

df['circa_ml_prediction_strength'] = df.apply(
    lambda r: strength_label(
        (r['Handle_Home'] - r['Bets_Home']) if r['circa_ml_prediction'] == r['Home_Team']
        else (r['Handle_Away'] - r['Bets_Away'])
    ), axis=1
)
df['circa_rl_prediction_strength'] = df.apply(
    lambda r: strength_label(
        (r['RL_Handle_Home'] - r['RL_Bets_Home']) if r['circa_rl_prediction'] == r['Home_Team']
        else (r['RL_Handle_Away'] - r['RL_Bets_Away'])
    ), axis=1
)
df['circa_total_prediction_strength'] = df.apply(
    lambda r: strength_label(
        (r['Total_Over_Handle'] - r['Total_Over_Bets']) if r['circa_total_prediction'] == 'Over'
        else (r['Total_Under_Handle'] - r['Total_Under_Bets'])
    ), axis=1
)

# ---------- Final Upsert ----------
df_final = df[[
    'unique_id','date','Away_Team','Home_Team',
    'Money_Away','Money_Home',
    'Handle_Away','Handle_Home','Bets_Away','Bets_Home',
    'RL_Away','RL_Home','RL_Handle_Away','RL_Handle_Home','RL_Bets_Away','RL_Bets_Home',
    'Total_Over_Handle','Total_Under_Handle','Total_Over_Bets','Total_Under_Bets',
    'circa_ml_prediction','circa_ml_prediction_strength',
    'circa_rl_prediction','circa_rl_prediction_strength',
    'circa_total_prediction','circa_total_prediction_strength'
]]
records = df_final.to_dict('records')
clean = [{k:(None if isinstance(v,float) and not math.isfinite(v) else v) for k,v in row.items()} for row in records]
supabase.table(TABLE_NAME).upsert(clean, on_conflict=['unique_id']).execute()
print("✅ Circa lines with predictions uploaded")

