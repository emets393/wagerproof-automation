# ------ Circa Lines for Sharp Betting Page ------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
from datetime import datetime
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
    "Toronto Blue Jays": "Toronto", "Washington Nationals": "Washington"
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

# ---------- Parse Table ----------
soup = BeautifulSoup(html, 'html.parser')
table = soup.find("table", class_="freezetable")
rows = table.find_all("tr")

data = []
for row in rows[1:]:
    cols = [td.get_text(strip=True).replace('\xa0', ' ') for td in row.find_all("td")]
    if len(cols) >= 10:
        data.append(cols[:10])

df = pd.DataFrame(data, columns=[
    "Matchup", "Money", "Handle", "Bets", "Total",
    "Handle_1", "Bets_2", "RL", "Handle_3", "Bets_4"
])

# ---------- Split Matchup into Teams ----------
def split_matchup(matchup):
    for full in team_name_map:
        if matchup.startswith(full):
            away = full
            home = matchup.replace(full, '').strip()
            return pd.Series([away, home])
    return pd.Series([None, None])

df[['Away_Full', 'Home_Full']] = df['Matchup'].apply(lambda m: split_matchup(m.replace('History','').strip()))
df['Away_Team'] = df['Away_Full'].map(team_name_map)
df['Home_Team'] = df['Home_Full'].map(team_name_map)

# ---------- Split Money Line ----------
def split_money(val):
    val = val.replace('–','-').strip()
    parts = re.findall(r'-?\d+', val)
    return pd.Series([int(parts[0]), int(parts[1])]) if len(parts) == 2 else pd.Series([None, None])

df[['Money_Away','Money_Home']] = df['Money'].apply(split_money)

# ---------- Split Percent Columns ----------
def split_pct(val):
    parts = re.findall(r'\d+%?', val)
    clean = [p.rstrip('%') for p in parts]
    return pd.Series(clean[:2]) if len(clean) >= 2 else pd.Series([None, None])

df[['Handle_Away','Handle_Home']]       = df['Handle'].apply(split_pct)
df[['Bets_Away','Bets_Home']]           = df['Bets'].apply(split_pct)
df[['RL_Handle_Away','RL_Handle_Home']] = df['Handle_3'].apply(split_pct)
df[['RL_Bets_Away','RL_Bets_Home']]     = df['Bets_4'].apply(split_pct)
# Total: Over vs Under
df[['Total_Over_Handle','Total_Under_Handle']] = df['Handle_1'].apply(split_pct)
df[['Total_Over_Bets','Total_Under_Bets']]     = df['Bets_2'].apply(split_pct)

# ---------- Split Run Line ----------
def split_rl(val):
    parts = re.findall(r'[+-]?\d+\.?\d*', val)
    return pd.Series(parts[:2]) if len(parts) == 2 else pd.Series([None, None])

df[['RL_Away','RL_Home']] = df['RL'].apply(split_rl)

# ---------- Convert to Numeric ----------
pct_cols = [
    'Handle_Away','Handle_Home','Bets_Away','Bets_Home',
    'RL_Handle_Away','RL_Handle_Home','RL_Bets_Away','RL_Bets_Home',
    'Total_Over_Handle','Total_Under_Handle','Total_Over_Bets','Total_Under_Bets'
]
for c in pct_cols:
    df[c] = pd.to_numeric(df[c], errors='coerce') / 100

df['Money_Away'] = pd.to_numeric(df['Money_Away'], errors='coerce')
df['Money_Home'] = pd.to_numeric(df['Money_Home'], errors='coerce')
df['RL_Away']   = pd.to_numeric(df['RL_Away'],   errors='coerce')
df['RL_Home']   = pd.to_numeric(df['RL_Home'],   errors='coerce')

# ---------- Date + Excel Serial ----------
df['date'] = datetime.today().strftime('%Y-%m-%d')
def date_to_excel_serial(ds):
    d = datetime.strptime(ds, "%Y-%m-%d")
    base = datetime(1899,12,30)
    return (d - base).days

df['excel_date'] = df['date'].apply(date_to_excel_serial)

# ---------- Drop Invalid Team Rows ----------
df = df.dropna(subset=['Away_Team','Home_Team'])

# ---------- Unique ID ----------
df['unique_id'] = df['Home_Team'] + df['Away_Team'] + df['excel_date'].astype(str)

# ---------- Prediction Logic ----------
# Money Line
df['ml_prediction'] = df.apply(
    lambda r: r['Home_Team'] if r['Handle_Home'] > r['Handle_Away'] else r['Away_Team'],
    axis=1
)
df['ml_prediction_strength'] = df.apply(
    lambda r: abs(
        (r['Handle_Home'] - r['Bets_Home']) if r['Handle_Home'] > r['Handle_Away']
        else (r['Handle_Away'] - r['Bets_Away'])
    ),
    axis=1
)

# Run Line
df['rl_prediction'] = df.apply(
    lambda r: r['Home_Team'] if r['RL_Handle_Home'] > r['RL_Handle_Away'] else r['Away_Team'],
    axis=1
)
df['rl_prediction_strength'] = df.apply(
    lambda r: abs(
        (r['RL_Handle_Home'] - r['RL_Bets_Home']) if r['RL_Handle_Home'] > r['RL_Handle_Away']
        else (r['RL_Handle_Away'] - r['RL_Bets_Away'])
    ),
    axis=1
)

# ── Prediction logic for Total (Over/Under) ──
df['total_prediction'] = df.apply(
    lambda r: 'Over' if r['Total_Over_Handle'] > r['Total_Under_Handle'] else 'Under',
    axis=1
)
df['total_prediction_strength'] = df.apply(
    lambda r: abs(
        (r['Total_Over_Handle'] - r['Total_Over_Bets'])
        if r['Total_Over_Handle'] > r['Total_Under_Handle']
        else (r['Total_Under_Handle'] - r['Total_Under_Bets'])
    ),
    axis=1
)

# 1) Build the final payload
df_final = df[[
    'unique_id','date','Away_Team','Home_Team',
    'Money_Away','Money_Home',
    'Handle_Away','Handle_Home','Bets_Away','Bets_Home',
    'RL_Away','RL_Home','RL_Handle_Away','RL_Handle_Home','RL_Bets_Away','RL_Bets_Home',
    'Total_Over_Handle','Total_Under_Handle','Total_Over_Bets','Total_Under_Bets',
    'ml_prediction','ml_prediction_strength',
    'rl_prediction','rl_prediction_strength',
    'total_prediction','total_prediction_strength'
]]

# 2) Sanitize out inf/NaN so JSON is valid
import numpy as np
df_final = df_final.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df_final), None)

# 3) Single upsert
supabase.table(TABLE_NAME) \
    .upsert(df_final.to_dict('records'), on_conflict=['unique_id']) \
    .execute()

print("✅ Circa lines with predictions uploaded")
