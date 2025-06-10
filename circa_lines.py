# ------ Circa Lines for Sharp Betting Page ------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from supabase import create_client, Client

# ---------- Supabase Setup ----------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzc1OTQ0NywiZXhwIjoyMDYzMzM1NDQ3fQ.sMh6lWpp3OLvwJLZft0CqS5nyMNo8xuxQcL4GLOXZ4w"
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
    "ST Louis Cardinals": "ST Louis", "Tampa Bay Rays": "Tampa Bay",
    "Texas Rangers": "Texas", "Toronto Blue Jays": "Toronto",
    "Washington Nationals": "Washington"
}

# ---------- Selenium Setup ----------
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

url = "https://data.vsin.com/betting-splits/?bookid=circa"
driver.get(url)
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
    if len(cols) >= 9:
        data.append(cols[:10])

df = pd.DataFrame(data, columns=[
    "Matchup", "Money", "Handle", "Bets", "Total",
    "Handle_1", "Bets_2", "RL", "Handle_3", "Bets_4"
])

df['Matchup'] = df['Matchup'].str.replace('History', '', regex=False).str.strip()

# ---------- Split Matchup into Teams ----------
def split_matchup(matchup):
    for full_name in team_name_map:
        if matchup.startswith(full_name):
            away = full_name
            home = matchup.replace(full_name, '').strip()
            return pd.Series([away, home])
    return pd.Series([None, None])

df[['Away_Full', 'Home_Full']] = df['Matchup'].apply(split_matchup)
df['Away_Team'] = df['Away_Full'].map(team_name_map)
df['Home_Team'] = df['Home_Full'].map(team_name_map)

# ---------- Split Money Line ----------
def split_money(val):
    if not isinstance(val, str) or len(val) < 6:
        return pd.Series([None, None])
    val = val.replace('–', '-').strip()
    parts = re.findall(r'-?\d{3}', val)
    if len(parts) == 2:
        return pd.Series([int(parts[0]), int(parts[1])])
    if val.count('-') == 1 and len(val) == 7:
        return pd.Series([int(val[:3]), int(val[3:])])
    return pd.Series([None, None])

df[['Money_Away', 'Money_Home']] = df['Money'].apply(split_money)

# ---------- Split Percent Columns ----------
def split_pct(val):
    parts = re.split(r'%\-?', val)
    return pd.Series([p.strip() if p else None for p in parts[:2]])

df[['Handle_Away', 'Handle_Home']] = df['Handle'].apply(split_pct)
df[['Bets_Away', 'Bets_Home']] = df['Bets'].apply(split_pct)
df[['RL_Handle_Away', 'RL_Handle_Home']] = df['Handle_3'].apply(split_pct)
df[['RL_Bets_Away', 'RL_Bets_Home']] = df['Bets_4'].apply(split_pct)

# ---------- Split Run Line ----------
def split_rl(val):
    parts = re.findall(r'[+-]?\d+\.?\d*', val)
    return pd.Series(parts[:2] if len(parts) == 2 else [None, None])

df[['RL_Away', 'RL_Home']] = df['RL'].apply(split_rl)

# ---------- Convert to Numeric ----------
percent_cols = [
    'Handle_Away', 'Handle_Home', 'Bets_Away', 'Bets_Home',
    'RL_Handle_Away', 'RL_Handle_Home', 'RL_Bets_Away', 'RL_Bets_Home'
]
for col in percent_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce') / 100

df['Money_Away'] = pd.to_numeric(df['Money_Away'], errors='coerce')
df['Money_Home'] = pd.to_numeric(df['Money_Home'], errors='coerce')
df['RL_Away'] = pd.to_numeric(df['RL_Away'], errors='coerce')
df['RL_Home'] = pd.to_numeric(df['RL_Home'], errors='coerce')

# ---------- Date + Excel Serial ----------
df['date'] = datetime.today().strftime('%Y-%m-%d')

def date_to_excel_serial(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    base_date = datetime(1899, 12, 30)
    return (date_obj - base_date).days

df['excel_date'] = df['date'].apply(date_to_excel_serial)

# ---------- Drop Invalid Team Rows ----------
df = df.dropna(subset=['Home_Team', 'Away_Team'])

# ---------- Unique ID ----------
df['unique_id'] = df['Home_Team'] + df['Away_Team'] + df['excel_date'].astype(str)

# ---------- Final Columns ----------
final_columns = [
    'unique_id', 'date', 'Away_Team', 'Home_Team',
    'Money_Away', 'Money_Home',
    'Handle_Away', 'Handle_Home', 'Bets_Away', 'Bets_Home',
    'RL_Away', 'RL_Home', 'RL_Handle_Away', 'RL_Handle_Home',
    'RL_Bets_Away', 'RL_Bets_Home'
]
df_final = df[final_columns]

# ---------- Convert Money Lines to Int ----------
df_final[['Money_Away', 'Money_Home']] = df_final[['Money_Away', 'Money_Home']].round(0).astype("Int64")

# ---------- Upload to Supabase ----------
records = df_final.to_dict(orient="records")
supabase.table(TABLE_NAME).upsert(records, on_conflict=["unique_id"]).execute()
print("✅ Data uploaded to Supabase")
