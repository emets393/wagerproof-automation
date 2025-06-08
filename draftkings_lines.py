# --- DraftKings lines scraper for today's current date only ---

import re
import regex as re2
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
from supabase import create_client, Client

# ------------------ Supabase Setup ------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)
TABLE_NAME = "draftkings_lines"

# ------------------ Load VSIN Page ------------------
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# ✅ Use WebDriver Manager to ensure correct driver is used
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get("https://data.vsin.com/betting-splits/?bookid=dk&view=mlb")
time.sleep(10)
soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

# ------------------ Identify target date ------------------
target_date = datetime.today().date()
print(f"🎯 Target date: {target_date}")

# ------------------ Collect date tags in page ------------------
date_pattern = re.compile(r'^[A-Za-z]+,?\s?[A-Za-z]{3}\s?\d{1,2}$')
date_map = {}  # element index → date

elements = list(soup.find_all())
current_date = None

# Map DOM index to date
for i, tag in enumerate(elements):
    text = tag.get_text(strip=True)
    if re.match(date_pattern, text):
        try:
            parts = re.split(r",\s*|\s+", text)
            if len(parts) >= 2:
                md = parts[-2] + " " + parts[-1]
                parsed_date = datetime.strptime(md.strip(), "%b %d").replace(year=datetime.now().year).date()
                date_map[i] = parsed_date
        except:
            continue


# ------------------ Extract table rows and assign dates ------------------
table = soup.find("table", class_="freezetable")
rows = table.find_all("tr")

data = []
team_map = {
    "Arizona Diamondbacks": "Arizona", "Atlanta Braves": "Atlanta", "Baltimore Orioles": "Baltimore",
    "Boston Red Sox": "Boston", "Chicago Cubs": "Cubs", "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Cincinnati", "Cleveland Guardians": "Cleveland", "Colorado Rockies": "Colorado",
    "Detroit Tigers": "Detroit", "Houston Astros": "Houston", "Kansas City Royals": "Kansas City",
    "Los Angeles Angels": "Angels", "Los Angeles Dodgers": "Dodgers", "Miami Marlins": "Miami",
    "Milwaukee Brewers": "Milwaukee", "Minnesota Twins": "Minnesota", "New York Mets": "Mets",
    "New York Yankees": "Yankees", "Athletics": "Athletics", "Philadelphia Phillies": "Philadelphia",
    "Pittsburgh Pirates": "Pittsburgh", "San Diego Padres": "San Diego", "San Francisco Giants": "San Francisco",
    "Seattle Mariners": "Seattle", "Tampa Bay Rays": "Tampa Bay",
    "Texas Rangers": "Texas", "Toronto Blue Jays": "Toronto", "Washington Nationals": "Washington",
    "ST Louis Cardinals": "ST Louis"
}
sorted_teams = sorted(team_map.keys(), key=len, reverse=True)

# Track current section date based on position in DOM
last_known_date = None
flat_elements = list(soup.find_all())

for row in rows:
    cols = row.find_all("td")
    if len(cols) < 10:
        continue

    # Find index of this row in the flat element list
    row_index = flat_elements.index(row)

    # Get closest preceding date label
    applicable_dates = [v for k, v in date_map.items() if k < row_index]
    if applicable_dates:
        last_known_date = applicable_dates[-1]

    if last_known_date != target_date:
        continue  # skip rows not matching the desired date

    cells = [col.get_text(strip=True) for col in cols]
    raw_matchup = re.sub(r'History.*', '', cells[0]).strip()
    gm_match = re.search(r'\[GM\s*(\d)\]', raw_matchup)
    game_number = int(gm_match.group(1)) if gm_match else 1
    raw_matchup = re.sub(r'\[GM\s*\d\]', '', raw_matchup).strip()

    home_team = away_team = None
    for team1 in sorted_teams:
        if raw_matchup.startswith(team1):
            away_team = team_map[team1]
            rest = raw_matchup[len(team1):].strip()
            for team2 in sorted_teams:
                if rest.startswith(team2):
                    home_team = team_map[team2]
                    break
            break

    if not home_team or not away_team:
        print(f"⚠️ Skipping unparsed matchup: '{raw_matchup}'")
        continue

    # Money lines
    money_away, money_home = None, None
    match = re.match(r'([+-]?\d{3})([+-]?\d{3})', cells[1])
    if match:
        money_away, money_home = int(match.group(1)), int(match.group(2))

    def split_pct(val):
        parts = re.findall(r'(\d+)%', val)
        return [float(p)/100 for p in parts] if len(parts) == 2 else [None, None]

    handle_away, handle_home = split_pct(cells[2])
    bets_away, bets_home = split_pct(cells[3])
    ou_handle_over_vals = split_pct(cells[5])
    ou_bets_over_vals = split_pct(cells[6])
    ou_handle_over = ou_handle_over_vals[0] if ou_handle_over_vals else None
    ou_bets_over = ou_bets_over_vals[0] if ou_bets_over_vals else None

    raw_total = cells[4]
    matches = re2.findall(r'(?=(\d{1,2}(?:\.\d)?))', raw_total)
    valid_lines = [float(m) for m in matches if 3 <= float(m) <= 20]
    total_line = valid_lines[0] if valid_lines else None

    rl_away, rl_home = map(float, re.findall(r'[+-]?\d+(?:\.\d+)?', cells[7])) if re.findall(r'[+-]?\d+(?:\.\d+)?', cells[7]) else (None, None)
    rl_handle_away, rl_handle_home = split_pct(cells[8])
    rl_bets_away, rl_bets_home = split_pct(cells[9])

    date_str = last_known_date.strftime("%Y-%m-%d")
    excel_numeric_date = (last_known_date - datetime(1899, 12, 30).date()).days
    unique_id = f"{home_team}{away_team}{excel_numeric_date}{game_number}"
    import_time = datetime.now().isoformat()

    data.append({
        "unique_id": unique_id,
        "date": date_str,
        "away_team": away_team,
        "home_team": home_team,
        "money_away": money_away,
        "money_home": money_home,
        "handle_away": handle_away,
        "handle_home": handle_home,
        "bets_away": bets_away,
        "bets_home": bets_home,
        "o_u_line": total_line,
        "ou_handle_over": ou_handle_over,
        "ou_bets_over": ou_bets_over,
        "rl_away": rl_away,
        "rl_home": rl_home,
        "rl_handle_away": rl_handle_away,
        "rl_handle_home": rl_handle_home,
        "rl_bets_away": rl_bets_away,
        "rl_bets_home": rl_bets_home,
        "import_time": import_time
    })

# ------------------ Upload to Supabase ------------------
print(f"🧹 Deleting old rows from `{TABLE_NAME}`...")
supabase.table(TABLE_NAME).delete().neq("unique_id", "").execute()

data = [row for row in data if all(v is not None for v in row.values())]
if not data:
    print("⚠️ No clean rows to insert. Skipping upload.")
else:
    print(f"⬆️ Inserting {len(data)} clean rows into `{TABLE_NAME}`...")
    response = supabase.table(TABLE_NAME).insert(data).execute()
    if hasattr(response, "data"):
        print("✅ Upload successful.")
    else:
        print(f"❌ Upload failed: {response}")