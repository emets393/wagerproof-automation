#----Teamrankings schedules and results and insert into supabase

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client, Client
import traceback

# -----------------------------
# Supabase credentials
# -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzc1OTQ0NywiZXhwIjoyMDYzMzM1NDQ3fQ.sMh6lWpp3OLvwJLZft0CqS5nyMNo8xuxQcL4GLOXZ4w"
supabase: Client = create_client(url, SUPABASE_KEY)

# -----------------------------
# Team mapping manually defined
# -----------------------------
team_configs = [
    {"teamrankings": "Arizona", "short_name": "Arizona", "table": "arizona_games", "url": "https://www.teamrankings.com/mlb/team/arizona-diamondbacks"},
    {"teamrankings": "Atlanta", "short_name": "Atlanta", "table": "atlanta_games", "url": "https://www.teamrankings.com/mlb/team/Atlanta-Braves"},
    {"teamrankings": "Baltimore", "short_name": "Baltimore", "table": "baltimore_games", "url": "https://www.teamrankings.com/mlb/team/Baltimore-Orioles"},
    {"teamrankings": "Boston", "short_name": "Boston", "table": "boston_games", "url": "https://www.teamrankings.com/mlb/team/Boston-Red-Sox"},
    {"teamrankings": "Chi Cubs", "short_name": "Cubs", "table": "cubs_games", "url": "https://www.teamrankings.com/mlb/team/Chicago-Cubs"},
    {"teamrankings": "Chi Sox", "short_name": "White Sox", "table": "white_sox_games", "url": "https://www.teamrankings.com/mlb/team/Chicago-White-Sox"},
    {"teamrankings": "Cincinnati", "short_name": "Cincinnati", "table": "cincinnati_games", "url": "https://www.teamrankings.com/mlb/team/Cincinnati-Reds"},
    {"teamrankings": "Cleveland", "short_name": "Cleveland", "table": "cleveland_games", "url": "https://www.teamrankings.com/mlb/team/Cleveland-Guardians"},
    {"teamrankings": "Colorado", "short_name": "Colorado", "table": "colorado_games", "url": "https://www.teamrankings.com/mlb/team/Colorado-Rockies"},
    {"teamrankings": "Detroit", "short_name": "Detroit", "table": "detroit_games", "url": "https://www.teamrankings.com/mlb/team/Detroit-Tigers"},
    {"teamrankings": "Houston", "short_name": "Houston", "table": "houston_games", "url": "https://www.teamrankings.com/mlb/team/Houston-Astros"},
    {"teamrankings": "Kansas City", "short_name": "Kansas City", "table": "kansas_city_games", "url": "https://www.teamrankings.com/mlb/team/Kansas-City-Royals"},
    {"teamrankings": "LA Angels", "short_name": "Angels", "table": "angels_games", "url": "https://www.teamrankings.com/mlb/team/Los-Angeles-Angels"},
    {"teamrankings": "LA Dodgers", "short_name": "Dodgers", "table": "dodgers_games", "url": "https://www.teamrankings.com/mlb/team/Los-Angeles-Dodgers"},
    {"teamrankings": "Miami", "short_name": "Miami", "table": "miami_games", "url": "https://www.teamrankings.com/mlb/team/Miami-Marlins"},
    {"teamrankings": "Milwaukee", "short_name": "Milwaukee", "table": "milwaukee_games", "url": "https://www.teamrankings.com/mlb/team/Milwaukee-Brewers"},
    {"teamrankings": "Minnesota", "short_name": "Minnesota", "table": "minnesota_games", "url": "https://www.teamrankings.com/mlb/team/Minnesota-Twins"},
    {"teamrankings": "NY Mets", "short_name": "Mets", "table": "mets_games", "url": "https://www.teamrankings.com/mlb/team/New-York-Mets"},
    {"teamrankings": "NY Yankees", "short_name": "Yankees", "table": "yankees_games", "url": "https://www.teamrankings.com/mlb/team/New-York-Yankees"},
    {"teamrankings": "Sacramento", "short_name": "Athletics", "table": "athletics_games", "url": "https://www.teamrankings.com/mlb/team/Sacramento-Athletics"},
    {"teamrankings": "Philadelphia", "short_name": "Philadelphia", "table": "philadelphia_games", "url": "https://www.teamrankings.com/mlb/team/Philadelphia-Phillies"},
    {"teamrankings": "Pittsburgh", "short_name": "Pittsburgh", "table": "pittsburgh_games", "url": "https://www.teamrankings.com/mlb/team/Pittsburgh-Pirates"},
    {"teamrankings": "San Diego", "short_name": "San Diego", "table": "san_diego_games", "url": "https://www.teamrankings.com/mlb/team/San-Diego-Padres"},
    {"teamrankings": "SF Giants", "short_name": "San Francisco", "table": "san_francisco_games", "url": "https://www.teamrankings.com/mlb/team/San-Francisco-Giants"},
    {"teamrankings": "Seattle", "short_name": "Seattle", "table": "seattle_games", "url": "https://www.teamrankings.com/mlb/team/Seattle-Mariners"},
    {"teamrankings": "St. Louis", "short_name": "ST Louis", "table": "st_louis_games", "url": "https://www.teamrankings.com/mlb/team/St-Louis-Cardinals"},
    {"teamrankings": "Tampa Bay", "short_name": "Tampa Bay", "table": "tampa_bay_games", "url": "https://www.teamrankings.com/mlb/team/Tampa-Bay-Rays"},
    {"teamrankings": "Texas", "short_name": "Texas", "table": "texas_games", "url": "https://www.teamrankings.com/mlb/team/Texas-Rangers"},
    {"teamrankings": "Toronto", "short_name": "Toronto", "table": "toronto_games", "url": "https://www.teamrankings.com/mlb/team/Toronto-Blue-Jays"},
    {"teamrankings": "Washington", "short_name": "Washington", "table": "washington_games", "url": "https://www.teamrankings.com/mlb/team/Washington-Nationals"}
]


teamrankings_to_short = {config["teamrankings"]: config["short_name"] for config in team_configs}

def parse_date(val):
    try:
        if isinstance(val, str) and "/" in val:
            m, d = map(int, val.split("/"))
            return datetime(2025, m, d)
        return pd.to_datetime(val)
    except:
        return pd.NaT

def clean_record(rec):
    cleaned = {}
    for k, v in rec.items():
        if isinstance(v, float) and (np.isnan(v) or pd.isna(v)):
            cleaned[k] = None
        elif v == "NaN":
            cleaned[k] = None
        elif isinstance(v, float) and v.is_integer():
            cleaned[k] = int(v)
        else:
            cleaned[k] = v
    return cleaned

def generate_unique_id(row):
    if row["was_home"]:
        return f"{row['team']}{row['opponent']}{row['excel_date']}_{row['series_game_number']}"
    else:
        return f"{row['opponent']}{row['team']}{row['excel_date']}_{row['series_game_number']}"

for config in team_configs:
    print(f"\n🪜 Scraping: {config['table']}")
    try:
        response = requests.get(config["url"])
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", class_="tr-table datatable scrollable")
        if table is None:
            print(f"⚠️ No data table found for {config['table']}")
            continue

        headers = [th.text.strip() for th in table.find("thead").find_all("th")]
        rows = [[td.text.strip() for td in tr.find_all("td")] for tr in table.find("tbody").find_all("tr")]
        df = pd.DataFrame(rows, columns=headers)

        df = df.rename(columns={"Date": "date", "Opponent": "opponent", "Location": "location", "Result": "result"})
        df["date"] = df["date"].apply(parse_date)
        df["excel_date"] = df["date"].apply(lambda d: (d.date() - datetime(1899, 12, 30).date()).days if pd.notnull(d) else None)

        score_parts = df["result"].str.extract(r'([WL])\s?(\d+)-(\d+)')
        df["win_loss"] = score_parts[0]
        df["team_score"] = pd.to_numeric(score_parts[1], errors="coerce")
        df["opponent_score"] = pd.to_numeric(score_parts[2], errors="coerce")

        df["team"] = config["teamrankings"]
        df["opponent"] = df["opponent"].str.replace("\xa0", " ").str.strip()

        df["team_short"] = df["team"].map(teamrankings_to_short)
        df["opponent_short"] = df["opponent"].map(teamrankings_to_short).fillna(df["opponent"])

        def resolve_was_home(row):
            loc = row["location"].strip().capitalize()
            if loc == "Home":
                return True
            elif loc == "Away":
                return False
            else:
                return row["team_short"] < row["opponent_short"]

        df["was_home"] = df.apply(resolve_was_home, axis=1)

        def compute_join_key(row):
            home = row["team_short"] if row["was_home"] else row["opponent_short"]
            away = row["opponent_short"] if row["was_home"] else row["team_short"]
            return home + away + str(row["excel_date"])

        # ✅ Add daily_game_number so that every game gets 1, or 1/2 for doubleheaders
        df["daily_game_number"] = df.groupby("excel_date").cumcount() + 1

        def compute_join_key_with_game_number(row):
            home = row["team_short"] if row["was_home"] else row["opponent_short"]
            away = row["opponent_short"] if row["was_home"] else row["team_short"]
            return home + away + str(row["excel_date"]) + str(row["daily_game_number"])

        df["join_table_string"] = df.apply(compute_join_key_with_game_number, axis=1)


        df["game_number"] = df.reset_index().index + 1
        df["is_playoff"] = df["game_number"].apply(lambda x: 1 if x > 162 else 0)

        df["prev_opponent"] = df["opponent"].shift(1)
        df["new_series"] = (df["opponent"] != df["prev_opponent"]).astype(int)
        df["series_id"] = config["short_name"][:3].upper() + "_" + df["new_series"].cumsum().astype(str)
        df["series_game_number"] = df.groupby("series_id").cumcount() + 1
        df["home_win"] = ((df["win_loss"] == "W") & (df["was_home"])).astype(int)

        # ✅ Correct pre-game series_home_wins
        # Total wins by the team (home or away), shifted so it's pre-game
        df["team_win"] = (df["win_loss"] == "W").astype(int)
        df["series_home_wins"] = (
            df.groupby("series_id")["team_win"]
              .transform(lambda x: x.shift(1, fill_value=0).cumsum())
)


        # ✅ Correct pre-game streak logic
        streaks = []
        streak = 0
        last_result = None
        for result in df["win_loss"]:
            if result == last_result:
                streak += 1 if result == "W" else -1
            else:
                streak = 1 if result == "W" else -1
            last_result = result
            streaks.append(streak)
        df["streak"] = pd.Series(streaks).shift(1).fillna(0).astype(int)

        df["last_win"] = df["win_loss"].shift(1).map({"W": 1, "L": 0})
        df["last_runs"] = df["team_score"].shift(1)
        df["last_runs_allowed"] = df["opponent_score"].shift(1)

        df["date"] = df["date"].astype(str)
        df["unique_id"] = df.apply(generate_unique_id, axis=1)

        final = df[[
            "team", "opponent", "date", "excel_date", "was_home",
            "team_score", "opponent_score", "win_loss", "unique_id",
            "join_table_string", "is_playoff", "series_id", "series_game_number",
            "series_home_wins", "streak", "last_win", "last_runs", "last_runs_allowed"
        ]].copy()

        records = [clean_record(r) for r in final.to_dict(orient="records")]
        supabase.table(config["table"]).delete().neq("team", "").execute()
        supabase.table(config["table"]).insert(records).execute()
        print(f"✅ Uploaded: {config['table']}")

    except Exception as e:
        print(f"❌ Error uploading {config['table']}: {e}")
        traceback.print_exc()