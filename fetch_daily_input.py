import requests
from datetime import datetime
import json

def fetch_daily_games():
    today = datetime.today().strftime('%Y-%m-%d')

    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"
    res = requests.get(url)
    data = res.json()

    games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

    results = []
    for game in games:
        home = game['teams']['home']['team']['name']
        away = game['teams']['away']['team']['name']
        results.append({
            "date": today,
            "home_team": home,
            "away_team": away
        })

    return results

if __name__ == "__main__":
    games = fetch_daily_games()
    print(json.dumps({
        "games": games,
        "count": len(games)
    }, indent=2))
