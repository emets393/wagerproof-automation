import os
from supabase import create_client, Client

# Supabase setup
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)

team_tables = [
    "arizona_games", "atlanta_games", "baltimore_games", "boston_games", "cubs_games", "white_sox_games",
    "cincinnati_games", "cleveland_games", "colorado_games", "detroit_games", "houston_games", "kansas_city_games",
    "angels_games", "dodgers_games", "miami_games", "milwaukee_games", "minnesota_games", "mets_games",
    "yankees_games", "athletics_games", "philadelphia_games", "pittsburgh_games", "san_diego_games",
    "san_francisco_games", "seattle_games", "st_louis_games", "tampa_bay_games", "texas_games", "toronto_games",
    "washington_games"
]

def generate_update_sql(table):
    return f"""
        UPDATE {table} AS t
        SET 
            series_overs = sub.series_overs,
            series_unders = sub.series_unders
        FROM (
            SELECT
                t1.unique_id,
                COUNT(CASE WHEN t2.ou_result = 1 THEN 1 END) AS series_overs,
                COUNT(CASE WHEN t2.ou_result = 0 THEN 1 END) AS series_unders
            FROM {table} t1
            LEFT JOIN {table} t2
                ON t1.series_id = t2.series_id
                AND t2.series_game_number < t1.series_game_number
            GROUP BY t1.unique_id
        ) AS sub
        WHERE t.unique_id = sub.unique_id;
    """

# Run all updates
for table in team_tables:
    print(f"🔁 Updating: {table}")
    try:
        sql = generate_update_sql(table)
        supabase.rpc("execute_raw_sql", {"sql": sql}).execute()
        print(f"✅ Updated {table}")
    except Exception as e:
        print(f"❌ Failed on {table}: {e}")
