import os
from supabase import create_client, Client

# -----------------------------
# Supabase setup
# -----------------------------
url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

# -----------------------------
# List of all team tables
# -----------------------------
team_tables = [
    "arizona_games", "atlanta_games", "baltimore_games", "boston_games", "cubs_games", "white_sox_games",
    "cincinnati_games", "cleveland_games", "colorado_games", "detroit_games", "houston_games", "kansas_city_games",
    "angels_games", "dodgers_games", "miami_games", "milwaukee_games", "minnesota_games", "mets_games",
    "yankees_games", "athletics_games", "philadelphia_games", "pittsburgh_games", "san_diego_games",
    "san_francisco_games", "seattle_games", "st_louis_games", "tampa_bay_games", "texas_games", "toronto_games",
    "washington_games"
]

# -----------------------------
# Run each update statement
# -----------------------------
for table in team_tables:
    print(f"🔄 Updating {table}")
    sql = f"""
        UPDATE {table}
        SET ou_result = td.ou_result
        FROM training_data td
        WHERE {table}.join_table_string = td.unique_id;
    """
    try:
        supabase.rpc("execute_raw_sql", {"sql": sql}).execute()
        print(f"✅ {table} updated")
    except Exception as e:
        print(f"❌ Failed to update {table}: {e}")
