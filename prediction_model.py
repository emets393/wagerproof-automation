import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
import numpy as np
from supabase import create_client, Client
from datetime import datetime
import uuid

# -----------------------------
# Supabase Setup
# -----------------------------
url = "https://gnjrklxotmbvnxbnnqgq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
supabase: Client = create_client(url, SUPABASE_KEY)

# -----------------------------
# Step 1: Fetch Data
# -----------------------------
today = datetime.today().date()

# Training Data
training_data_resp = supabase.table("training_data").select("*").execute()
training_data = pd.DataFrame(training_data_resp.data)
training_data = training_data[pd.to_datetime(training_data['date']).dt.date < today]

# Input Data
input_data_resp = supabase.table("input_values_view").select("*").execute()
input_data = pd.DataFrame(input_data_resp.data)

print(f"\U0001f4ca Total rows in training data before cleaning: {len(training_data)}")

# -----------------------------
# Step 2: Features and Targets
# -----------------------------
features = [
    'series_game_number', 'series_home_wins', 'series_away_wins', 'series_overs', 'series_unders', 'o_u_line',
    'home_ml', 'home_rl', 'home_ml_handle', 'home_ml_bets', 'home_rl_handle', 'home_rl_bets',
    'away_ml', 'away_rl', 'away_ml_handle', 'away_ml_bets', 'away_rl_handle', 'away_rl_bets',
    'ou_handle_over', 'ou_bets_over', 'same_division', 'same_league', 'home_whip', 'home_era',
    'away_whip', 'away_era', 'streak', 'away_streak', 'home_win_pct', 'away_win_pct',
    'home_ops_last_3', 'away_ops_last_3', 'home_team_last_3', 'away_team_last_3',
    'home_last_win', 'away_last_win', 'home_last_runs', 'away_last_runs',
    'home_last_runs_allowed', 'away_last_runs_allowed'
]

categorical_features = [
    'home_handedness', 'away_handedness', 'month', 'day', 'season',
    'home_team_number', 'away_team_number', 'away_pitcher_id', 'home_pitcher_id'
]

# Drop rows with missing values in input data
input_data = input_data.dropna(subset=features + categorical_features)
if input_data.empty:
    print("⚠️ No input rows left to predict after cleaning.")
    exit()

# Drop rows with missing values in training target columns
target_cols = ['ou_result', 'run_line_winner', 'ha_winner']
training_data = training_data.dropna(subset=target_cols)

# -----------------------------
# Step 3: Encode Categorical
# -----------------------------
encoders = {}
for col in categorical_features:
    training_data[col] = training_data[col].astype(str)
    le = LabelEncoder()
    training_data[col] = le.fit_transform(training_data[col])
    encoders[col] = le

    input_data[col] = input_data[col].astype(str)
    input_data[col] = input_data[col].apply(lambda x: le.transform([x])[0] if x in le.classes_ else -1)

# -----------------------------
# Step 4: Train/Test Split
# -----------------------------
X = training_data[features]
y_ou = training_data['ou_result']
y_rl = training_data['run_line_winner']
y_ha = training_data['ha_winner']

X_ou_train, X_ou_test, y_ou_train, y_ou_test = train_test_split(X, y_ou, test_size=0.3, stratify=y_ou, random_state=42)
X_rl_train, X_rl_test, y_rl_train, y_rl_test = train_test_split(X, y_rl, test_size=0.3, stratify=y_rl, random_state=42)
X_ha_train, X_ha_test, y_ha_train, y_ha_test = train_test_split(X, y_ha, test_size=0.3, stratify=y_ha, random_state=42)

# -----------------------------
# Step 5: Model + Tier Accuracy
# -----------------------------
xgb_config = {
    "use_label_encoder": False,
    "eval_metric": "logloss",
    "n_estimators": 100,
    "max_depth": 3,
    "learning_rate": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42
}

def calculate_tier_accuracy(y_true, probs):
    tiers = [(i / 10, (i + 1) / 10) for i in range(10)]
    results = []
    for low, high in tiers:
        mask = (probs >= low) & (probs < high)
        count = mask.sum()
        if count > 0:
            acc = accuracy_score(y_true[mask], probs[mask] >= 0.5)
            results.append((low, high, round(acc, 3), count))
        else:
            results.append((low, high, None, 0))
    return results

def get_tier_accuracy(prob, tier_accs):
    for low, high, acc, _ in tier_accs:
        if low <= prob < high:
            return acc
    return None

def get_prediction(prob, tier_acc, pos_label, neg_label):
    base = pos_label if prob >= 0.5 else neg_label
    if tier_acc is None:
        return base
    return base if tier_acc >= 0.5 else (neg_label if base == pos_label else pos_label)

def get_strength(tier_acc):
    return "strong" if tier_acc is not None and (tier_acc >= 0.55 or tier_acc <= 0.45) else "basic"

model_ou = xgb.XGBClassifier(**xgb_config)
model_ou.fit(X_ou_train, y_ou_train)
probs_ou = model_ou.predict_proba(X_ou_test)[:, 1]
tier_acc_ou = calculate_tier_accuracy(y_ou_test, probs_ou)

model_rl = xgb.XGBClassifier(**xgb_config)
model_rl.fit(X_rl_train, y_rl_train)
probs_rl = model_rl.predict_proba(X_rl_test)[:, 1]
tier_acc_rl = calculate_tier_accuracy(y_rl_test, probs_rl)

model_ha = xgb.XGBClassifier(**xgb_config)
model_ha.fit(X_ha_train, y_ha_train)
probs_ha = model_ha.predict_proba(X_ha_test)[:, 1]
tier_acc_ha = calculate_tier_accuracy(y_ha_test, probs_ha)

print("\n🔍 OU Tier Accuracy:")
for t in tier_acc_ou: print(t)
print("\n🔍 RL Tier Accuracy:")
for t in tier_acc_rl: print(t)
print("\n🔍 HA Tier Accuracy:")
for t in tier_acc_ha: print(t)

# -----------------------------
# Step 6: Make Predictions
# -----------------------------
X_input = input_data[features]
prob_ou = model_ou.predict_proba(X_input)[:, 1]
prob_rl = model_rl.predict_proba(X_input)[:, 1]
prob_ha = model_ha.predict_proba(X_input)[:, 1]

# -----------------------------
# Step 7: Format & Upload
# -----------------------------
results = []
for i in range(len(input_data)):
    row = input_data.iloc[i]
    ou_acc = get_tier_accuracy(prob_ou[i], tier_acc_ou)
    rl_acc = get_tier_accuracy(prob_rl[i], tier_acc_rl)
    ha_acc = get_tier_accuracy(prob_ha[i], tier_acc_ha)

    ou_pred = get_prediction(prob_ou[i], ou_acc, "Over", "Under")
    rl_pred = get_prediction(prob_rl[i], rl_acc, row['home_team'], row['away_team'])
    ha_pred = get_prediction(prob_ha[i], ha_acc, row['home_team'], row['away_team'])

    results.append({
        "id": str(uuid.uuid4()),
        "unique_id": row['unique_id'],
        "game_date": row['date'],
        "home_team": row['home_team'],
        "away_team": row['away_team'],
        "ou_probability": float(prob_ou[i]),
        "run_line_probability": float(prob_rl[i]),
        "ml_probability": float(prob_ha[i]),
        "ou_tier_accuracy": float(ou_acc) if ou_acc is not None else None,
        "run_line_tier_accuracy": float(rl_acc) if rl_acc is not None else None,
        "ml_tier_accuracy": float(ha_acc) if ha_acc is not None else None,
        "ou_prediction": ou_pred,
        "runline_prediction": rl_pred,
        "moneyline_prediction": ha_pred,
        "strong_ou_prediction": get_strength(ou_acc),
        "strong_runline_prediction": get_strength(rl_acc),
        "strong_ml_prediction": get_strength(ha_acc),
        "home_ml": float(row['home_ml']),
        "away_ml": float(row['away_ml']),
        "home_rl": float(row['home_rl']),
        "away_rl": float(row['away_rl']),
        "o_u_line": float(row['o_u_line']),
        "created_at": datetime.now().isoformat()
    })

supabase.table("daily_combined_predictions").insert(results).execute()
print(f"✅ Uploaded {len(results)} predictions to Supabase.")


