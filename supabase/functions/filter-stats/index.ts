import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js";

// ─── Hard-coded Supabase client ─────────────────────────────────────────────
const supabase = createClient(
  "https://gnjrklxotmbvnxbnnqgq.supabase.co",
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
);
// ───────────────────────────────────────────────────────────────────────────────

serve(async (req) => {
  const url = new URL(req.url);
  const view = url.searchParams.get("view") ?? "game_summary";

  let source: string;
  let selectCols: string;

  switch (view) {
    case "team_monthly":
      source = "team_monthly_summary";
      selectCols = `
        team, season, month, total_games,
        win_pct, loss_pct,
        runline_win_pct, runline_loss_pct,
        over_pct, under_pct
      `;
      break;

    case "game_summary":
      source = "training_data_summary";
      selectCols = `
        home_team, away_team, series_game_number,
        series_home_wins, series_away_wins,
        series_overs, series_unders, o_u_line,
        home_ml, away_ml, home_rl, away_rl,
        home_ml_handle, away_ml_handle,
        home_ml_bets, away_ml_bets,
        home_rl_handle, away_rl_handle,
        home_rl_bets, away_rl_bets,
        ou_handle_over, ou_bets_over,
        same_division, same_league,
        streak, away_streak,
        home_win_pct, away_win_pct,
        home_last_win, away_last_win,
        home_last_runs, away_last_runs,
        home_last_runs_allowed, away_last_runs_allowed,
        home_ops_last_3, away_ops_last_3,
        home_team_last_3, away_team_last_3,
        season, month, day,
        home_pitcher, away_pitcher,
        home_whip, away_whip,
        home_era, away_era,
        home_handedness, away_handedness,
        win_pct, loss_pct,
        runline_win_pct, runline_loss_pct,
        over_pct, under_pct
      `;
      break;

    case "raw":
      source = "training_data";
      selectCols = `*`;
      break;

    default:
      return new Response(
        JSON.stringify({ error: "Invalid view parameter" }),
        { status: 400 }
      );
  }

  let query = supabase.from(source).select(selectCols);

  // special OR logic for ERA in raw mode
  if (view === "raw" && url.searchParams.has("era_min")) {
    const min = Number(url.searchParams.get("era_min"));
    query = query.or(`home_era.gte.${min},away_era.gte.${min}`);
  }

  // filter definitions
  const filterConfigs = [
    { param: "team",            column: view==="team_monthly"? "team" : undefined, op: "ilike" },
    { param: "home_team",       op: "ilike" },
    { param: "away_team",       op: "ilike" },
    { param: "series",          column: "series_game_number", op: "eq", cast: v=>Number(v) },
    { param: "series_home_wins",op: "eq",    cast: v=>Number(v) },
    { param: "series_away_wins",op: "eq",    cast: v=>Number(v) },
    { param: "series_overs",    op: "eq",    cast: v=>Number(v) },
    { param: "series_unders",   op: "eq",    cast: v=>Number(v) },
    { param: "o_u_line",        op: "eq",    cast: v=>Number(v) },
    { param: "home_ml",         op: "eq",    cast: v=>Number(v) },
    { param: "away_ml",         op: "eq",    cast: v=>Number(v) },
    { param: "home_rl",         op: "eq",    cast: v=>Number(v) },
    { param: "away_rl",         op: "eq",    cast: v=>Number(v) },
    { param: "home_ml_handle",  op: "eq",    cast: v=>Number(v) },
    { param: "away_ml_handle",  op: "eq",    cast: v=>Number(v) },
    { param: "home_ml_bets",    op: "eq",    cast: v=>Number(v) },
    { param: "away_ml_bets",    op: "eq",    cast: v=>Number(v) },
    { param: "home_rl_handle",  op: "eq",    cast: v=>Number(v) },
    { param: "away_rl_handle",  op: "eq",    cast: v=>Number(v) },
    { param: "home_rl_bets",    op: "eq",    cast: v=>Number(v) },
    { param: "away_rl_bets",    op: "eq",    cast: v=>Number(v) },
    { param: "ou_handle_over",  op: "eq",    cast: v=>Number(v) },
    { param: "ou_bets_over",    op: "eq",    cast: v=>Number(v) },
    { param: "same_division",   op: "eq",    cast: v=>Number(v) },
    { param: "same_league",     op: "eq",    cast: v=>Number(v) },
    { param: "streak",          op: "eq",    cast: v=>Number(v) },
    { param: "away_streak",     op: "eq",    cast: v=>Number(v) },
    { param: "home_win_pct",    op: "eq",    cast: v=>Number(v) },
    { param: "away_win_pct",    op: "eq",    cast: v=>Number(v) },
    { param: "home_last_win",   op: "e

