import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js";

// Hard‑coded Supabase client
const supabase = createClient(
  "https://gnjrklxotmbvnxbnnqgq.supabase.co",
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImduanJrbHhvdG1idm54Ym5ucWdxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTQwMzM5MywiZXhwIjoyMDY0OTc5MzkzfQ.RGi1Br_luWhexvBJJC1AaMSEMHJGl9Li_NUlwiUshsA"
);

serve(async (req) => {
  const url = new URL(req.url);

  // Base query: pull raw indicator columns and exact count
  let query = supabase
    .from("training_data")
    .select("ha_winner,run_line_winner,ou_result", { count: "exact" });

  // ERA filter matches either pitcher
  if (url.searchParams.has("era_min")) {
    const v = Number(url.searchParams.get("era_min"));
    query = query.or(`home_era.gte.${v},away_era.gte.${v}`);
  }

  // All user‑requested filters
  const filterConfigs = [
    { param: "home_team",          op: "ilike"   },
    { param: "away_team",          op: "ilike"   },
    { param: "series_game_number", op: "eq",    cast: v => Number(v) },
    { param: "series_home_wins",   op: "eq",    cast: v => Number(v) },
    { param: "series_away_wins",   op: "eq",    cast: v => Number(v) },
    { param: "series_overs",       op: "eq",    cast: v => Number(v) },
    { param: "series_unders",      op: "eq",    cast: v => Number(v) },
    { param: "o_u_line",           op: "eq",    cast: v => Number(v) },
    { param: "home_ml",            op: "eq",    cast: v => Number(v) },
    { param: "home_rl",            op: "eq",    cast: v => Number(v) },
    { param: "home_ml_handle",     op: "eq",    cast: v => Number(v) },
    { param: "home_ml_bets",       op: "eq",    cast: v => Number(v) },
    { param: "home_rl_handle",     op: "eq",    cast: v => Number(v) },
    { param: "home_rl_bets",       op: "eq",    cast: v => Number(v) },
    { param: "away_ml",            op: "eq",    cast: v => Number(v) },
    { param: "away_rl",            op: "eq",    cast: v => Number(v) },
    { param: "away_ml_handle",     op: "eq",    cast: v => Number(v) },
    { param: "away_ml_bets",       op: "eq",    cast: v => Number(v) },
    { param: "away_rl_handle",     op: "eq",    cast: v => Number(v) },
    { param: "away_rl_bets",       op: "eq",    cast: v => Number(v) },
    { param: "ou_handle_over",     op: "eq",    cast: v => Number(v) },
    { param: "ou_bets_over",       op: "eq",    cast: v => Number(v) },
    { param: "same_division",      op: "eq",    cast: v => Number(v) },
    { param: "same_league",        op: "eq",    cast: v => Number(v) },
    { param: "streak",             op: "eq",    cast: v => Number(v) },
    { param: "away_streak",        op: "eq",    cast: v => Number(v) },
    { param: "home_win_pct",       op: "eq",    cast: v => Number(v) },
    { param: "away_win_pct",       op: "eq",    cast: v => Number(v) },
    { param: "home_last_win",      op: "eq",    cast: v => Number(v) },
    { param: "away_last_win",      op: "eq",    cast: v => Number(v) },
    { param: "home_last_runs",     op: "eq",    cast: v => Number(v) },
    { param: "away_last_runs",     op: "eq",    cast: v => Number(v) },
    { param: "home_last_runs_allowed", op: "eq", cast: v => Number(v) },
    { param: "away_last_runs_allowed", op: "eq", cast: v => Number(v) },
    { param: "home_ops_last_3",    op: "eq",    cast: v => Number(v) },
    { param: "away_ops_last_3",    op: "eq",    cast: v => Number(v) },
    { param: "home_team_last_3",   op: "eq",    cast: v => Number(v) },
    { param: "away_team_last_3",   op: "eq",    cast: v => Number(v) },
    { param: "season",             op: "eq",    cast: v => Number(v) },
    { param: "month",              op: "eq",    cast: v => Number(v) },
    { param: "day",                op: "eq",    cast: v => Number(v) },
    { param: "home_pitcher",       op: "ilike" },
    { param: "home_whip",          op: "eq",    cast: v => Number(v) },
    { param: "home_era",           op: "eq",    cast: v => Number(v) },
    { param: "away_pitcher",       op: "ilike" },
    { param: "away_whip",          op: "eq",    cast: v => Number(v) },
    { param: "away_era",           op: "eq",    cast: v => Number(v) },
    { param: "home_handedness",    op: "eq",    cast: v => Number(v) },
    { param: "away_handedness",    op: "eq",    cast: v => Number(v) },
  ];

  // apply filters dynamically
  for (const { param, column, op, cast } of filterConfigs) {
    if (!url.searchParams.has(param)) continue;
    const raw = url.searchParams.get(param)!;
    const val = cast ? cast(raw) : raw;
    const col = column ?? param;
    if (op === "eq") query = query.eq(col, val as any);
    else query = query.ilike(col, `%${val}%`);
  }

  // execute
  const { data, count, error } = await query;
  if (error) return new Response(JSON.stringify({ error: error.message }), { status: 500 });

  // compute metrics
  const total = count || 0;
  let win = 0, loss = 0, runWin = 0, runLoss = 0, ov = 0, un = 0;
  for (const r of data) {
    r.ha_winner ? win++ : loss++;
    r.run_line_winner ? runWin++ : runLoss++;
    r.ou_result ? ov++ : un++;
  }
  const pct = (n: number) => total ? n / total : 0;

  return new Response(JSON.stringify({
    total_games: total,
    win_pct: pct(win),
    loss_pct: pct(loss),
    runline_win_pct: pct(runWin),
    runline_loss_pct: pct(runLoss),
    over_pct: pct(ov),
    under_pct: pct(un)
  }), { headers: { "Content-Type": "application/json" } });
});
