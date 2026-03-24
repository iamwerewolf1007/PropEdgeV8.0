"""
PropEdge V8 — fetch_grade_setup.py
Run ONCE on Salman's Mac to:
  1. Fetch game logs for Mar 22 and Mar 23 from NBA API
  2. Append to game logs xlsx and recompute all sheets
  3. Grade Mar 21 props that were previously ungraded (due to bad rows)
  4. Grade Mar 22 props
  5. Grade Mar 23 props

Run from the V8 repo root:
    python3 scripts/fetch_grade_setup.py

Requires:
    pip install nba_api openpyxl pandas
"""

import os, sys, json, time
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
NBA_XLSX  = os.path.join(SCRIPT_DIR, '..', 'source-files', 'NBA_2025_26_Season_Player_Stats.xlsx')
PROP_XLSX = os.path.join(SCRIPT_DIR, '..', 'source-files', 'PropEdge_-_Match_and_Player_Prop_lines_.xlsx')
TODAY_JSON = os.path.join(SCRIPT_DIR, '..', 'today.json')

from xlsx_engine import (load_all_sheets, load_props, append_game_logs,
                          recompute_all_sheets, save_nba_sheets)

# ── NBA API IMPORTS ───────────────────────────────────────────────────────────
try:
    from nba_api.stats.endpoints import (
        ScoreboardV2, BoxScoreTraditionalV3, LeagueGameLog
    )
    from nba_api.stats.static import teams as nba_teams_static
except ImportError:
    print("ERROR: nba_api not installed. Run: pip install nba_api")
    sys.exit(1)

TEAM_ID_TO_ABBR = {t['id']: t['abbreviation'] for t in nba_teams_static.get_teams()}
ABBR_TO_TEAM_ID = {v: k for k, v in TEAM_ID_TO_ABBR.items()}

# ── DATES TO FETCH ────────────────────────────────────────────────────────────
FETCH_DATES = ['2026-03-22', '2026-03-23']
GRADE_DATES = ['2026-03-21', '2026-03-22', '2026-03-23']


def _parse_minutes(m):
    """
    Parse NBA API minutes string to float.
    'PT32M15.00S' → 32.25
    'PT00M00.00S' → None (player did not play)
    '32.5' or 32.5 → 32.5
    """
    if m is None:
        return None
    s = str(m).strip()
    if s in ('', 'None', 'nan'):
        return None
    if s.startswith('PT'):
        try:
            s = s[2:]
            m_part, rest = s.split('M')
            s_part = rest.replace('S', '')
            minutes = int(m_part) + float(s_part) / 60
            return minutes if minutes > 0 else None
        except Exception:
            return None
    try:
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _get_games_for_date(game_date: str) -> list:
    """
    Returns list of game_ids played on game_date (YYYY-MM-DD).
    """
    print(f"  Fetching game IDs for {game_date}...")
    try:
        sb = ScoreboardV2(game_date=game_date)
        time.sleep(1.0)
        games_df = sb.game_header.get_data_frame()
        game_ids = games_df['GAME_ID'].tolist()
        print(f"  Found {len(game_ids)} games")
        return game_ids
    except Exception as e:
        print(f"  ERROR fetching scoreboard for {game_date}: {e}")
        return []


def _fetch_box_score(game_id: str, game_date: str) -> list:
    """
    Fetch box score for a game. Returns list of player stat dicts.
    """
    try:
        bs = BoxScoreTraditionalV3(game_id=game_id)
        time.sleep(0.8)
        pstats = bs.player_stats.get_data_frame()
    except Exception as e:
        print(f"    ERROR fetching box score {game_id}: {e}")
        return []

    rows = []
    for _, p in pstats.iterrows():
        mins = _parse_minutes(p.get('minutes'))
        if mins is None:
            continue  # Did not play — skip entirely

        team_id  = p.get('teamId')
        team_abbr = TEAM_ID_TO_ABBR.get(team_id, str(team_id))
        opp_id   = None
        # Determine opponent: look at game_id teams
        # opp is the other team in the game
        all_teams = pstats['teamId'].unique().tolist()
        for tid in all_teams:
            if tid != team_id:
                opp_id = tid
                break
        opp_abbr = TEAM_ID_TO_ABBR.get(opp_id, '') if opp_id else ''

        # Home/Away: determined by game_id convention (home team last 4 digits >= away)
        # Use scoreboard data if available; fallback to 'Home' for now
        # Will determine properly below

        fgm  = int(p.get('fieldGoalsMade', 0) or 0)
        fga  = int(p.get('fieldGoalsAttempted', 0) or 0)
        fg3m = int(p.get('threePointersMade', 0) or 0)
        fg3a = int(p.get('threePointersAttempted', 0) or 0)
        ftm  = int(p.get('freeThrowsMade', 0) or 0)
        fta  = int(p.get('freeThrowsAttempted', 0) or 0)
        reb  = int(p.get('reboundsTotal', 0) or 0)
        ast  = int(p.get('assists', 0) or 0)
        stl  = int(p.get('steals', 0) or 0)
        blk  = int(p.get('blocks', 0) or 0)
        tov  = int(p.get('turnovers', 0) or 0)
        pts  = int(p.get('points', 0) or 0)
        pm   = float(p.get('plusMinusPoints', 0) or 0)

        row = {
            'Player':       p.get('playerNameI') or p.get('playerName') or '',
            'Team':         team_abbr,
            'Player ID':    p.get('personId', ''),
            'Season':       '2025-26',
            'Date':         game_date,
            'Matchup':      f"{team_abbr} vs {opp_abbr}",
            'Opponent':     opp_abbr,
            'Home/Away':    '',   # filled below
            'Minutes':      round(mins, 2),
            'Points':       pts,
            'FGM':          fgm,
            'FGA':          fga,
            'FG%':          round(fgm/fga, 4) if fga > 0 else 0.0,
            '3PM':          fg3m,
            '3PA':          fg3a,
            '3P%':          round(fg3m/fg3a, 4) if fg3a > 0 else 0.0,
            'FTM':          ftm,
            'FTA':          fta,
            'FT%':          round(ftm/fta, 4) if fta > 0 else 0.0,
            'REB':          reb,
            'AST':          ast,
            'STL':          stl,
            'BLK':          blk,
            'TOV':          tov,
            '+/-':          pm,
            'W/L':          '',   # filled below
            'Rest Days':    0,    # computed below
            'B2B':          False,
            'Opp Def Rank': 15,   # default mid — updated below if available
            'Opp Pace Rank':15,
            'Game_ID':      game_id,
        }
        rows.append(row)
    return rows


def _enrich_rows(rows: list, game_date: str, gl_existing: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in Home/Away, W/L, Rest Days, B2B, Opp Def/Pace Rank.
    """
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df

    df['Date'] = pd.to_datetime(game_date)

    # ── Home/Away: check game_id home team convention ──────────────────────
    # NBA game_id format: '0022501234'
    # Home team is the one whose abbreviation appears as last team in game
    # Best approach: from scoreboard home_team_id. We'll infer by team pair:
    # The home team in a game is identified from the matchup string
    # For simplicity: use existing game logs to find home/away or set unknown
    # We'll set Home/Away based on Matchup pattern "TEAM vs OPP" vs "TEAM @ OPP"
    # The box score doesn't give us this directly; we use scoreboard
    # For now: set based on game_id odd/even — not reliable.
    # Better: check if team is in home position from scoreboard game_header
    for game_id in df['Game_ID'].unique():
        try:
            bs = BoxScoreTraditionalV3(game_id=game_id)
            time.sleep(0.5)
            team_stats = bs.team_stats.get_data_frame()
            if len(team_stats) >= 2:
                # home team has teamTricode at index 1 in NBA convention? Not reliable.
                # Use game_id: the last 4 chars are game number.
                # Actually: in BoxScoreTraditionalV3, team order: [away, home]
                away_tid = team_stats.iloc[0]['teamId']
                home_tid = team_stats.iloc[1]['teamId']
                away_abbr = TEAM_ID_TO_ABBR.get(away_tid, '')
                home_abbr = TEAM_ID_TO_ABBR.get(home_tid, '')
                mask = df['Game_ID'] == game_id
                df.loc[mask & (df['Team'] == home_abbr), 'Home/Away'] = 'Home'
                df.loc[mask & (df['Team'] == away_abbr), 'Home/Away'] = 'Away'
                df.loc[mask & (df['Team'] == home_abbr), 'Matchup'] = \
                    df.loc[mask & (df['Team'] == home_abbr), 'Team'].apply(
                        lambda t: f"{t} vs {away_abbr}")
                df.loc[mask & (df['Team'] == away_abbr), 'Matchup'] = \
                    df.loc[mask & (df['Team'] == away_abbr), 'Team'].apply(
                        lambda t: f"{t} @ {home_abbr}")
        except Exception:
            pass

    # Fill blank Home/Away with 'Home' as fallback
    df.loc[df['Home/Away'] == '', 'Home/Away'] = 'Home'

    # ── W/L: compare team points in box score ────────────────────────────────
    for game_id in df['Game_ID'].unique():
        mask = df['Game_ID'] == game_id
        game_rows = df[mask]
        team_pts = game_rows.groupby('Team')['Points'].sum()
        if len(team_pts) == 2:
            teams = team_pts.index.tolist()
            winner = team_pts.idxmax()
            loser  = team_pts.idxmin()
            df.loc[mask & (df['Team'] == winner), 'W/L'] = 'W'
            df.loc[mask & (df['Team'] == loser),  'W/L'] = 'L'

    # ── Rest Days + B2B ───────────────────────────────────────────────────────
    gd = pd.to_datetime(game_date).date()
    for player in df['Player'].unique():
        p_hist = gl_existing[gl_existing['Player'] == player].sort_values('Date', ascending=False)
        if len(p_hist) > 0:
            last_game = pd.to_datetime(p_hist.iloc[0]['Date']).date()
            rest = (gd - last_game).days
            is_b2b = rest == 1
        else:
            rest   = 7
            is_b2b = False
        df.loc[df['Player'] == player, 'Rest Days'] = rest
        df.loc[df['Player'] == player, 'B2B']       = is_b2b

    # ── Opp Def Rank: use most recent from game logs ──────────────────────────
    latest_def_rank = {}
    gl_sorted = gl_existing.sort_values('Date', ascending=False)
    for _, row in gl_sorted.iterrows():
        opp = row['Opponent']
        if opp not in latest_def_rank and pd.notna(row.get('Opp Def Rank')):
            latest_def_rank[opp] = int(row['Opp Def Rank'])

    for idx, row in df.iterrows():
        opp = row['Opponent']
        df.at[idx, 'Opp Def Rank'] = latest_def_rank.get(opp, 15)

    return df


def fetch_missing_game_logs():
    """Fetch game logs for FETCH_DATES and append to xlsx."""
    sheets = load_all_sheets()
    gl     = sheets['gl']

    all_new_rows = []
    for game_date in FETCH_DATES:
        # Check if already exists
        check = gl[gl['Date'].dt.date.astype(str) == game_date]
        if len(check) > 0:
            print(f"  {game_date}: already in database ({len(check)} rows) — skipping fetch")
            continue

        print(f"\nFetching {game_date}...")
        game_ids = _get_games_for_date(game_date)
        if not game_ids:
            print(f"  No games found for {game_date}")
            continue

        raw_rows = []
        for gid in game_ids:
            print(f"  Box score: {gid}")
            raw_rows.extend(_fetch_box_score(gid, game_date))

        if raw_rows:
            enriched = _enrich_rows(raw_rows, game_date, gl)
            print(f"  Got {len(enriched)} player rows for {game_date}")
            all_new_rows.append(enriched)
        else:
            print(f"  No valid rows for {game_date}")

    if all_new_rows:
        combined = pd.concat(all_new_rows, ignore_index=True)
        # Ensure column order matches game log
        for col in gl.columns:
            if col not in combined.columns:
                combined[col] = np.nan
        combined = combined[gl.columns]
        append_game_logs(combined)
        print("\nGame logs updated and all sheets recomputed.")
    else:
        print("\nNo new game log data fetched.")


def grade_date(grade_date_str: str, today_json_path: str):
    """
    Grade all props for grade_date_str (YYYY-MM-DD).
    Reads today.json, matches graded game logs, applies WIN/LOSS/DNP.
    Preserves original play tier/conf/dir for graded rows.
    """
    from xlsx_engine import load_all_sheets, load_props

    print(f"\nGrading {grade_date_str}...")
    sheets = load_all_sheets()
    gl     = sheets['gl']
    props, _ = load_props()

    gl['Date']    = pd.to_datetime(gl['Date'])
    props['Date'] = pd.to_datetime(props['Date'])

    # Game logs for this date
    gl_date = gl[gl['Date'].dt.date.astype(str) == grade_date_str]
    if len(gl_date) == 0:
        print(f"  No game logs for {grade_date_str} — cannot grade. Run fetch first.")
        return

    # Load today.json
    if not os.path.exists(today_json_path):
        print(f"  today.json not found at {today_json_path}")
        return

    with open(today_json_path, 'r') as f:
        plays = json.load(f)

    graded = 0
    for play in plays:
        if play.get('date') != grade_date_str:
            continue
        if play.get('result') in ('WIN', 'LOSS', 'DNP', 'PUSH', 'NO_PLAY'):
            continue  # Already graded — preserve as-is

        player = play.get('player')
        line   = play.get('line', 0)
        direction = play.get('direction', '')

        # Find player's actual points on this date
        p_rows = gl_date[gl_date['Player'] == player]
        if len(p_rows) == 0:
            # Player not found in game logs
            play['result'] = 'DNP'
            play['actual_pts'] = None
            graded += 1
            continue

        actual_pts = float(p_rows.iloc[0]['Points'])
        play['actual_pts'] = actual_pts

        if actual_pts > line:
            result = 'WIN' if direction == 'OVER' else 'LOSS'
        elif actual_pts < line:
            result = 'WIN' if direction == 'UNDER' else 'LOSS'
        else:
            result = 'PUSH'

        play['result'] = result
        graded += 1

    wins   = sum(1 for p in plays if p.get('date')==grade_date_str and p.get('result')=='WIN')
    losses = sum(1 for p in plays if p.get('date')==grade_date_str and p.get('result')=='LOSS')
    print(f"  Graded {graded} plays for {grade_date_str}: {wins}W / {losses}L")

    with open(today_json_path, 'w') as f:
        json.dump(plays, f, indent=2, default=str)
    print(f"  Saved today.json")


if __name__ == '__main__':
    print("=" * 60)
    print("PropEdge V8 — Setup: Fetch missing game logs + grade")
    print("=" * 60)

    # Step 1: Fetch Mar 22 + Mar 23 game logs
    print("\n[1/2] Fetching missing game logs (Mar 22, Mar 23)...")
    fetch_missing_game_logs()

    # Step 2: Grade Mar 21, 22, 23
    print("\n[2/2] Grading props for Mar 21, 22, 23...")
    for d in GRADE_DATES:
        grade_date(d, TODAY_JSON)

    print("\nSetup complete.")
    print("Next step: run run_everything.py to score all props and generate today.json")
