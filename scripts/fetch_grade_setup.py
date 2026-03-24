"""
PropEdge V8 — fetch_grade_setup.py
Run ONCE on Salman's Mac to:
  1. Fetch game logs for Mar 22 and Mar 23 from NBA API
  2. Append to game logs xlsx and recompute all sheets
  3. Grade Mar 21, Mar 22, Mar 23 props

Run from the V8 repo root:
    python3 scripts/fetch_grade_setup.py

Requires: pip install nba_api openpyxl pandas

NOTES:
- Uses ScoreboardV3 (ScoreboardV2 is broken for 2025-26 season)
- NBA API returns PT00M00.00S for all players in historical completed games.
  Fix: keep any player who scored points (they clearly played), only skip
  genuine DNPs (minutes=0 AND points=0 AND no other stats).
- Game IDs are deduplicated from scoreboard to prevent repeat fetches.
"""

import os, sys, json, time
from datetime import date, timedelta
import pandas as pd
import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

NBA_XLSX   = os.path.join(SCRIPT_DIR, '..', 'source-files', 'NBA_2025_26_Season_Player_Stats.xlsx')
PROP_XLSX  = os.path.join(SCRIPT_DIR, '..', 'source-files', 'PropEdge_-_Match_and_Player_Prop_lines_.xlsx')
TODAY_JSON = os.path.join(SCRIPT_DIR, '..', 'today.json')

from xlsx_engine import load_all_sheets, append_game_logs

# ── NBA API ───────────────────────────────────────────────────────────────────
try:
    from nba_api.stats.endpoints import ScoreboardV3, BoxScoreTraditionalV3
    from nba_api.stats.static import teams as nba_teams_static
except ImportError:
    print("ERROR: nba_api not installed. Run: pip install nba_api")
    sys.exit(1)

TEAM_ID_TO_ABBR = {t['id']: t['abbreviation'] for t in nba_teams_static.get_teams()}

FETCH_DATES = ['2026-03-22', '2026-03-23']
GRADE_DATES = ['2026-03-21', '2026-03-22', '2026-03-23']


def _parse_minutes(m) -> float:
    """
    Parse NBA API minutes string to float.
    'PT32M15.00S' → 32.25
    'PT00M00.00S' → 0.0  (NOT None — caller decides whether to skip)
    '32.5'        → 32.5
    Returns 0.0 for any unparseable/zero value.
    """
    if m is None:
        return 0.0
    s = str(m).strip()
    if s in ('', 'None', 'nan'):
        return 0.0
    if s.startswith('PT'):
        try:
            inner = s[2:]
            m_part, rest = inner.split('M')
            s_part = rest.replace('S', '')
            return max(0.0, int(m_part) + float(s_part) / 60)
        except Exception:
            return 0.0
    try:
        return max(0.0, float(s))
    except (TypeError, ValueError):
        return 0.0


def _get_games_for_date(game_date: str) -> list:
    """
    Return deduplicated list of game_ids for game_date using ScoreboardV3.
    """
    print(f"  Fetching game IDs for {game_date}...")
    try:
        sb = ScoreboardV3(game_date=game_date)
        time.sleep(1.0)
        data = sb.get_dict()
        # ScoreboardV3 structure: scoreboard → games list
        games = data.get('scoreboard', {}).get('games', [])
        # Deduplicate game IDs
        seen = set()
        game_ids = []
        for g in games:
            gid = g.get('gameId', '')
            if gid and gid not in seen:
                seen.add(gid)
                game_ids.append(gid)
        print(f"  Found {len(game_ids)} unique games")
        return game_ids
    except Exception as e:
        print(f"  ERROR fetching scoreboard for {game_date}: {e}")
        return []


def _fetch_box_score(game_id: str, game_date: str,
                     existing_gl: pd.DataFrame) -> list:
    """
    Fetch box score for one game. Returns player stat row dicts.

    KEY FIX: NBA API returns PT00M00.00S for all players in historical
    completed games. We therefore CANNOT rely on minutes > 0 to determine
    if a player played. Instead:
      - Keep player if points > 0 OR rebounds > 0 OR assists > 0 (clearly played)
      - Keep player if parsed minutes > 0
      - Skip only if ALL stats are zero (genuine DNP)
    """
    try:
        bs = BoxScoreTraditionalV3(game_id=game_id)
        time.sleep(0.8)
        pstats = bs.player_stats.get_data_frame()
    except Exception as e:
        print(f"    ERROR box score {game_id}: {e}")
        return []

    if pstats.empty:
        return []

    # Determine home/away team: index 0 = away, index 1 = home in NBA convention
    team_ids = pstats['teamId'].unique().tolist()
    home_tid = None
    away_tid = None
    try:
        ts = bs.team_stats.get_data_frame()
        if len(ts) >= 2:
            away_tid = ts.iloc[0]['teamId']
            home_tid = ts.iloc[1]['teamId']
    except Exception:
        pass

    rows = []
    gd = pd.to_datetime(game_date).date()

    for _, p in pstats.iterrows():
        pts  = int(p.get('points', 0) or 0)
        reb  = int(p.get('reboundsTotal', 0) or 0)
        ast  = int(p.get('assists', 0) or 0)
        mins = _parse_minutes(p.get('minutes'))
        fgm  = int(p.get('fieldGoalsMade', 0) or 0)
        fga  = int(p.get('fieldGoalsAttempted', 0) or 0)
        fg3m = int(p.get('threePointersMade', 0) or 0)
        fg3a = int(p.get('threePointersAttempted', 0) or 0)
        ftm  = int(p.get('freeThrowsMade', 0) or 0)
        fta  = int(p.get('freeThrowsAttempted', 0) or 0)
        stl  = int(p.get('steals', 0) or 0)
        blk  = int(p.get('blocks', 0) or 0)
        tov  = int(p.get('turnovers', 0) or 0)
        pm   = float(p.get('plusMinusPoints', 0) or 0)

        # Skip genuine DNPs — zero everything
        if pts == 0 and reb == 0 and ast == 0 and fga == 0 and fta == 0 and mins == 0.0:
            continue

        team_id   = p.get('teamId')
        team_abbr = TEAM_ID_TO_ABBR.get(team_id, str(team_id))

        # Opponent
        all_tids  = pstats['teamId'].unique().tolist()
        opp_id    = next((t for t in all_tids if t != team_id), None)
        opp_abbr  = TEAM_ID_TO_ABBR.get(opp_id, '') if opp_id else ''

        # Home / Away
        if home_tid and away_tid:
            is_home   = (team_id == home_tid)
        else:
            is_home   = True  # safe fallback
        ha = 'Home' if is_home else 'Away'
        matchup = f"{team_abbr} vs {opp_abbr}" if is_home else f"{team_abbr} @ {opp_abbr}"

        # Rest days & B2B
        player_name = p.get('playerNameI') or p.get('playerName') or ''
        p_hist = existing_gl[existing_gl['Player'] == player_name].sort_values('Date', ascending=False)
        prior  = p_hist[p_hist['Date'].dt.date < gd]
        if len(prior) > 0:
            last_date = prior.iloc[0]['Date'].date()
            rest_days = (gd - last_date).days
            is_b2b    = rest_days == 1
        else:
            rest_days = 7
            is_b2b    = False

        # Opp def rank from latest known value
        opp_def_rank = 15
        if len(existing_gl) > 0:
            opp_rows = existing_gl[existing_gl['Opponent'] == opp_abbr].sort_values('Date', ascending=False)
            if len(opp_rows) > 0 and pd.notna(opp_rows.iloc[0].get('Opp Def Rank')):
                opp_def_rank = int(opp_rows.iloc[0]['Opp Def Rank'])

        # Use parsed minutes; if still 0 but player clearly played, estimate from context
        # (won't affect model — minutes only used for filtering, not signals)
        effective_mins = mins if mins > 0 else (20.0 if pts > 0 or fga > 0 else 0.0)

        rows.append({
            'Player':       player_name,
            'Team':         team_abbr,
            'Player ID':    p.get('personId', ''),
            'Season':       '2025-26',
            'Date':         game_date,
            'Matchup':      matchup,
            'Opponent':     opp_abbr,
            'Home/Away':    ha,
            'Minutes':      round(effective_mins, 2),
            'Points':       pts,
            'FGM':          fgm,
            'FGA':          fga,
            'FG%':          round(fgm / fga, 4) if fga > 0 else 0.0,
            '3PM':          fg3m,
            '3PA':          fg3a,
            '3P%':          round(fg3m / fg3a, 4) if fg3a > 0 else 0.0,
            'FTM':          ftm,
            'FTA':          fta,
            'FT%':          round(ftm / fta, 4) if fta > 0 else 0.0,
            'REB':          reb,
            'AST':          ast,
            'STL':          stl,
            'BLK':          blk,
            'TOV':          tov,
            '+/-':          pm,
            'W/L':          '',   # filled in enrichment below
            'Rest Days':    rest_days,
            'B2B':          is_b2b,
            'Opp Def Rank': opp_def_rank,
            'Opp Pace Rank': 15,
            'Game_ID':      game_id,
        })
    return rows


def _fill_wl(rows: list) -> list:
    """Fill W/L by comparing team totals within each game."""
    from collections import defaultdict
    game_team_pts: dict = defaultdict(lambda: defaultdict(int))
    for r in rows:
        game_team_pts[r['Game_ID']][r['Team']] += r['Points']

    for r in rows:
        teams = game_team_pts[r['Game_ID']]
        if len(teams) == 2:
            winner = max(teams, key=teams.get)
            r['W/L'] = 'W' if r['Team'] == winner else 'L'
    return rows


def fetch_missing_game_logs():
    """Fetch game logs for FETCH_DATES and append to xlsx."""
    sheets = load_all_sheets()
    gl     = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])

    all_new_rows = []

    for game_date in FETCH_DATES:
        # Skip if already in database
        existing = gl[gl['Date'].dt.date.astype(str) == game_date]
        if len(existing) > 0:
            print(f"  {game_date}: {len(existing)} rows already in database — skipping")
            continue

        print(f"\nFetching {game_date}...")
        game_ids = _get_games_for_date(game_date)
        if not game_ids:
            print(f"  No games found for {game_date}")
            continue

        raw_rows = []
        for gid in game_ids:
            print(f"  Box score: {gid}")
            raw_rows.extend(_fetch_box_score(gid, game_date, gl))

        if raw_rows:
            raw_rows = _fill_wl(raw_rows)
            print(f"  Got {len(raw_rows)} player rows for {game_date}")
            # Verify we have real data
            pts_total = sum(r['Points'] for r in raw_rows)
            print(f"  Total points across all players: {pts_total} (sanity check)")
            all_new_rows.extend(raw_rows)
        else:
            print(f"  No valid rows for {game_date}")

    if all_new_rows:
        new_df = pd.DataFrame(all_new_rows)
        # Ensure column order matches existing game log
        for col in gl.columns:
            if col not in new_df.columns:
                new_df[col] = np.nan
        new_df = new_df[gl.columns]
        append_game_logs(new_df)
        print(f"\n✓ Appended {len(new_df)} new game log rows, all sheets recomputed")
    else:
        print("\nNo new game log data fetched.")


def grade_date(grade_date_str: str):
    """Grade all ungraded props for grade_date_str."""
    sheets = load_all_sheets()
    gl     = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])

    gl_date = gl[gl['Date'].dt.date.astype(str) == grade_date_str]
    if len(gl_date) == 0:
        print(f"  No game logs for {grade_date_str} — cannot grade.")
        return

    # Build points lookup: player → actual points
    pts_map = {}
    for _, row in gl_date.iterrows():
        player = row['Player']
        pts    = float(row['Points'])
        mins   = float(row['Minutes'])
        # A player is graded if they have points OR played (mins > 0)
        if mins > 0 or pts > 0:
            pts_map[player] = pts
        elif player not in pts_map:
            pts_map[player] = None  # DNP

    if not os.path.exists(TODAY_JSON):
        print(f"  today.json not found")
        return

    with open(TODAY_JSON) as f:
        plays = json.load(f)

    graded = wins = losses = 0
    FINAL_RESULTS = ('WIN', 'LOSS', 'DNP', 'PUSH', 'NO_PLAY')

    for play in plays:
        if play.get('date') != grade_date_str:
            continue
        if play.get('result') in FINAL_RESULTS:
            continue  # Already graded — never touch

        player    = play.get('player', '')
        line      = float(play.get('line', 0))
        direction = play.get('direction', '')

        if player not in pts_map:
            play['result']     = 'DNP'
            play['actual_pts'] = None
            graded += 1
            continue

        actual = pts_map[player]
        play['actual_pts'] = actual

        if actual is None:
            play['result'] = 'DNP'
            graded += 1
            continue

        if actual > line:
            result = 'WIN'  if direction == 'OVER'  else 'LOSS'
        elif actual < line:
            result = 'WIN'  if direction == 'UNDER' else 'LOSS'
        else:
            result = 'PUSH'

        play['result'] = result
        graded += 1
        if result == 'WIN':   wins   += 1
        elif result == 'LOSS': losses += 1

    print(f"  Graded {graded} plays for {grade_date_str}: {wins}W / {losses}L")

    with open(TODAY_JSON, 'w') as f:
        json.dump(plays, f, separators=(',', ':'), default=str)


if __name__ == '__main__':
    print("=" * 60)
    print("PropEdge V8 — fetch_grade_setup.py")
    print("=" * 60)

    print("\n[1/2] Fetching missing game logs (Mar 22, Mar 23)...")
    fetch_missing_game_logs()

    print("\n[2/2] Grading props for Mar 21, 22, 23...")
    for d in GRADE_DATES:
        print(f"\nGrading {d}...")
        grade_date(d)

    print("\n✓ Setup complete.")
    print("Next: python3 scripts/run_everything.py")
