"""
PropEdge V8 — run_everything.py
Scores all props, builds today.json, git pushes.

Design rules:
  - Graded plays (WIN/LOSS/DNP/PUSH): preserved ENTIRELY, never re-scored
  - Ungraded plays: scored fresh every run
  - Precomputed lookups for performance (~13k props in <30s)
"""

import os, sys, json, subprocess
from datetime import datetime, date, timedelta
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.join(SCRIPT_DIR, '..')
TODAY_JSON = os.path.join(REPO_ROOT, 'today.json')
sys.path.insert(0, SCRIPT_DIR)

from xlsx_engine import load_all_sheets, load_props
from model import score_prop


def _build_lookups(sheets, gl):
    """Precompute all per-player lookup dicts for fast scoring."""
    avg = sheets['avg']; ha = sheets['ha']; b2b = sheets['b2b']
    oq  = sheets['oq'];  sh = sheets['shoot']; mn = sheets['mins']
    h2h = sheets['h2h']

    team_lkp   = dict(zip(avg['Player'], avg['Team']))
    avg_dict   = {r['Player']: r.to_dict() for _,r in avg.iterrows()}
    ha_dict    = {r['Player']: r.to_dict() for _,r in ha.iterrows()}
    b2b_dict   = {r['Player']: r.to_dict() for _,r in b2b.iterrows()}
    oq_dict    = {r['Player']: r.to_dict() for _,r in oq.iterrows()}
    sh_dict    = {r['Player']: r.to_dict() for _,r in sh.iterrows()}
    mn_dict    = {r['Player']: r.to_dict() for _,r in mn.iterrows()}
    h2h_dict   = {(r['Player'],r['Opponent']): r.to_dict() for _,r in h2h.iterrows()}

    # B2B lookup: (player, date_str) → bool
    gl_played = gl[gl['Minutes']>0].sort_values(['Player','Date'])
    b2b_lkp   = {}
    for player, grp in gl_played.groupby('Player'):
        dates = grp['Date'].dt.date.tolist()
        for i, d in enumerate(dates):
            b2b_lkp[(player, str(d))] = (i > 0 and (d - dates[i-1]).days == 1)

    # Points lookup: (player, date_str) → float
    pts_lkp = {}
    for _, r in gl_played.iterrows():
        pts_lkp[(r['Player'], str(r['Date'].date()))] = float(r['Points'])

    # recent20: per player, last 20 game scores and home/away flags
    recent20_dict = {}
    gl_recent = gl_played.sort_values(['Player','Date'], ascending=[True,False])
    for player, grp in gl_recent.groupby('Player'):
        last20 = grp.head(20)
        recent20_dict[player] = {
            'scores': [int(x) for x in last20['Points'].tolist()],
            'homes':  [bool(x=='Home') for x in last20['Home/Away'].tolist()],
        }

    # recent20: per player last 20 scored games + home/away flags
    recent20_dict = {}
    gl_r = gl_played.sort_values(['Player','Date'], ascending=[True,False])
    for _pl, _grp in gl_r.groupby('Player'):
        _last = _grp.head(20)
        recent20_dict[_pl] = {
            'scores': [int(x) for x in _last['Points'].tolist()],
            'homes':  [bool(x=='Home') for x in _last['Home/Away'].tolist()],
        }

    return dict(team=team_lkp, avg=avg_dict, ha=ha_dict, b2b=b2b_dict,
                oq=oq_dict, sh=sh_dict, mn=mn_dict, h2h=h2h_dict,
                b2b_flag=b2b_lkp, pts=pts_lkp, recent20=recent20_dict)


def score_all(props: pd.DataFrame, lkp: dict, existing_graded: dict) -> list:
    all_plays = []
    skipped = scored = preserved = 0

    for _, row in props.iterrows():
        player    = str(row['Player'])
        game_date = str(row['Date'].date())
        line      = float(row['Line'])
        home, away = str(row['Home']), str(row['Away'])

        # Graded → preserve entirely
        key = (player, game_date, line)
        if key in existing_graded:
            all_plays.append(existing_graded[key])
            preserved += 1
            continue

        pt = lkp['team'].get(player, '')
        if not pt: skipped += 1; continue

        is_home = pt.upper() == home.upper()
        opp     = away if is_home else home
        is_b2b  = lkp['b2b_flag'].get((player, game_date), False)

        avg_r = lkp['avg'].get(player, {})
        if not avg_r: skipped += 1; continue

        result = score_prop(
            line=line, player_name=player, opponent=opp,
            is_home=is_home, is_b2b=is_b2b,
            avg_row=avg_r,
            ha_row=lkp['ha'].get(player, {}),
            b2b_row=lkp['b2b'].get(player, {}),
            oq_row=lkp['oq'].get(player, {}),
            h2h_row=lkp['h2h'].get((player, opp), {}),
            shoot_row=lkp['sh'].get(player, {}),
            mins_row=lkp['mn'].get(player, {}),
        )

        actual = lkp['pts'].get((player, game_date))
        grade  = None
        if actual is not None:
            d = result['direction']
            if actual > line:   grade = 'WIN' if d=='OVER'  else 'LOSS'
            elif actual < line: grade = 'WIN' if d=='UNDER' else 'LOSS'
            else:               grade = 'PUSH'

        all_plays.append({
            'player': player, 'team': pt,
            'position':  str(row.get('Position','')),
            'date':      game_date,
            'game_time': str(row.get('Game_Time_ET','')),
            'game':      str(row.get('Game','')),
            'home': home, 'away': away, 'opponent': opp,
            'is_home': is_home, 'is_b2b': is_b2b,
            'line': line,
            'min_line':   float(row.get('Min Line', line)),
            'max_line':   float(row.get('Max Line', line)),
            'over_odds':  int(row.get('Over Odds', -110)),
            'under_odds': int(row.get('Under Odds', -110)),
            'books':      int(row.get('Books', 1)),
            'event_id':   str(row.get('Event ID', '')),
            'direction':  result['direction'],
            'confidence': result['confidence'],
            'tier':       result['tier'],
            'prob_over':  result['prob_over'],
            'signals':    {str(k): v for k,v in result['signals'].items()},
            'result':     grade,
            'actual_pts': actual,
            'l10':  lkp['avg'].get(player, {}).get('L10 Avg PTS'),
            'l20':  lkp['avg'].get(player, {}).get('L20 Avg PTS'),
            'l30':  lkp['avg'].get(player, {}).get('L30 Avg PTS'),
            'recent20':       lkp['recent20'].get(player, {}).get('scores', []),
            'recent20_homes': lkp['recent20'].get(player, {}).get('homes', []),
            'line_history':   [line],
        })
        scored += 1

    print(f"  Scored:{scored}  Preserved:{preserved}  Skipped:{skipped}")
    return all_plays


def load_graded(path: str) -> dict:
    if not os.path.exists(path): return {}
    with open(path) as f:
        plays = json.load(f)
    return {(p['player'], p['date'], float(p['line'])): p
            for p in plays if p.get('result') in ('WIN','LOSS','DNP','PUSH','NO_PLAY')}


def season_stats(plays: list) -> dict:
    graded = [p for p in plays if p.get('result') in ('WIN','LOSS')]
    wins   = sum(1 for p in graded if p['result']=='WIN')
    t1     = [p for p in graded if p.get('tier')=='T1']
    t2     = [p for p in graded if p.get('tier')=='T2']
    t1w    = sum(1 for p in t1 if p['result']=='WIN')
    t2w    = sum(1 for p in t2 if p['result']=='WIN')
    def pct(w, n): return round(w/n*100,1) if n else 0
    return dict(graded=len(graded), hr=pct(wins,len(graded)),
                t1=len(t1), t1_hr=pct(t1w,len(t1)),
                t2=len(t2), t2_hr=pct(t2w,len(t2)))


def git_push(stats: dict):
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    msg = (f"V8 {now} | Graded:{stats['graded']} HR:{stats['hr']}% | "
           f"T1:{stats['t1']}({stats['t1_hr']}%) T2:{stats['t2']}({stats['t2_hr']}%)")
    cwd = os.path.abspath(REPO_ROOT)
    try:
        subprocess.run(['git','add','-A'],          cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','commit','-m',msg],   cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','push'],              cwd=cwd, check=True, capture_output=True)
        print(f"  Git push OK: {msg}")
    except subprocess.CalledProcessError as e:
        print(f"  Git push failed: {e.stderr.decode()[:200] if e.stderr else e}")


if __name__ == '__main__':
    print("="*60)
    print("PropEdge V8 — run_everything.py")
    print("="*60)

    print("\n[1/5] Loading database...")
    sheets = load_all_sheets()
    props, _ = load_props()
    gl = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])
    print(f"  GL:{len(gl)} Props:{len(props)} Players:{sheets['avg']['Player'].nunique()}")

    print("\n[2/5] Building lookup tables...")
    lkp = _build_lookups(sheets, gl)

    print("\n[3/5] Loading existing graded plays...")
    existing = load_graded(TODAY_JSON)
    print(f"  {len(existing)} graded plays preserved")

    print("\n[4/5] Scoring all props...")
    all_plays = score_all(props, lkp, existing)

    print("\n[5/5] Saving and pushing...")
    with open(TODAY_JSON,'w') as f:
        json.dump(all_plays, f, separators=(',',':'), default=str)
    print(f"  Saved today.json ({len(all_plays)} plays)")

    stats = season_stats(all_plays)
    print(f"  Season: {stats['graded']} graded | {stats['hr']}% HR")
    print(f"  T1:{stats['t1']}({stats['t1_hr']}%)  T2:{stats['t2']}({stats['t2_hr']}%)")
    git_push(stats)
    print("\nDone.")
