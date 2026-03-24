"""
PropEdge V8 — grade_today.py
Grades all ungraded props for a completed date.

Usage: python3 scripts/grade_today.py [YYYY-MM-DD]
       (defaults to yesterday)

Rules:
  - WIN:  actual > line & OVER, or actual < line & UNDER
  - LOSS: actual < line & OVER, or actual > line & UNDER
  - PUSH: actual == line
  - DNP:  player not in game logs for that date
  - Never re-grades plays that already have a result
"""
import os, sys, json, subprocess
from datetime import datetime, date, timedelta
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.join(SCRIPT_DIR, '..')
TODAY_JSON = os.path.join(REPO_ROOT, 'today.json')
sys.path.insert(0, SCRIPT_DIR)

from xlsx_engine import load_all_sheets

FINAL = ('WIN','LOSS','DNP','PUSH','NO_PLAY')


def grade(grade_date, plays, gl):
    gl_date = gl[gl['Date'].dt.date.astype(str) == grade_date]
    if len(gl_date) == 0:
        print(f"  No game logs for {grade_date}. Run fetch_grade_setup.py first.")
        return plays, 0, 0, 0

    pts_map = {}
    for _, r in gl_date.iterrows():
        p, m = r['Player'], float(r['Minutes'])
        if m > 0:
            pts_map[p] = float(r['Points'])
        elif p not in pts_map:
            pts_map[p] = None  # DNP

    graded = wins = losses = 0
    for play in plays:
        if play.get('date') != grade_date: continue
        if play.get('result') in FINAL:    continue

        p   = play.get('player','')
        ln  = float(play.get('line', 0))
        dir = play.get('direction','')

        if p not in pts_map:
            play['result'] = 'DNP'; play['actual_pts'] = None
            graded += 1; continue

        actual = pts_map[p]
        play['actual_pts'] = actual
        if actual is None:
            play['result'] = 'DNP'; graded += 1; continue

        if actual > ln:   res = 'WIN' if dir=='OVER'  else 'LOSS'
        elif actual < ln: res = 'WIN' if dir=='UNDER' else 'LOSS'
        else:             res = 'PUSH'
        play['result'] = res
        graded += 1
        if res == 'WIN':  wins  += 1
        elif res == 'LOSS': losses += 1

    return plays, graded, wins, losses


if __name__ == '__main__':
    grade_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today() - timedelta(days=1))
    print("="*60)
    print(f"PropEdge V8 — grade_today.py  [{grade_date}]")
    print("="*60)

    print("\n[1/3] Loading game logs...")
    sheets = load_all_sheets()
    gl = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])
    gl_check = gl[gl['Date'].dt.date.astype(str) == grade_date]
    print(f"  Game log rows for {grade_date}: {len(gl_check)}")
    if len(gl_check) == 0:
        print("  Run fetch_grade_setup.py first."); sys.exit(1)

    print("\n[2/3] Grading plays...")
    if not os.path.exists(TODAY_JSON):
        print("  today.json not found."); sys.exit(0)
    with open(TODAY_JSON) as f:
        plays = json.load(f)

    plays, graded, wins, losses = grade(grade_date, plays, gl)
    t1g = [p for p in plays if p.get('date')==grade_date and p.get('tier')=='T1' and p.get('result') in ('WIN','LOSS')]
    t2g = [p for p in plays if p.get('date')==grade_date and p.get('tier')=='T2' and p.get('result') in ('WIN','LOSS')]
    t1w = sum(1 for p in t1g if p['result']=='WIN')
    t2w = sum(1 for p in t2g if p['result']=='WIN')
    hr_str = f"{round(wins/(wins+losses)*100,1)}%" if wins+losses else "N/A"
    print(f"\n  {grade_date}: {graded} graded | {wins}W {losses}L ({hr_str})")
    print(f"  T1: {len(t1g)} plays | {t1w}W {len(t1g)-t1w}L")
    print(f"  T2: {len(t2g)} plays | {t2w}W {len(t2g)-t2w}L")

    print("\n[3/3] Saving and pushing...")
    def _safe(obj):
        if isinstance(obj, float) and (obj != obj or obj == float('inf') or obj == float('-inf')):
            return None
        return str(obj)
    with open(TODAY_JSON,'w') as f:
        json.dump(plays, f, separators=(',',':'), default=_safe)
    print(f"  Saved today.json")

    try:
        cwd = os.path.abspath(REPO_ROOT)
        msg = f"Grade {grade_date} | {wins}W {losses}L {hr_str} | T1:{len(t1g)} T2:{len(t2g)}"
        subprocess.run(['git','add','-A'],         cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','commit','-m',msg],  cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','push'],             cwd=cwd, check=True, capture_output=True)
        print(f"  Git push OK: {msg}")
    except Exception as e:
        print(f"  Git push failed: {e}")
    print("\nDone.")
