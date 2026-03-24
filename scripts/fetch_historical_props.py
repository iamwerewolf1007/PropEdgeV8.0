"""
PropEdge V8 — fetch_historical_props.py
Fetches historical player props from The Odds API for completed game dates.
Uses the /historical/ endpoint (costs 10 credits per event vs 1 for live).

Usage:
    python3 scripts/fetch_historical_props.py [YYYY-MM-DD]
    (defaults to yesterday)

Cost estimate: ~10 events × 10 credits = ~100 credits per date.
"""
import os, sys, json, time, requests
from datetime import datetime, date, timedelta
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.join(SCRIPT_DIR, '..')
TODAY_JSON = os.path.join(REPO_ROOT, 'today.json')
sys.path.insert(0, SCRIPT_DIR)

from xlsx_engine import load_all_sheets
from model import score_prop
from run_everything import _build_lookups

ODDS_KEY  = 'c0bab20a574208a41a6e0d930cdaf313'
BASE      = 'https://api.the-odds-api.com/v4'
SPORT     = 'basketball_nba'
CREDIT_ALERT = 170

NAME_TO_ABBR = {
    'Atlanta Hawks':'ATL','Boston Celtics':'BOS','Brooklyn Nets':'BKN',
    'Charlotte Hornets':'CHA','Chicago Bulls':'CHI','Cleveland Cavaliers':'CLE',
    'Dallas Mavericks':'DAL','Denver Nuggets':'DEN','Detroit Pistons':'DET',
    'Golden State Warriors':'GSW','Houston Rockets':'HOU','Indiana Pacers':'IND',
    'LA Clippers':'LAC','Los Angeles Clippers':'LAC','Los Angeles Lakers':'LAL',
    'Memphis Grizzlies':'MEM','Miami Heat':'MIA','Milwaukee Bucks':'MIL',
    'Minnesota Timberwolves':'MIN','New Orleans Pelicans':'NOP',
    'New York Knicks':'NYK','Oklahoma City Thunder':'OKC','Orlando Magic':'ORL',
    'Philadelphia 76ers':'PHI','Phoenix Suns':'PHX','Portland Trail Blazers':'POR',
    'Sacramento Kings':'SAC','San Antonio Spurs':'SAS','Toronto Raptors':'TOR',
    'Utah Jazz':'UTA','Washington Wizards':'WAS',
}

def _alert(headers):
    rem = headers.get('x-requests-remaining')
    used = headers.get('x-requests-used')
    print(f"  API credits — used:{used} remaining:{rem}")
    if rem and int(rem) <= CREDIT_ALERT:
        try:
            import subprocess
            subprocess.run(['osascript','-e',
                f'display notification "Odds API credits LOW: {rem}" with title "PropEdge V8"'],
                capture_output=True)
        except Exception: pass


def fetch_historical_events(target_date: str) -> list:
    """
    Get all NBA events for target_date using historical endpoint.
    Uses a snapshot at noon ET (17:00 UTC) — pre-game lines.
    """
    # Snapshot time: noon ET on the game day = 17:00 UTC
    snapshot = f"{target_date}T17:00:00Z"
    url = f"{BASE}/historical/sports/{SPORT}/events"
    params = {
        'apiKey':   ODDS_KEY,
        'date':     snapshot,
        'dateFormat': 'iso',
    }
    print(f"  Fetching events snapshot at {snapshot}...")
    resp = requests.get(url, params=params, timeout=20)
    _alert(resp.headers)
    resp.raise_for_status()
    data = resp.json()
    events = data.get('data', data) if isinstance(data, dict) else data
    # Filter to just this date
    day_events = [e for e in events if e.get('commence_time','')[:10] == target_date]
    print(f"  Found {len(day_events)} events on {target_date}")
    return day_events, snapshot


def fetch_historical_props(event_id: str, event: dict, snapshot: str) -> list:
    """Fetch historical player props for one event at snapshot time."""
    home = NAME_TO_ABBR.get(event.get('home_team',''), event.get('home_team',''))
    away = NAME_TO_ABBR.get(event.get('away_team',''), event.get('away_team',''))
    url  = f"{BASE}/historical/sports/{SPORT}/events/{event_id}/odds"
    params = {
        'apiKey':     ODDS_KEY,
        'date':       snapshot,
        'regions':    'us',
        'markets':    'player_points',
        'oddsFormat': 'american',
        'dateFormat': 'iso',
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        _alert(resp.headers)
        resp.raise_for_status()
        data = resp.json()
        event_data = data.get('data', data)
    except Exception as e:
        print(f"    ERROR {event_id}: {e}")
        return []

    player_data = {}
    bookmakers = event_data.get('bookmakers', []) if isinstance(event_data, dict) else []
    for book in bookmakers:
        for mkt in book.get('markets', []):
            if mkt.get('key') != 'player_points': continue
            for o in mkt.get('outcomes', []):
                player = o.get('description', o.get('name',''))
                line   = float(o.get('point', 0))
                odds   = int(o.get('price', -110))
                otype  = o.get('name', '')
                if player not in player_data: player_data[player] = {}
                if line not in player_data[player]:
                    player_data[player][line] = {'over':[],'under':[],'books':set()}
                player_data[player][line]['books'].add(book.get('title',''))
                if 'Over' in otype: player_data[player][line]['over'].append(odds)
                else:               player_data[player][line]['under'].append(odds)

    results = []
    gt = event.get('commence_time','')[:16].replace('T',' ')
    for player, lines in player_data.items():
        best = max(lines, key=lambda l: len(lines[l]['books']))
        d = lines[best]
        n = len(d['books'])
        if n == 0: continue
        results.append(dict(
            player=player, home=home, away=away,
            game=f"{event.get('away_team',away)} @ {event.get('home_team',home)}",
            game_time=gt, event_id=event_id,
            line=best,
            min_line=min(lines.keys()), max_line=max(lines.keys()),
            over_odds =int(sum(d['over'])/len(d['over']))   if d['over']  else -110,
            under_odds=int(sum(d['under'])/len(d['under'])) if d['under'] else -110,
            books=n,
        ))
    return results


def score_and_merge(raw_props, target_date, lkp, existing_plays):
    """Score fetched props and merge into existing_plays without duplicating."""
    # Index ungraded existing plays for dedup
    idx_map = {}
    for i, p in enumerate(existing_plays):
        if p.get('result') not in ('WIN','LOSS','DNP','PUSH','NO_PLAY'):
            idx_map[(p['player'], p.get('date',''), float(p.get('line',0)))] = i

    new_plays = []
    scored = updated = skipped = 0

    for raw in raw_props:
        player = raw['player']
        line   = raw['line']
        home, away = raw['home'], raw['away']

        pt = lkp['team'].get(player, '')
        if not pt: skipped += 1; continue

        is_home = pt.upper() == home.upper()
        opp     = away if is_home else home
        is_b2b  = lkp['b2b_flag'].get((player, target_date), False)
        avg_r   = lkp['avg'].get(player, {})
        if not avg_r: skipped += 1; continue

        result = score_prop(
            line=line, player_name=player, opponent=opp,
            is_home=is_home, is_b2b=is_b2b,
            avg_row=avg_r,
            ha_row=lkp['ha'].get(player,{}),
            b2b_row=lkp['b2b'].get(player,{}),
            oq_row=lkp['oq'].get(player,{}),
            h2h_row=lkp['h2h'].get((player,opp),{}),
            shoot_row=lkp['sh'].get(player,{}),
            mins_row=lkp['mn'].get(player,{}),
        )

        r20 = lkp['recent20'].get(player, {})
        play = dict(
            player=player, team=pt, position='',
            date=target_date, game_time=raw['game_time'],
            game=raw['game'], home=home, away=away, opponent=opp,
            is_home=is_home, is_b2b=is_b2b,
            line=line, min_line=raw['min_line'], max_line=raw['max_line'],
            over_odds=raw['over_odds'], under_odds=raw['under_odds'],
            books=raw['books'], event_id=raw['event_id'],
            direction=result['direction'], confidence=result['confidence'],
            tier=result['tier'], prob_over=result['prob_over'],
            signals={str(k):v for k,v in result['signals'].items()},
            result=None, actual_pts=None,
            l10=avg_r.get('L10 Avg PTS'), l20=avg_r.get('L20 Avg PTS'),
            l30=avg_r.get('L30 Avg PTS'),
            recent20=r20.get('scores',[]), recent20_homes=r20.get('homes',[]),
            line_history=[line],
        )

        k = (player, target_date, float(line))
        if k in idx_map:
            prev = existing_plays[idx_map[k]]
            hist = prev.get('line_history', [prev.get('line', line)])
            if line not in hist: hist = hist + [line]
            play['line_history'] = hist
            existing_plays[idx_map[k]] = play
            updated += 1
        else:
            new_plays.append(play)
            scored += 1

    print(f"  Scored:{scored}  Updated:{updated}  Skipped:{skipped}")
    return existing_plays + new_plays


if __name__ == '__main__':
    import subprocess

    target_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today() - timedelta(days=1))
    print("="*60)
    print(f"PropEdge V8 — fetch_historical_props.py  [{target_date}]")
    print("="*60)

    print("\n[1/4] Loading database...")
    sheets = load_all_sheets()
    gl = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])
    lkp = _build_lookups(sheets, gl)

    print("\n[2/4] Fetching historical events from Odds API...")
    events, snapshot = fetch_historical_events(target_date)
    if not events:
        print("  No events found — nothing to do.")
        sys.exit(0)

    print("\n[3/4] Fetching historical player props per game...")
    all_raw = []
    for ev in events:
        home = NAME_TO_ABBR.get(ev.get('home_team',''), ev.get('home_team',''))
        away = NAME_TO_ABBR.get(ev.get('away_team',''), ev.get('away_team',''))
        print(f"  {away} @ {home}")
        props = fetch_historical_props(ev['id'], ev, snapshot)
        print(f"    → {len(props)} player props")
        all_raw.extend(props)
        time.sleep(1.0)   # historical endpoint is rate-limited more strictly

    print(f"\n  Total raw props: {len(all_raw)}")
    if not all_raw:
        print("  No props retrieved.")
        sys.exit(0)

    print("\n[4/4] Scoring and saving...")
    existing = json.load(open(TODAY_JSON)) if os.path.exists(TODAY_JSON) else []
    merged   = score_and_merge(all_raw, target_date, lkp, existing)

    day_plays = [p for p in merged if p['date'] == target_date]
    t1 = [p for p in day_plays if p['tier']=='T1']
    t2 = [p for p in day_plays if p['tier']=='T2']
    t3 = [p for p in day_plays if p['tier']=='T3']
    print(f"\n  {target_date}: {len(day_plays)} plays | T1:{len(t1)} T2:{len(t2)} T3:{len(t3)}")

    with open(TODAY_JSON,'w') as f:
        json.dump(merged, f, separators=(',',':'), default=str)
    print(f"  Saved today.json ({len(merged)} total plays)")

    try:
        cwd = os.path.abspath(REPO_ROOT)
        msg = f"Historical props {target_date} | T1:{len(t1)} T2:{len(t2)} T3:{len(t3)}"
        subprocess.run(['git','add','-A'],         cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','commit','-m',msg],  cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','push'],             cwd=cwd, check=True, capture_output=True)
        print(f"  Git push OK")
    except Exception as e:
        print(f"  Git push failed: {e}")

    print("\nDone.")
