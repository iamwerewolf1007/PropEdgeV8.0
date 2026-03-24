"""
PropEdge V8 — prematch_today.py
Fetches today's NBA player props from The Odds API, scores them, merges into today.json.

Usage: python3 scripts/prematch_today.py [YYYY-MM-DD]
"""
import os, sys, json, time, requests
from datetime import datetime, date
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.join(SCRIPT_DIR, '..')
TODAY_JSON = os.path.join(REPO_ROOT, 'today.json')
sys.path.insert(0, SCRIPT_DIR)

from xlsx_engine import load_all_sheets
from model import score_prop
from run_everything import _build_lookups

ODDS_KEY  = 'c0bab20a574208a41a6e0d930cdaf313'
BASE_URL  = 'https://api.the-odds-api.com/v4'
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


def _alert_low_credits(headers):
    rem = headers.get('x-requests-remaining')
    if rem and int(rem) <= CREDIT_ALERT:
        msg = f"Odds API credits LOW: {rem} remaining"
        print(f"⚠️  {msg}")
        try:
            import subprocess
            subprocess.run(['osascript','-e',
                f'display notification "{msg}" with title "PropEdge V8"'],
                capture_output=True)
        except Exception: pass


def fetch_events(target_date):
    r = requests.get(f"{BASE_URL}/sports/{SPORT}/events",
                     params={'apiKey':ODDS_KEY,'dateFormat':'iso'}, timeout=15)
    _alert_low_credits(r.headers)
    r.raise_for_status()
    return [e for e in r.json() if e.get('commence_time','')[:10] == target_date]


def fetch_props(event, target_date):
    eid  = event['id']
    home = NAME_TO_ABBR.get(event.get('home_team',''), event.get('home_team',''))
    away = NAME_TO_ABBR.get(event.get('away_team',''), event.get('away_team',''))
    r = requests.get(f"{BASE_URL}/sports/{SPORT}/events/{eid}/odds",
                     params={'apiKey':ODDS_KEY,'regions':'us','markets':'player_points',
                             'oddsFormat':'american','dateFormat':'iso'}, timeout=15)
    _alert_low_credits(r.headers)
    if r.status_code != 200: return []

    player_data = {}
    for book in r.json().get('bookmakers',[]):
        for mkt in book.get('markets',[]):
            if mkt.get('key') != 'player_points': continue
            for o in mkt.get('outcomes',[]):
                p     = o.get('description', o.get('name',''))
                line  = float(o.get('point', 0))
                odds  = int(o.get('price',-110))
                otype = o.get('name','')
                if p not in player_data: player_data[p] = {}
                if line not in player_data[p]:
                    player_data[p][line] = {'over':[],'under':[],'books':set()}
                player_data[p][line]['books'].add(book.get('title',''))
                if 'Over' in otype: player_data[p][line]['over'].append(odds)
                else:               player_data[p][line]['under'].append(odds)

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
            game_time=gt, event_id=eid,
            line=best,
            min_line=min(lines.keys()), max_line=max(lines.keys()),
            over_odds =int(sum(d['over'])/len(d['over']))   if d['over']  else -110,
            under_odds=int(sum(d['under'])/len(d['under'])) if d['under'] else -110,
            books=n,
        ))
    return results


def score_and_merge(raw_props, target_date, lkp, existing_plays):
    idx_map = {}
    for i, p in enumerate(existing_plays):
        if p.get('result') not in ('WIN','LOSS','DNP','PUSH','NO_PLAY'):
            idx_map[(p['player'], p.get('date',''), float(p.get('line',0)))] = i

    new_plays = []
    for raw in raw_props:
        player = raw['player']
        line   = raw['line']
        home, away = raw['home'], raw['away']

        pt = lkp['team'].get(player,'')
        if not pt: continue

        is_home = pt.upper() == home.upper()
        opp     = away if is_home else home
        is_b2b  = lkp['b2b_flag'].get((player, target_date), False)
        avg_r   = lkp['avg'].get(player, {})
        if not avg_r: continue

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

        avg_r2 = lkp['avg'].get(player, {})
        r20    = lkp['recent20'].get(player, {})
        play = dict(
            player=player, team=pt, position='',
            date=target_date, game_time=raw['game_time'], game=raw['game'],
            home=home, away=away, opponent=opp,
            is_home=is_home, is_b2b=is_b2b,
            line=line, min_line=raw['min_line'], max_line=raw['max_line'],
            over_odds=raw['over_odds'], under_odds=raw['under_odds'],
            books=raw['books'], event_id=raw['event_id'],
            direction=result['direction'], confidence=result['confidence'],
            tier=result['tier'], prob_over=result['prob_over'],
            signals={str(k):v for k,v in result['signals'].items()},
            result=None, actual_pts=None,
            l10=avg_r2.get('L10 Avg PTS'),
            l20=avg_r2.get('L20 Avg PTS'),
            l30=avg_r2.get('L30 Avg PTS'),
            recent20=r20.get('scores', []),
            recent20_homes=r20.get('homes', []),
            line_history=[line],
        )

        k = (player, target_date, float(line))
        if k in idx_map:
            # Preserve line_history — append new line if it changed
            prev = existing_plays[idx_map[k]]
            hist = prev.get('line_history', [prev.get('line', line)])
            if line not in hist:
                hist = hist + [line]
            play['line_history'] = hist
            existing_plays[idx_map[k]] = play
        else:
            new_plays.append(play)

    return existing_plays + new_plays


if __name__ == '__main__':
    import subprocess
    target_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    print("="*60)
    print(f"PropEdge V8 — prematch_today.py  [{target_date}]")
    print("="*60)

    print("\n[1/4] Loading database...")
    sheets = load_all_sheets()
    gl = sheets['gl']
    gl['Date'] = pd.to_datetime(gl['Date'])
    lkp = _build_lookups(sheets, gl)

    print("\n[2/4] Fetching events...")
    events = fetch_events(target_date)
    print(f"  {len(events)} games on {target_date}")
    if not events: sys.exit(0)

    print("\n[3/4] Fetching player props...")
    all_raw = []
    for ev in events:
        home = NAME_TO_ABBR.get(ev.get('home_team',''), ev.get('home_team',''))
        away = NAME_TO_ABBR.get(ev.get('away_team',''), ev.get('away_team',''))
        print(f"  {away} @ {home}")
        all_raw.extend(fetch_props(ev, target_date))
        time.sleep(0.5)
    print(f"  Total raw props: {len(all_raw)}")

    existing = json.load(open(TODAY_JSON)) if os.path.exists(TODAY_JSON) else []
    print("\n[4/4] Scoring and saving...")
    merged = score_and_merge(all_raw, target_date, lkp, existing)
    todays = [p for p in merged if p['date']==target_date]
    t1=[p for p in todays if p['tier']=='T1']
    t2=[p for p in todays if p['tier']=='T2']
    t3=[p for p in todays if p['tier']=='T3']
    print(f"\n  {target_date}: {len(todays)} plays | T1:{len(t1)} T2:{len(t2)} T3:{len(t3)}")

    with open(TODAY_JSON,'w') as f:
        json.dump(merged, f, separators=(',',':'), default=str)
    print(f"  Saved today.json ({len(merged)} total)")

    try:
        cwd = os.path.abspath(REPO_ROOT)
        msg = f"Prematch {target_date} | T1:{len(t1)} T2:{len(t2)} T3:{len(t3)}"
        subprocess.run(['git','add','-A'],         cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','commit','-m',msg],  cwd=cwd, check=True, capture_output=True)
        subprocess.run(['git','push'],             cwd=cwd, check=True, capture_output=True)
        print(f"  Git push OK")
    except Exception as e:
        print(f"  Git push failed: {e}")
    print("\nDone.")
