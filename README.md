# PropEdge V8

NBA player prop betting analytics dashboard.

## Quick Start

```bash
# 1. Clone repo
git clone https://github.com/iamwerewolf1007/PropEdgeV8.0
cd PropEdgeV8.0

# 2. Install deps
pip3 install pandas openpyxl requests nba_api

# 3. First-time setup: fetch missing game logs (Mar 22, 23) and grade them
python3 scripts/fetch_grade_setup.py

# 4. Score all props and build today.json
python3 scripts/run_everything.py

# 5. Activate automation (runs on schedule daily)
bash setup_automation.sh
```

## Manual Commands

| Command | What it does |
|---|---|
| `python3 scripts/run_everything.py` | Score all props, save today.json, git push |
| `python3 scripts/prematch_today.py` | Fetch today's props from Odds API, score, merge |
| `python3 scripts/prematch_today.py 2026-03-24` | Fetch props for specific date |
| `python3 scripts/grade_today.py` | Grade yesterday's props |
| `python3 scripts/grade_today.py 2026-03-22` | Grade specific date |
| `python3 scripts/fetch_grade_setup.py` | One-time: fetch Mar 22+23 from NBA API |

## Automation Schedule (UK time)

| Time | Script |
|---|---|
| 06:00 | grade_today.py |
| 08:00 | prematch_today.py |
| 18:00 | prematch_today.py |
| 22:00 | prematch_today.py |

## Model

10 sigmoid-weighted signals:

| Signal | Description | Weight |
|---|---|---|
| 1 | L5 avg vs line | 3.00 |
| 2 | L10 avg vs line | 2.50 |
| 3 | L20 avg vs line | 2.00 |
| 4 | L30 avg vs line | 1.50 |
| 5 | Home/Away split vs line | 1.50 |
| 6 | B2B/Rest split vs line | 1.50 |
| 7 | DVP tier (Top10/Mid/Bot10) vs line | 1.00 |
| 8 | H2H vs opponent (neutral if <3 games) | 0.50 |
| 9 | FG% shooting trend L10 vs L30 | 1.00 |
| 10 | Minutes trend L10 vs L30 | 0.75 |

**Tier thresholds** (calibrated to V4 distribution):
- T1 ≥ 0.7594 → ~7.5% of plays, ~62% HR
- T2 ≥ 0.6273 → ~32% of plays, ~57% HR
- T3 < 0.6273 → remaining plays

## Checkpoint

Mar 23 prematch target (V4 reference): **T1: 11, T2: 59**

Run after `fetch_grade_setup.py` completes:
```bash
python3 scripts/prematch_today.py 2026-03-23
# Check: today.json Mar 23 plays should show T1:~11 T2:~59
```

## Files

```
PropEdgeV8.0/
├── index.html                      # Dashboard PWA
├── today.json                      # All plays (auto-generated)
├── setup_automation.sh             # Activate launchd agents
├── README.md
├── source-files/
│   ├── NBA_2025_26_Season_Player_Stats.xlsx
│   └── PropEdge_-_Match_and_Player_Prop_lines_.xlsx
├── scripts/
│   ├── model.py                    # Scoring engine
│   ├── xlsx_engine.py              # Database I/O
│   ├── run_everything.py           # Master scorer
│   ├── prematch_today.py           # Odds API fetch
│   ├── grade_today.py              # Results grader
│   └── fetch_grade_setup.py        # One-time NBA API backfill
└── launchd/
    ├── com.propedge.grade.plist
    ├── com.propedge.prematch1.plist
    ├── com.propedge.prematch2.plist
    └── com.propedge.prematch3.plist
```
