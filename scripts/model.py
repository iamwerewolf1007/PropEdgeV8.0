"""
PropEdge V8 — Core Scoring Model
Sigmoid-based 10-signal scoring.

Calibration (validated Mar 2026):
  T1 >= 0.7594  -> 7.4% of plays, 62.2% HR
  T2 >= 0.6273  -> 32.1% of plays, 57.3% HR
"""
import math

WEIGHTS = {1:3.00, 2:2.50, 3:2.00, 4:1.50, 5:1.50,
           6:1.50, 7:1.00, 8:0.50, 9:1.00, 10:0.75}
TOTAL_WEIGHT = sum(WEIGHTS.values())   # 15.25

T1_CONF = 0.7594
T2_CONF = 0.6273

PTS_SCALE   = 4.0
TREND_SCALE = 2.0
MIN_SCALE   = 1.5

TEAM_DEF_RANK = {
    'BOS':1,'OKC':2,'DET':3,'HOU':4,'NYK':5,'PHX':6,'SAS':7,'TOR':8,'CHA':9,
    'ORL':11,'GSW':12,'MIN':13,'CLE':13,'LAL':15,'BKN':16,'PHI':17,'MIL':18,
    'DEN':19,'ATL':20,'POR':21,'MIA':21,'DAL':23,'MEM':24,'NOP':25,'CHI':26,
    'IND':26,'SAC':28,'WAS':29,'UTA':30,'LAC':15,
}

def _def_tier(opp):
    rank = TEAM_DEF_RANK.get(str(opp).upper(), 15)
    return 'TOP10' if rank<=10 else ('BOT10' if rank>=21 else 'MID')

def _diff(avg, line):
    if avg is None: return None
    try:
        v = float(avg)
    except (TypeError, ValueError):
        return None
    if math.isnan(v): return None
    return v - line

def _sig(diff, scale):
    if diff is None: return 0.5
    return 1.0 / (1.0 + math.exp(-diff / scale))

def score_prop(line, player_name, opponent, is_home, is_b2b,
               avg_row, ha_row, b2b_row, oq_row, h2h_row,
               shoot_row, mins_row):
    sigs = {}

    # S1-S4: window averages
    for s, w in [(1,5),(2,10),(3,20),(4,30)]:
        sigs[s] = _sig(_diff(avg_row.get(f'L{w} Avg PTS'), line), PTS_SCALE)

    # S5: home/away
    ctx = (ha_row.get('Home L10 Avg PTS') or ha_row.get('Home All Avg PTS')) if is_home \
        else (ha_row.get('Away L10 Avg PTS') or ha_row.get('Away All Avg PTS'))
    sigs[5] = _sig(_diff(ctx, line), PTS_SCALE)

    # S6: b2b/rest
    if is_b2b:
        g = b2b_row.get('B2B Games', 0) or 0
        ctx6 = b2b_row.get('B2B Avg PTS') if g >= 2 else None
    else:
        g = b2b_row.get('Rest Games', 0) or 0
        ctx6 = b2b_row.get('Rest Avg PTS') if g >= 2 else None
    sigs[6] = _sig(_diff(ctx6, line), PTS_SCALE)

    # S7: opp quality DVP
    tier = _def_tier(opponent)
    if tier == 'TOP10':   aq, gk = oq_row.get('vs Top-10 Def Avg PTS'), 'vs Top-10 Def Games'
    elif tier == 'BOT10': aq, gk = oq_row.get('vs Bot-10 Def Avg PTS'), 'vs Bot-10 Def Games'
    else:                 aq, gk = oq_row.get('vs Mid Def Avg PTS'),    'vs Mid Def Games'
    tg = oq_row.get(gk, 0) or 0
    sigs[7] = _sig(_diff(aq if tg >= 3 else None, line), PTS_SCALE)

    # S8: H2H (neutral if <3 games)
    h2h_g = (h2h_row.get('H2H Games', 0) or 0) if h2h_row else 0
    sigs[8] = _sig(_diff(h2h_row.get('H2H Avg PTS'), line), PTS_SCALE) if h2h_g >= 3 else 0.5

    # S9: shooting trend
    sigs[9]  = _sig(shoot_row.get('FG% Trend (L10-L30)'), TREND_SCALE)

    # S10: minutes trend
    sigs[10] = _sig(mins_row.get('MIN Trend (L10-L30)'), MIN_SCALE)

    prob_over  = sum(sigs[i] * WEIGHTS[i] for i in range(1, 11)) / TOTAL_WEIGHT
    direction  = 'OVER' if prob_over >= 0.5 else 'UNDER'
    confidence = round(prob_over if prob_over >= 0.5 else 1.0 - prob_over, 4)
    tier_label = 'T1' if confidence >= T1_CONF else ('T2' if confidence >= T2_CONF else 'T3')

    return {'direction': direction, 'confidence': confidence, 'tier': tier_label,
            'signals': sigs, 'prob_over': round(prob_over, 4), 'line': line}
