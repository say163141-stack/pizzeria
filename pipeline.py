#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
СТАВОЧНАЯ ПИЦЦЕРИЯ — единый честный конвейер (durable).
====================================================================
Один файл заменяет разрозненные inline-скрипты. Его может запустить
любая свежая сессия (в т.ч. по расписанию) и получить актуальный пул.

ЧТО ДЕЛАЕТ (по шагам):
  1) fetch_odds     — реальные кэфы the-odds-api (футбол h2h+totals, теннис, MMA)
  2) model_football — лёгкая голевая модель из openfootball (EWMA атака/защита,
                      Пуассон-сетка → 1X2 / тотал / фора / обе-забьют)
  3) injuries       — травмы RotoWire (EPL) как КОНТЕКСТ (не валидированный вход)
  4) build_kupon    — усадка-к-рынку + фактор ротации → честные value/ставки
  5) build_pages    — вставляет данные в шаблоны (Штаб + Купон)

ГЛАВНЫЙ ЧЕСТНЫЙ ПРИНЦИП (почему edge маленький):
  бэктест 1X2 показал, что модель рынок НЕ бьёт (лучший вес модели w≈0).
  Поэтому в направленных рынках (1X2, фора) мы показываем в основном РЫНОК
  (усадка W_DIR мал), и «раздутые» edge голевой заглушки схлопываются к ~0.
  Это правда, а не поражение: продукт работает как монитор + трекер CLV,
  реальные рекомендации на деньги — на паузе, пока модель не заслужит доверие.

  Пока источники xG (football-data.co.uk / understat нужного сезона) в ауте,
  футбольная модель — ГОЛЕВАЯ ЗАГЛУШКА (без xG). Она честно помечена modeled,
  но её abсолютные вероятности не калиброваны → сильная усадка к рынку.

ЗАПУСК:
  export ODDS_API_KEY=...   # ключ the-odds-api
  python3 pipeline.py       # всё сразу
  python3 pipeline.py fetch|model|kupon|pages   # отдельный шаг
ВЫХОД: football_markets.json, tennis_markets.json, odds_snapshot.json,
       kupon_data.json, pizzeria_hero.html, kupon.html
"""
import os, sys, io, csv, json, math, statistics, urllib.request, datetime, difflib
from collections import defaultdict

NOW = datetime.datetime.now(datetime.timezone.utc)
UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36'}
KEY = os.environ.get('ODDS_API_KEY', '').strip()
ODDS = "https://api.the-odds-api.com/v4"
# ОДИН букмекер — вся аналитика в рамках его линии. Winline в фиде нет → 1xBet (onexbet) как аналог.
BOOKMAKER = os.environ.get('ODDS_BOOKMAKER', 'onexbet').strip()
BOOKMAKER_NAME = {'onexbet': '1xBet', 'marathonbet': 'Marathonbet', 'pinnacle': 'Pinnacle',
                  'williamhill': 'William Hill'}.get(BOOKMAKER, BOOKMAKER)

# ---- football model params ----
SEASONS = ['2023-24', '2024-25', '2025-26', '2026-27']
# лиги, по которым есть рейтинги (openfootball) → полный анализ; остальные фикстуры = β (рынок)
DIVS = ['en.1', 'en.2', 'en.3', 'en.4', 'es.1', 'es.2', 'de.1', 'de.2',
        'it.1', 'it.2', 'fr.1', 'fr.2', 'nl.1', 'pt.1']
SCALE = {'en.1': 1.00, 'en.2': 0.80, 'en.3': 0.66, 'en.4': 0.54,
         'es.1': 1.00, 'es.2': 0.78, 'de.1': 0.98, 'de.2': 0.78,
         'it.1': 0.97, 'it.2': 0.76, 'fr.1': 0.95, 'fr.2': 0.74,
         'nl.1': 0.86, 'pt.1': 0.84}
# Футбол: лиги с рейтингами openfootball → тянем h2h+totals (полная модель, 2 кредита).
MODELED_COMPS = ['soccer_epl', 'soccer_spain_la_liga', 'soccer_italy_serie_a', 'soccer_germany_bundesliga']
# β-лиги (без своей модели) → тянем только h2h (1 кредит), показываем в слейте.
# Порядок = приоритет (при нехватке бюджета режется с конца). Пользователь просил Россию/Китай.
BETA_COMPS = ['soccer_uefa_champs_league', 'soccer_uefa_europa_league',
              'soccer_russia_premier_league', 'soccer_china_superleague',
              'soccer_france_ligue_one', 'soccer_netherlands_eredivisie', 'soccer_portugal_primeira_liga',
              'soccer_efl_champ', 'soccer_usa_mls', 'soccer_brazil_campeonato',
              'soccer_argentina_primera_division', 'soccer_turkey_super_league',
              'soccer_saudi_arabia_pro_league', 'soccer_mexico_ligamx',
              'soccer_japan_j_league', 'soccer_korea_kleague1', 'soccer_belgium_first_div',
              'soccer_spl', 'soccer_greece_super_league', 'soccer_netherlands_eredivisie']
# Бюджет кредитов the-odds-api на один прогон. Free = 500/мес. При ежедневном прогоне держим ~16.
# Апгрейд тарифа the-odds-api → подними это число, покрытие само расширится.
MAX_CREDITS = int(os.environ.get('ODDS_MAX_CREDITS', '16'))
DAYS_AHEAD = 5          # окно: ближайшие N дней (сегодня/завтра/послезавтра…)
MAX_FOOT = 80           # потолок числа футбольных фикстур в слейте
LG_NAMES = {'soccer_epl': 'АПЛ', 'soccer_spain_la_liga': 'Ла Лига', 'soccer_italy_serie_a': 'Серия A',
            'soccer_germany_bundesliga': 'Бундеслига', 'soccer_france_ligue_one': 'Лига 1',
            'soccer_netherlands_eredivisie': 'Эредивизи', 'soccer_portugal_primeira_liga': 'Примейра',
            'soccer_efl_champ': 'Чемпионшип', 'soccer_uefa_champs_league': 'ЛЧ',
            'soccer_uefa_europa_league': 'ЛЕ', 'soccer_russia_premier_league': 'РПЛ',
            'soccer_china_superleague': 'Китай', 'soccer_usa_mls': 'MLS',
            'soccer_brazil_campeonato': 'Бразилия', 'soccer_argentina_primera_division': 'Аргентина',
            'soccer_turkey_super_league': 'Турция', 'soccer_saudi_arabia_pro_league': 'Саудия',
            'soccer_mexico_ligamx': 'Мексика', 'soccer_japan_j_league': 'Япония',
            'soccer_korea_kleague1': 'Корея', 'soccer_belgium_first_div': 'Бельгия',
            'soccer_spl': 'Шотландия', 'soccer_greece_super_league': 'Греция'}
ALPHA, GOAL_CAL = 0.06, 1.07
LG_HOME, LG_AWAY = 1.50, 1.15           # средние голы дома/в гостях
# ---- усадка к рынку (честность) ----
W_DIR, W_TOT = 0.12, 0.35               # вес модели: направленные рынки / тоталы
MIDWEEK_MULT = 0.5                      # ротация: в будни усаживаем ещё сильнее
STAKE_CAP = 1.5                         # потолок ставки, % банка (четверть-Келли)
CAUTION_VAL = 0.18                      # порог «осторожно» по value
COINFLIP = 0.58                         # ниже этой увер-ти направленную ставку не стейкаем
W_MMA = 0.30                            # вес Elo-модели MMA против рынка (грубый Elo рынок не бьёт)

NAME2CODE = {'Newcastle United': 'NEW', 'Bournemouth': 'BOU', 'Brentford': 'BRE', 'Sunderland': 'SUN',
 'Brighton and Hove Albion': 'BHA', 'Leeds United': 'LEE', 'Manchester City': 'MCI', 'Fulham': 'FUL',
 'Crystal Palace': 'CRY', 'Nottingham Forest': 'NFO', 'Tottenham Hotspur': 'TOT', 'Manchester United': 'MUN',
 'Arsenal': 'ARS', 'Chelsea': 'CHE', 'Liverpool': 'LIV', 'Aston Villa': 'AVL', 'Everton': 'EVE',
 'West Ham United': 'WHU', 'Wolverhampton Wanderers': 'WOL', 'Burnley': 'BUR', 'Hull City': 'HUL',
 'Ipswich Town': 'IPS'}


def fetch(u, t=25):
    return urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=t).read().decode('utf-8', 'replace')


def pois(l, k):
    return math.exp(-l) * l ** k / math.factorial(k)


def devig(prices):
    inv = [1 / p for p in prices if p and p > 1]
    s = sum(inv)
    return [x / s for x in inv] if s else None


def fut(iso):
    try:
        return datetime.datetime.fromisoformat(iso.replace('Z', '+00:00')) > NOW
    except Exception:
        return False


def whenstr(iso):
    try:
        return datetime.datetime.fromisoformat(iso.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return iso


def shrink(our, ext, key, midweek):
    """Усадка вероятности модели к рынку (честность). Возвращает показываемую вер."""
    if ext is None:
        return our
    w = (W_TOT if key == 'ou' else W_DIR)
    if midweek:
        w *= MIDWEEK_MULT
    return w * our + (1 - w) * ext


def is_midweek(iso):
    try:
        return datetime.datetime.fromisoformat(iso.replace('Z', '+00:00')).weekday() in (1, 2, 3)  # Tue/Wed/Thu
    except Exception:
        return False


# ==================== 1. ODDS ====================
def active_sports(prefix):
    """Активные соревнования the-odds-api по префиксу ключа (tennis_, mma_) — /sports бесплатен.
    Позволяет пулу самому подхватывать новые турниры (US Open кончился → следующий ATP появился)."""
    try:
        d = json.loads(fetch(f"{ODDS}/sports/?apiKey={KEY}"))
        return [s['key'] for s in d if s.get('active') and s['key'].startswith(prefix)]
    except Exception as e:
        print(f"  ! active_sports {prefix}: {e}")
        return []


def get_odds(sportkey, markets):
    # ВСЕ коэффициенты — от ОДНОГО букмекера (BOOKMAKER). Winline в фиде the-odds-api нет,
    # ближайший доступный аналог (Россия, широкое покрытие) — 1xBet (onexbet).
    bm = f"&bookmakers={BOOKMAKER}" if BOOKMAKER else ""
    try:
        return json.loads(fetch(f"{ODDS}/sports/{sportkey}/odds/?apiKey={KEY}&regions=eu&markets={markets}{bm}&oddsFormat=decimal"))
    except Exception as e:
        print(f"  ! odds {sportkey}: {e}")
        return []


def med_h2h(m):
    pr = defaultdict(list)
    for bk in m.get('bookmakers', []):
        h = next((x for x in bk.get('markets', []) if x['key'] == 'h2h'), None)
        if h:
            for o in h['outcomes']:
                if o.get('price', 0) > 1:
                    pr[o['name']].append(o['price'])
    return {n: statistics.median(v) for n, v in pr.items()}


def med_over(m, line):
    ov = []
    for bk in m.get('bookmakers', []):
        t = next((x for x in bk.get('markets', []) if x['key'] == 'totals'), None)
        if t:
            for o in t['outcomes']:
                if o.get('name') == 'Over' and abs((o.get('point') or 0) - line) < .26 and o.get('price', 0) > 1:
                    ov.append(o['price'])
    return statistics.median(ov) if ov else None


def med_total(m, line, side):
    """Цена тотала (Over/Under) на конкретной линии от букмекера."""
    vals = []
    nm = 'Over' if side == 'over' else 'Under'
    for bk in m.get('bookmakers', []):
        t = next((x for x in bk.get('markets', []) if x['key'] == 'totals'), None)
        if t:
            for o in t['outcomes']:
                if o.get('name') == nm and abs((o.get('point') or 0) - line) < .26 and o.get('price', 0) > 1:
                    vals.append(o['price'])
    return statistics.median(vals) if vals else None


# ==================== 2. FOOTBALL MODEL ====================
def load_openfootball():
    rows = []
    for s in SEASONS:
        for d in DIVS:
            try:
                raw = fetch(f"https://raw.githubusercontent.com/openfootball/football.json/master/{s}/{d}.json")
                data = json.loads(raw)
            except Exception:
                continue
            ms = data.get('matches', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for m in ms:
                if not isinstance(m, dict):
                    continue
                t1, t2 = m.get('team1'), m.get('team2')
                sc = m.get('score')
                ft = None
                if isinstance(sc, dict):
                    ft = sc.get('ft')
                elif isinstance(sc, list):
                    ft = sc
                if ft is None and 'score1' in m and 'score2' in m:
                    ft = [m.get('score1'), m.get('score2')]
                if isinstance(ft, str) and '-' in ft:
                    ft = ft.split('-')
                if not (t1 and t2 and isinstance(ft, list) and len(ft) == 2):
                    continue
                try:
                    gh, ga = int(ft[0]), int(ft[1])
                except Exception:
                    continue
                day = None
                for f in ('%Y-%m-%d',):
                    try:
                        day = datetime.datetime.strptime(m.get('date', ''), f)
                    except Exception:
                        pass
                rows.append({'day': day or datetime.datetime(2000, 1, 1), 'd': d,
                             'h': clean(t1), 'a': clean(t2), 'gh': gh, 'ga': ga})
    rows.sort(key=lambda x: x['day'])
    return rows


def clean(n):
    for suf in (' FC', ' AFC', ' City', ):  # keep 'City' — только FC/AFC режем
        pass
    return n.replace(' FC', '').replace(' AFC', '').strip()


def build_football():
    rows = load_openfootball()
    att, dfn, div_of = defaultdict(lambda: None), defaultdict(lambda: None), {}
    for m in rows:
        for t, side in ((m['h'], 'd'), (m['a'], 'd')):
            if att[t] is None:
                sc = SCALE.get(m['d'], 0.7)
                att[t] = sc
                dfn[t] = 1.0 / sc if sc else 1.4
                div_of[t] = m['d']
        lh = LG_HOME * att[m['h']] * dfn[m['a']] * GOAL_CAL
        la = LG_AWAY * att[m['a']] * dfn[m['h']] * GOAL_CAL
        rh = m['gh'] / max(0.25, lh)
        ra = m['ga'] / max(0.25, la)
        att[m['h']] *= (1 - ALPHA) + ALPHA * rh
        dfn[m['a']] *= (1 - ALPHA) + ALPHA * rh
        att[m['a']] *= (1 - ALPHA) + ALPHA * ra
        dfn[m['h']] *= (1 - ALPHA) + ALPHA * ra

    def canon(n):
        n = clean(n)
        if n in att:
            return n
        mm = difflib.get_close_matches(n, list(att.keys()), n=1, cutoff=0.6)
        return mm[0] if mm else None

    def full_grid(lh, la, n=11):
        ph = [pois(lh, i) for i in range(n)]
        pa = [pois(la, i) for i in range(n)]
        g = {'P1': 0.0, 'X': 0.0, 'P2': 0.0, 'btts': 0.0, 'scores': {}}
        for L in (1.5, 2.5, 3.5, 4.5):
            g[f'ov{L}'] = 0.0
        for i in range(n):
            for j in range(n):
                p = ph[i] * pa[j]
                if i > j: g['P1'] += p
                elif i == j: g['X'] += p
                else: g['P2'] += p
                if i > 0 and j > 0: g['btts'] += p
                for L in (1.5, 2.5, 3.5, 4.5):
                    if i + j > L: g[f'ov{L}'] += p
                if i <= 5 and j <= 5:
                    g['scores'][f'{i}:{j}'] = g['scores'].get(f'{i}:{j}', 0) + p
        return g

    horizon = NOW + datetime.timedelta(days=DAYS_AHEAD)

    def in_window(iso):
        try:
            t = datetime.datetime.fromisoformat(iso.replace('Z', '+00:00'))
            return NOW < t <= horizon
        except Exception:
            return False

    # план в рамках бюджета кредитов: сначала модельные (h2h+totals=2), потом β (h2h=1)
    plan, credits, betas = [], 0, [b for b in dict.fromkeys(BETA_COMPS)]  # dedup β, keep order
    for comp in MODELED_COMPS:
        if credits + 2 > MAX_CREDITS:
            break
        plan.append((comp, 'h2h,totals')); credits += 2
    for comp in betas:
        if credits + 1 > MAX_CREDITS:
            break
        plan.append((comp, 'h2h')); credits += 1
    print(f"  футбол: план {len(plan)} лиг, ~{credits} кредитов (лимит {MAX_CREDITS})")
    events, seen = [], set()
    for comp, markets in plan:
        raw = get_odds(comp, markets)
        league = LG_NAMES.get(comp, comp.replace('soccer_', ''))
        for m in raw:
            iso = m.get('commence_time', '')
            if not in_window(iso):
                continue
            home, away = m.get('home_team'), m.get('away_team')
            med = med_h2h(m)
            if home not in med or away not in med:
                continue
            key = f"{home}|{away}|{iso[:10]}"
            if key in seen:
                continue
            seen.add(key)
            fav_home = med[home] <= med[away]
            fav = home if fav_home else away
            dog = away if fav_home else home
            odds_fav = round(med[fav], 2)
            if odds_fav <= 1.02 or odds_fav >= 26:
                continue
            ext_fav = 1 / med[fav]
            ch, ca = canon(home), canon(away)
            base = {'a': fav, 'b': dog, 'home': home, 'away': away, 'league': league,
                    'when': whenstr(iso), 'iso': iso, 'midweek': is_midweek(iso)}
            if ch and ca:  # есть рейтинги → ПОЛНАЯ модель по всем рынкам
                lh = LG_HOME * att[ch] * dfn[ca] * GOAL_CAL
                la = LG_AWAY * att[ca] * dfn[ch] * GOAL_CAL
                g = full_grid(lh, la)
                sP = g['P1'] + g['X'] + g['P2']
                p1, px, p2 = g['P1'] / sP, g['X'] / sP, g['P2'] / sP
                btts = g['btts']

                def row(key, grp, name, our, odds):
                    r = {'key': key, 'grp': grp, 'name': name, 'our': round(our, 4)}
                    if odds and odds > 1:
                        r['odds'] = round(odds, 2); r['ext'] = round(1 / odds, 4)
                    else:
                        r['odds'] = None; r['ext'] = None
                    return r
                mk = []
                # Исход (1X2) — есть линия
                mk.append(row('1x2', 'Исход', f'Победа {home}', p1, med.get(home)))
                mk.append(row('x', 'Исход', 'Ничья', px, med.get('Draw')))
                mk.append(row('1x2', 'Исход', f'Победа {away}', p2, med.get(away)))
                # Двойной шанс (наша оценка, линии обычно нет)
                mk.append(row('dc', 'Двойной шанс', f'{home} не проиграет (1X)', p1 + px, None))
                mk.append(row('dc', 'Двойной шанс', f'{away} не проиграет (X2)', px + p2, None))
                mk.append(row('dc', 'Двойной шанс', 'без ничьей (12)', p1 + p2, None))
                # Тоталы — линия есть
                for L in (1.5, 2.5, 3.5):
                    ov = g[f'ov{L}']
                    mk.append(row('ou', 'Тотал', f'Больше {L}', ov, med_total(m, L, 'over')))
                    mk.append(row('ou', 'Тотал', f'Меньше {L}', 1 - ov, med_total(m, L, 'under')))
                # Обе забьют
                mk.append(row('btts', 'Обе забьют', 'Да', btts, None))
                mk.append(row('btts', 'Обе забьют', 'Нет', 1 - btts, None))
                # Точный счёт — топ-3
                top = sorted(g['scores'].items(), key=lambda x: -x[1])[:3]
                for scname, pv in top:
                    mk.append(row('cs', 'Точный счёт', scname.replace(':', '–'), pv, None))
                base['markets'] = mk
                base['modeled'] = True
                base['topscore'] = top[0][0].replace(':', '–') if top else None
            else:            # нет рейтингов → β (только рынок), но по всем 3 исходам
                dv3 = devig([med.get(home), med.get('Draw', 0) or 99, med.get(away)])
                mk = [{'key': '1x2', 'grp': 'Исход', 'name': f'Победа {home}', 'our': round((dv3[0] if dv3 else ext_fav), 4),
                       'ext': round(1 / med[home], 4) if med.get(home) else None, 'odds': round(med[home], 2) if med.get(home) else None, 'modeled': False},
                      {'key': 'x', 'grp': 'Исход', 'name': 'Ничья', 'our': round(dv3[1], 4) if dv3 else None,
                       'ext': round(1 / med['Draw'], 4) if med.get('Draw') else None, 'odds': round(med['Draw'], 2) if med.get('Draw') else None, 'modeled': False},
                      {'key': '1x2', 'grp': 'Исход', 'name': f'Победа {away}', 'our': round((dv3[2] if dv3 else 1 - ext_fav), 4),
                       'ext': round(1 / med[away], 4) if med.get(away) else None, 'odds': round(med[away], 2) if med.get(away) else None, 'modeled': False}]
                base['markets'] = mk
                base['modeled'] = False
            events.append(base)
    events.sort(key=lambda e: e['iso'])
    events = events[:MAX_FOOT]
    out = {'events': events}
    json.dump(out, open('football_markets.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    nmod = sum(1 for e in events if e.get('modeled'))
    print(f"  футбол: {len(events)} матчей в окне {DAYS_AHEAD}д ({nmod} с моделью, {len(events)-nmod} β) из {len(plan)} лиг")
    return out


# ==================== 3. INJURIES ====================
def burden(outs):
    aw = dw = 0.0
    for x in outs:
        p = x['pos']
        if 'F' in p: aw += 1.0
        if 'M' in p: aw += 0.5; dw += 0.4
        if 'D' in p: dw += 0.7
        if 'G' in p: dw += 0.6
    return aw, dw


def add_injuries():
    try:
        inj = json.loads(fetch("https://www.rotowire.com/soccer/tables/injury-report.php?league=EPL"))
    except Exception as e:
        print(f"  ! травмы: {e}")
        inj = []
    byteam = defaultdict(list)
    for r in inj:
        if 'OUT' in (r.get('status') or '').upper():
            byteam[r.get('team', '')].append({'player': r.get('player'), 'pos': (r.get('position') or '')[:3], 'injury': r.get('injury')})
    fm = json.load(open('football_markets.json', encoding='utf-8'))
    C = 0.045
    for e in fm['events']:
        ho = byteam.get(NAME2CODE.get(e.get('home'), ''), [])
        ao = byteam.get(NAME2CODE.get(e.get('away'), ''), [])
        haw, hdw = burden(ho); aaw, adw = burden(ao)
        fav = e.get('a', '')
        rows1 = [m for m in e['markets'] if m['key'] == '1x2']
        m1 = next((m for m in rows1 if fav and fav in m['name']), rows1[0] if rows1 else None)
        base = m1['our'] if m1 else 0.5
        fav_home = (e.get('a') == e.get('home'))
        own_a, own_d = (haw, hdw) if fav_home else (aaw, adw)
        opp_a, opp_d = (aaw, adw) if fav_home else (haw, hdw)
        net = (opp_a + 0.5 * opp_d) - (own_a + 0.5 * own_d)
        logit = math.log(base / (1 - base)) if 0 < base < 1 else 0
        adj = 1 / (1 + math.exp(-(logit + C * net)))

        def top(o):
            k = [f"{x['player']} ({x['pos']}, {x['injury']})" for x in o if 'F' in x['pos'] or 'M' in x['pos']]
            return k[:3] or [f"{x['player']} ({x['pos']})" for x in o[:2]]
        e['intel'] = {'homeOut': len(ho), 'awayOut': len(ao), 'homeKey': top(ho), 'awayKey': top(ao),
                      'base': round(base, 3), 'adj': round(adj, 3)}
    json.dump(fm, open('football_markets.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  травмы: добавлены (фид EPL {len(inj)})")


# ==================== TENNIS / MMA (β, market-only) ====================
def build_market_only(sportkeys, outfile, label, limit=12):
    if isinstance(sportkeys, str):
        sportkeys = [sportkeys]
    raw = []
    for k in sportkeys:
        raw += get_odds(k, 'h2h')
    picks = []
    for m in raw:
        if not fut(m.get('commence_time', '')):
            continue
        med = med_h2h(m)
        if len(med) < 2:
            continue
        names = list(med.keys())
        a, b = names[0], names[1]
        dv = devig([med[a], med[b]])
        if not dv:
            continue
        fav_i = 0 if med[a] <= med[b] else 1
        fav = names[fav_i]
        picks.append({'a': a, 'b': b, 'mk': f'Победа: {fav}', 'fav': fav,
                      'our': round(dv[fav_i], 4), 'ext': round(dv[fav_i], 4),
                      'odds': round(med[fav], 2), 'modeled': False,
                      'when': whenstr(m['commence_time']), 'iso': m['commence_time']})
    picks.sort(key=lambda x: x['when'])
    picks = picks[:limit]
    json.dump(picks, open(outfile, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  {label}: {len(picks)} будущих (β, только рынок)")
    return picks


# ==================== MMA: модель Elo по бойцам ====================
def build_mma_elo():
    """Строит рейтинги бойцов Elo из истории боёв UFC (Greco1899). Возвращает {боец: (rating, n)}."""
    import csv, io
    try:
        ev = {}
        for r in csv.DictReader(io.StringIO(fetch("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_event_details.csv"))):
            ev[r['EVENT'].strip()] = r.get('DATE', '')
        fights = list(csv.DictReader(io.StringIO(fetch("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_results.csv"))))
    except Exception as e:
        print(f"  ! MMA данные: {e}")
        return {}

    def pdate(s):
        for f in ('%B %d, %Y',):
            try:
                return datetime.datetime.strptime(s.strip(), f)
            except Exception:
                pass
        return datetime.datetime(1994, 1, 1)
    parsed = []
    for r in fights:
        bout = r.get('BOUT', '')
        out = (r.get('OUTCOME') or '').strip()
        if ' vs. ' not in bout or out not in ('W/L', 'L/W', 'D/D'):
            continue
        a, b = [x.strip() for x in bout.split(' vs. ', 1)]
        parsed.append((pdate(ev.get(r['EVENT'].strip(), '')), a, b, out))
    parsed.sort(key=lambda x: x[0])
    R, N = defaultdict(lambda: 1500.0), defaultdict(int)
    K = 32
    for _, a, b, out in parsed:
        Ra, Rb = R[a], R[b]
        Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))
        sa = 1.0 if out == 'W/L' else (0.0 if out == 'L/W' else 0.5)
        R[a] = Ra + K * (sa - Ea); R[b] = Rb + K * ((1 - sa) - (1 - Ea))
        N[a] += 1; N[b] += 1
    print(f"  MMA Elo: {len(R)} бойцов из {len(parsed)} боёв")
    return {k: (R[k], N[k]) for k in R}


def build_mma(sportkeys, outfile, limit=16):
    if isinstance(sportkeys, str):
        sportkeys = [sportkeys]
    elo = build_mma_elo()
    keys = list(elo.keys())

    def rate(name):
        if name in elo:
            return elo[name]
        mm = difflib.get_close_matches(name, keys, n=1, cutoff=0.86)
        return elo[mm[0]] if mm else None
    raw = []
    for k in sportkeys:
        raw += get_odds(k, 'h2h')
    picks, modeled = [], 0
    for m in raw:
        if not fut(m.get('commence_time', '')):
            continue
        med = med_h2h(m)
        if len(med) < 2:
            continue
        names = list(med.keys())
        a, b = names[0], names[1]
        dv = devig([med[a], med[b]])
        if not dv:
            continue
        ra, rb = rate(a), rate(b)
        disagree = 0.0
        if ra and rb and ra[1] >= 3 and rb[1] >= 3:  # оба в базе с ≥3 боями → наша модель
            model_a = 1 / (1 + 10 ** ((rb[0] - ra[0]) / 400))
            # ДИСЦИПЛИНА (урок футбола): грубый Elo рынок не бьёт → усадка к рынку.
            disagree = abs(model_a - dv[0])
            our_a = W_MMA * model_a + (1 - W_MMA) * dv[0]
            is_model = True
        else:
            our_a = dv[0]
            is_model = False
        our = [our_a, 1 - our_a]
        fav_i = 0 if our[0] >= our[1] else 1
        fav = names[fav_i]
        picks.append({'a': a, 'b': b, 'mk': f'Победа: {fav}', 'fav': fav,
                      'our': round(our[fav_i], 4), 'ext': round(dv[fav_i], 4), 'odds': round(med[fav], 2),
                      'modeled': is_model, 'disagree': round(disagree, 3),
                      'when': whenstr(m['commence_time']), 'iso': m['commence_time']})
        if is_model:
            modeled += 1
    picks.sort(key=lambda x: x['when'])
    picks = picks[:limit]
    json.dump(picks, open(outfile, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  MMA: {len(picks)} боёв ({modeled} по модели Elo, остальные β)")
    return picks


# ==================== ТЕННИС: surface-aware Elo ====================
TENNIS_LFS = "https://media.githubusercontent.com/media/hikmatazimzade/tennis-ai/main/data"
TENNIS_YEARS = list(range(2016, 2025))
W_TENNIS = 0.55  # вес Elo против рынка (рейтинги по 2024, рынок несёт свежую форму)


def build_tennis_elo():
    """Surface-aware Elo по истории ATP (Sackmann-зеркало, 2016–2024).
    Возвращает dict: name -> {'o':overall, 'h':hard, 'c':clay, 'g':grass, 'n':matches, 'ns':{surf:cnt}}."""
    import csv, io
    matches = []
    for y in TENNIS_YEARS:
        try:
            raw = fetch(f"{TENNIS_LFS}/atp_matches_{y}.csv", t=30)
            for r in csv.DictReader(io.StringIO(raw)):
                if r.get('winner_name') and r.get('loser_name'):
                    matches.append((r.get('tourney_date', ''), r.get('match_num', '0'),
                                    r['winner_name'].strip(), r['loser_name'].strip(),
                                    (r.get('surface') or 'Hard').strip()))
        except Exception as e:
            print(f"  ! теннис {y}: {e}")
    matches.sort(key=lambda x: (x[0], int(x[1]) if str(x[1]).isdigit() else 0))
    SK = {'Hard': 'h', 'Clay': 'c', 'Grass': 'g'}
    R = defaultdict(lambda: {'o': 1500.0, 'h': 1500.0, 'c': 1500.0, 'g': 1500.0, 'n': 0, 'ns': defaultdict(int)})

    def kf(n):
        return 250.0 / ((n + 5) ** 0.4)
    for _, _, w, l, surf in matches:
        sk = SK.get(surf, 'h')
        rw, rl = R[w], R[l]
        # прогноз-блэнд поверхность+общий
        do = rw['o'] - rl['o']; ds = rw[sk] - rl[sk]
        diff = 0.6 * ds + 0.4 * do
        Ew = 1 / (1 + 10 ** (-diff / 400))
        kw, klv = kf(rw['n']), kf(rl['n'])
        rw['o'] += kw * (1 - Ew); rl['o'] += klv * (0 - (1 - Ew))
        rw[sk] += kw * (1 - Ew); rl[sk] += klv * (0 - (1 - Ew))
        rw['n'] += 1; rl['n'] += 1; rw['ns'][sk] += 1; rl['ns'][sk] += 1
    print(f"  Теннис Elo: {len(R)} игроков из {len(matches)} матчей (ATP 2016–2024)")
    return {k: dict(o=v['o'], h=v['h'], c=v['c'], g=v['g'], n=v['n'], ns=dict(v['ns'])) for k, v in R.items()}


def _surface_of(sportkey):
    k = sportkey.lower()
    if 'french' in k or 'roland' in k: return 'c'
    if 'wimbledon' in k: return 'g'
    return 'h'  # US Open, Australian, большинство — хард (грубо)


def build_tennis(sportkeys, outfile, limit=20):
    if isinstance(sportkeys, str):
        sportkeys = [sportkeys]
    elo = build_tennis_elo()
    keys = list(elo.keys())

    def rate(name):
        if name in elo:
            return elo[name]
        mm = difflib.get_close_matches(name, keys, n=1, cutoff=0.84)
        return elo[mm[0]] if mm else None
    picks, modeled = [], 0
    for k in sportkeys:
        surf = _surface_of(k)
        for m in get_odds(k, 'h2h'):
            if not fut(m.get('commence_time', '')):
                continue
            med = med_h2h(m)
            if len(med) < 2:
                continue
            a, b = list(med.keys())[:2]
            dv = devig([med[a], med[b]])
            if not dv:
                continue
            ra, rb = rate(a), rate(b)
            disagree = 0.0
            if ra and rb and ra['n'] >= 10 and rb['n'] >= 10:
                do = ra['o'] - rb['o']; ds = ra[surf] - rb[surf]
                diff = 0.6 * ds + 0.4 * do
                model_a = 1 / (1 + 10 ** (-diff / 400))
                disagree = abs(model_a - dv[0])
                our_a = W_TENNIS * model_a + (1 - W_TENNIS) * dv[0]
                is_model = True
            else:
                our_a = dv[0]; is_model = False
            our = [our_a, 1 - our_a]
            fav_i = 0 if our[0] >= our[1] else 1
            fav = [a, b][fav_i]
            picks.append({'a': a, 'b': b, 'mk': f'Победа: {fav}', 'fav': fav,
                          'our': round(our[fav_i], 4), 'ext': round(dv[fav_i], 4), 'odds': round(med[fav], 2),
                          'modeled': is_model, 'disagree': round(disagree, 3),
                          'when': whenstr(m['commence_time']), 'iso': m['commence_time']})
            if is_model:
                modeled += 1
    picks.sort(key=lambda x: x['when'])
    picks = picks[:limit]
    json.dump(picks, open(outfile, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  теннис: {len(picks)} матчей ({modeled} по модели surface-Elo, остальные β)")
    return picks


# ==================== 4. BUILD KUPON ====================
def build_kupon():
    fm = json.load(open('football_markets.json', encoding='utf-8'))
    football = []
    football_pool = []
    SAFE_KEYS = ('dc', 'ou', '1x2')  # рынки, из которых выбираем «самую надёжную»
    for e in fm['events']:
        mw = e.get('midweek', False)
        intel = e.get('intel', {})
        markets = e.get('markets', [])
        # компактный список всех рынков для витрины (наша оценка по каждому)
        allmk = [{'grp': m.get('grp', ''), 'name': m['name'], 'our': round((m.get('our') or 0) * 100),
                  'odds': m.get('odds'), 'val': (round((shrink(m['our'], m['ext'], m['key'], mw) * m['odds'] - 1) * 100)
                                                 if m.get('odds') and m.get('ext') else None)}
                 for m in markets if m.get('our') is not None]
        # value-пики по всем рынкам с линией
        best_val = None
        for m in markets:
            our, ext, odds = m.get('our'), m.get('ext'), m.get('odds')
            if not odds or not ext:
                continue
            shown = shrink(our, ext, m['key'], mw)
            val = shown * odds - 1
            coinflip = m['key'] in ('1x2', 'x', 'ah') and shown < COINFLIP
            cand = {'name': m['name'], 'grp': m.get('grp', ''), 'our': round(shown * 100), 'odds': odds,
                    'val': round(val * 100), 'coinflip': coinflip}
            if val > 0.005 and not coinflip and (best_val is None or val > best_val['val'] / 100):
                best_val = cand
        # самая надёжная: максимальная наша вероятность среди «надёжных» рынков (без монеток)
        safe = None
        for m in markets:
            if m['key'] not in SAFE_KEYS or m.get('our') is None:
                continue
            if m['key'] == '1x2' and m['our'] < 0.62:
                continue
            if safe is None or m['our'] > safe['our']:
                safe = {'name': m['name'], 'grp': m.get('grp', ''), 'our': round(m['our'] * 100),
                        'odds': m.get('odds')}
        if best_val:
            football.append({
                'sport': 'football', 'ev': f"{e['home']} — {e['away']}", 'when': e['when'], 'iso': e.get('iso'),
                'league': e.get('league'), 'market': best_val['name'], 'grp': best_val['grp'],
                'our': best_val['our'], 'odds': best_val['odds'], 'val': best_val['val'],
                'stake': round(min(STAKE_CAP, max(0.0, (best_val['our'] / 100 - 1 / best_val['odds']) / (best_val['odds'] - 1)) * 100 / 4), 2),
                'homeOut': intel.get('homeOut', 0), 'awayOut': intel.get('awayOut', 0),
                'adj': intel.get('adj'), 'homeKey': intel.get('homeKey', []), 'awayKey': intel.get('awayKey', []),
                'midweek': mw, 'coinflip': False, 'caution': best_val['val'] > CAUTION_VAL * 100 or mw,
            })
        football_pool.append({'sport': 'football', 'ev': f"{e['home']} — {e['away']}", 'when': e['when'],
                              'iso': e.get('iso'), 'league': e.get('league'), 'fav': e.get('a'),
                              'modeled': e.get('modeled', False), 'topscore': e.get('topscore'),
                              'markets': allmk, 'safe': safe, 'bestval': best_val})
    football.sort(key=lambda x: x['val'], reverse=True)
    football_pool.sort(key=lambda x: x['iso'] or '')

    def load_pool(f, sport):
        try:
            arr = json.load(open(f, encoding='utf-8'))
        except Exception:
            return []
        out = []
        for p in arr:
            our = p['our'] if p['our'] <= 1 else p['our'] / 100
            ext = p.get('ext', our)
            odds = p.get('odds')
            val = round((our * odds - 1) * 100) if odds else None
            dis = p.get('disagree', 0)
            modeled = p.get('modeled', False)
            # рекомендуем только вменяемое: модель, перевес есть, не андердог, модель не спорит с рынком
            rec = bool(modeled and val is not None and val >= 3 and our >= 0.5 and dis <= 0.18)
            caution = bool(modeled and (dis > 0.18) and val and val > 10)
            out.append({'sport': sport, 'ev': f"{p['a']} — {p['b']}", 'when': p['when'], 'iso': p.get('iso'),
                        'fav': p.get('fav'), 'odds': odds, 'ourm': round(our * 100),
                        'extm': round((ext or our) * 100), 'val': val, 'modeled': modeled,
                        'rec': rec, 'caution': caution})
        return out

    data = {
        'updated': NOW.strftime('%d.%m.%Y %H:%M UTC'),
        'book': BOOKMAKER_NAME,
        'football': football,
        'football_pool': football_pool,
        'tennis': load_pool('tennis_markets.json', 'tennis'),
        'mma': load_pool('mma_markets.json', 'mma'),
        'calib': calib_summary(),
    }
    json.dump(data, open('kupon_data.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  купон: {len(football)} value-пиков, слейт футбола {len(football_pool)}, теннис {len(data['tennis'])}, MMA {len(data['mma'])}")
    return data


# ==================== 5. BUILD PAGES ====================
LOG = 'predictions_log.json'


def _load_log():
    try:
        return json.load(open(LOG, encoding='utf-8'))
    except Exception:
        return []


def log_predictions():
    """Логируем КАЖДЫЙ прогноз (что показали) с временем — чтобы потом авто-оценить.
    Это ядро калибровки: без журнала прогноз-против-факта нет ни CLV, ни трек-рекорда."""
    log = _load_log()
    seen = {e['id'] for e in log}
    added = 0

    def add(sport, ev, market, shown, ext, odds, iso, match):
        _id = f"{sport}|{ev}|{market}|{(iso or '')[:10]}"
        if _id in seen:
            return 0
        log.append({'id': _id, 'built': NOW.isoformat(), 'sport': sport, 'ev': ev, 'market': market,
                    'shown': round(shown, 4) if shown is not None else None,
                    'ext': round(ext, 4) if ext else None, 'odds': odds, 'iso': iso,
                    'match': match, 'status': 'pending', 'won': None, 'actual': None})
        seen.add(_id)
        return 1

    try:
        fm = json.load(open('football_markets.json', encoding='utf-8'))
        for e in fm['events']:
            mw = e.get('midweek', False)
            fav = e.get('a', '')
            m = {'home': e.get('home'), 'away': e.get('away'), 'fav': fav}
            for mk in e['markets']:
                # логируем только однозначно оцениваемые рынки: победа фаворита, тотал «больше», обе-да
                nm = mk.get('name', '')
                gradeable = ((mk['key'] == '1x2' and fav and fav in nm) or
                             (mk['key'] == 'ou' and 'Больше' in nm) or
                             (mk['key'] == 'btts' and nm == 'Да'))
                if not gradeable:
                    continue
                shown = shrink(mk.get('our'), mk.get('ext'), mk['key'], mw)
                added += add('football', f"{e['home']} — {e['away']}", nm, shown,
                             mk.get('ext'), mk.get('odds'), e.get('iso') or e.get('when'), {**m, 'key': mk['key']})
    except Exception as ex:
        print(f"  ! log football: {ex}")
    for f, sp in (('tennis_markets.json', 'tennis'), ('mma_markets.json', 'mma')):
        try:
            for p in json.load(open(f, encoding='utf-8')):
                added += add(sp, f"{p['a']} — {p['b']}", p['mk'], p['our'], p.get('ext'), p['odds'],
                             p.get('iso') or p.get('when'), {'a': p['a'], 'b': p['b'], 'fav': p['fav'], 'key': 'ml'})
        except Exception:
            pass
    json.dump(log, open(LOG, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  журнал: +{added} прогнозов (всего {len(log)})")


def _sim(a, b):
    return difflib.SequenceMatcher(None, (a or '').lower(), (b or '').lower()).ratio()


def _scores(sportkeys):
    out = []
    for k in sportkeys:
        try:
            d = json.loads(fetch(f"{ODDS}/sports/{k}/scores/?apiKey={KEY}&daysFrom=3"))
        except Exception:
            continue
        for x in d:
            if x.get('completed') and x.get('scores'):
                sc = {s['name']: _num(s['score']) for s in x['scores']}
                out.append({'home': x['home_team'], 'away': x['away_team'], 'sc': sc})
    return out


def _num(x):
    try:
        return int(x)
    except Exception:
        return None


def grade_predictions():
    """Оцениваем pending-прогнозы по реальным счётам (the-odds-api /scores)."""
    log = _load_log()
    foot = _scores(['soccer_epl'])
    ten = _scores(['tennis_atp_us_open', 'tennis_wta_us_open'])
    mma = _scores(['mma_mixed_martial_arts'])
    graded = 0

    def match_game(a, b, pool):
        # ОРИЕНТИРОВАННОЕ сопоставление: (a→home & b→away) ЛИБО (a→away & b→home).
        # Иначе «Man United vs Man City» ошибочно липнет к «Man City vs Coventry».
        best, bs = None, 0
        for r in pool:
            s = max(min(_sim(a, r['home']), _sim(b, r['away'])),
                    min(_sim(a, r['away']), _sim(b, r['home'])))
            if s > bs:
                bs, best = s, r
        return best if bs >= 0.85 else None

    for e in log:
        if e['status'] != 'pending':
            continue
        m = e.get('match', {})
        sp = e['sport']
        if sp == 'football':
            r = match_game(m.get('home'), m.get('away'), foot)
            if not r:
                continue
            gh, ga = r['sc'].get(r['home']), r['sc'].get(r['away'])
            if gh is None or ga is None:
                continue
            # ориентируем счёт на home/away прогноза
            hp = m.get('home')
            if _sim(hp, r['home']) >= _sim(hp, r['away']):
                ghh, gaa = gh, ga
            else:
                ghh, gaa = ga, gh
            tot = ghh + gaa
            key = m.get('key')
            won = None
            if key == '1x2':
                favhome = (m.get('fav') == m.get('home'))
                won = (ghh > gaa) if favhome else (gaa > ghh)
            elif key == 'ou':
                import re
                nums = re.findall(r'[\d.]+', e['market'])
                if nums:
                    won = tot > float(nums[-1])
            elif key == 'btts':
                won = (ghh > 0 and gaa > 0)
            if won is None:
                continue
            e['won'] = bool(won)
            e['actual'] = f"{m.get('home')} {ghh}-{gaa} {m.get('away')}"
            e['status'] = 'graded'
            graded += 1
        else:
            pool = ten if sp == 'tennis' else mma
            r = match_game(m.get('a'), m.get('b'), pool)
            if not r:
                continue
            hs, as_ = r['sc'].get(r['home']), r['sc'].get(r['away'])
            if hs is None or as_ is None:
                continue
            winner = r['home'] if hs > as_ else r['away']
            e['won'] = _sim(winner, m.get('fav')) > 0.75
            e['actual'] = f"{winner} прошёл ({hs}-{as_})"
            e['status'] = 'graded'
            graded += 1
    json.dump(log, open(LOG, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"  оценка: +{graded} прогнозов оценено")
    return log


def calib_summary():
    """Сводка калибровки по оценённым прогнозам (для панели в Купоне)."""
    log = _load_log()
    g = [e for e in log if e['status'] == 'graded' and e.get('shown') is not None]
    if not g:
        return {'n': 0}
    def block(items):
        if not items:
            return None
        n = len(items)
        hits = sum(1 for e in items if e['won'])
        exp = sum(e['shown'] for e in items)
        brier = statistics.mean([(e['shown'] - (1 if e['won'] else 0)) ** 2 for e in items])
        return {'n': n, 'hits': hits, 'exp': round(exp, 1), 'brier': round(brier, 3)}
    groups = {
        'all': g,
        '1x2': [e for e in g if e['match'].get('key') == '1x2'],
        'ou': [e for e in g if e['match'].get('key') == 'ou'],
        'btts': [e for e in g if e['match'].get('key') == 'btts'],
        'tennis': [e for e in g if e['sport'] == 'tennis'],
        'mma': [e for e in g if e['sport'] == 'mma'],
    }
    return {k: block(v) for k, v in groups.items()}


def build_stab():
    """Штаб = единый мобильный продукт. Витрина+рекомендации ведутся от POOL (kupon_data):
    те же честные ставки, что и раньше, но всё внутри Пиццерии. Плейсхолдеры: __IMGDATA__, __POOL__."""
    import re
    if not os.path.exists('tmpl2.html'):
        print("  ! tmpl2.html нет — Штаб пропущен")
        return
    img = None
    if os.path.exists('board_img.txt'):
        img = open('board_img.txt').read().strip()
    if not img and os.path.exists('pizzeria_hero.html'):
        m = re.search(r'data:image/[^"\']{200,}', open('pizzeria_hero.html', encoding='utf-8').read())
        img = m.group(0) if m else ''
    img = img or ''
    try:
        pool = json.load(open('kupon_data.json', encoding='utf-8'))
    except Exception:
        pool = {'updated': NOW.strftime('%d.%m.%Y %H:%M UTC'), 'football': [], 'tennis': [], 'mma': [], 'calib': {}}
    html = open('tmpl2.html', encoding='utf-8').read()
    html = html.replace('__POOL__', json.dumps(pool, ensure_ascii=False))
    html = html.replace('__IMGDATA__', img)
    open('pizzeria_hero.html', 'w', encoding='utf-8').write(html)
    nf = len([p for p in pool.get('football', []) if p.get('stake', 0) > 0 and not p.get('caution')])
    print(f"  Штаб обновлён (ставить: {nf} футбол, витрина: теннис {len(pool.get('tennis', []))}, MMA {len(pool.get('mma', []))})")


def build_pages():
    build_stab()
    data = json.load(open('kupon_data.json', encoding='utf-8'))
    # Купон
    if os.path.exists('kupon.html'):
        html = open('kupon.html', encoding='utf-8').read()
        import re
        html = re.sub(r'const DATA=.*?;\n', f'const DATA={json.dumps(data, ensure_ascii=False)};\n', html, count=1, flags=re.S)
        open('kupon.html', 'w', encoding='utf-8').write(html)
        # sanity: extract script and node --check done by caller
        print("  страница Купон обновлена")
    else:
        print("  ! kupon.html не найден — пропуск")


def main():
    step = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if not KEY and step in ('all', 'fetch', 'model'):
        raise SystemExit("нет ODDS_API_KEY (export ODDS_API_KEY=...)")
    if step in ('all', 'fetch', 'model'):
        print("[1/6] футбольная модель + кэфы")
        build_football()
        print("[2/6] травмы")
        add_injuries()
        print("[3/6] теннис/MMA (β, авто-поиск активных турниров)")
        tk = active_sports('tennis') or ['tennis_atp_us_open', 'tennis_wta_us_open']
        mk = active_sports('mma') or ['mma_mixed_martial_arts']
        build_tennis(tk, 'tennis_markets.json', limit=20)
        build_mma(mk, 'mma_markets.json', limit=16)
    if step in ('all', 'log', 'grade'):
        print("[4/6] журнал прогнозов + оценка по фактам")
        log_predictions()   # логируем текущий билд (dedup по id)
        if KEY:
            grade_predictions()   # оцениваем всё, что уже сыграло
    if step in ('all', 'kupon'):
        print("[5/6] купон")
        build_kupon()
    if step in ('all', 'pages'):
        print("[6/6] страницы")
        build_pages()
    print("готово.")


if __name__ == '__main__':
    main()
