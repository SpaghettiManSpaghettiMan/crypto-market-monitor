#!/usr/bin/env python3
"""
WatchlistGenerator.py
Kraken USD Pair Scanner for SwingTrader
═══════════════════════════════════════
Pulls all Kraken USD pairs, applies liquidity and volatility filters,
scores survivors for swing-trade viability, and outputs a ranked shortlist.

Usage:  python WatchlistGenerator.py
Output: Terminal report + watchlist.json

"""

import requests
import time
import math
import json
import os
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════════
#  CONFIG — tune these knobs
# ═══════════════════════════════════════════════════════════════════

# Minimum 24h USD volume to pass liquidity gate
MIN_VOLUME_USD = 500_000

# Maximum bid-ask spread as % of mid price
MAX_SPREAD_PCT = 0.50

# Minimum composite score to qualify
MIN_SCORE = 40

# ATR sweet spot (daily ATR as % of price)
ATR_MIN_PCT = 1.5     # below this = not enough movement for swings
ATR_MAX_PCT = 12.0    # above this = too chaotic / memecoin territory

# How many daily candles to pull for analysis
LOOKBACK_DAYS = 30

# Pairs to always skip (stablecoins, wrapped, pegged, EUR-stables)
BLACKLIST_BASES = {
    'USDT', 'USDC', 'DAI', 'PYUSD', 'TUSD', 'BUSD', 'GUSD',
    'PAX', 'UST',
    'EURT', 'EUROC',
    'WBTC', 'WETH',
}

# Pairs you're already running in SwingTrader
# Update this list to match your current SWINGTRADER config
CURRENT_PAIRS = {
    'XBTUSD', 'ETHUSD', 'SOLUSD', 'XRPUSD', 'ZECUSD',
    'BNBUSD', 'LTCUSD', 'BCHUSD',
}

# Rate limit delay between OHLC fetches (seconds)
API_DELAY = 1.2

# Save results to JSON alongside this script
SAVE_JSON = True


# ═══════════════════════════════════════════════════════════════════
#  KRAKEN PUBLIC API
# ═══════════════════════════════════════════════════════════════════

BASE_URL = "https://api.kraken.com/0/public"


def kraken_get(endpoint, params=None):
    """Make a Kraken public API request with basic error handling."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        errs = [e for e in data.get('error', []) if not e.startswith('EGeneral:Invalid')]
        if errs:
            print(f"  ⚠  Kraken API: {errs}")
            return None
        return data.get('result', {})
    except Exception as e:
        print(f"  ✗  Request failed ({endpoint}): {e}")
        return None


def fetch_all_pairs():
    """Get all available trading pairs from Kraken."""
    print("  Fetching asset pairs...")
    return kraken_get("AssetPairs") or {}


def fetch_tickers(pair_list):
    """Batch-fetch ticker data. Chunks to avoid URL length limits."""
    all_tickers = {}
    chunk_size = 30
    for i in range(0, len(pair_list), chunk_size):
        chunk = pair_list[i:i + chunk_size]
        tag = f"{i + 1}-{min(i + chunk_size, len(pair_list))}/{len(pair_list)}"
        print(f"  Fetching tickers ({tag})...")
        result = kraken_get("Ticker", {"pair": ",".join(chunk)})
        if result:
            all_tickers.update(result)
        time.sleep(0.5)
    return all_tickers


def fetch_ohlc(pair, interval=1440):
    """Fetch daily OHLC candles for a single pair."""
    since = int((datetime.now() - timedelta(days=LOOKBACK_DAYS + 5)).timestamp())
    result = kraken_get("OHLC", {"pair": pair, "interval": interval, "since": since})
    if not result:
        return []
    # Result key may not match input pair name exactly
    for key, val in result.items():
        if key != 'last' and isinstance(val, list):
            return val
    return []


# ═══════════════════════════════════════════════════════════════════
#  FILTER PIPELINE
# ═══════════════════════════════════════════════════════════════════

def extract_usd_pairs(all_pairs):
    """Return only USD-quoted, non-blacklisted, non-darkpool pairs."""
    usd_pairs = {}

    for pair_key, info in all_pairs.items():
        quote = info.get('quote', '')
        if quote not in ('ZUSD', 'USD'):
            continue

        base = info.get('base', '')
        alt  = info.get('altname', pair_key)

        # Strip Kraken's X/Z prefixes for blacklist matching
        clean = base.lstrip('XZ') if len(base) > 3 else base
        if clean in BLACKLIST_BASES or base in BLACKLIST_BASES:
            continue

        # Skip dark-pool pairs
        if '.d' in pair_key.lower() or '.d' in alt.lower():
            continue

        usd_pairs[pair_key] = {
            'key':        pair_key,
            'altname':    alt,
            'wsname':     info.get('wsname', alt),
            'base':       base,
            'clean_base': clean,
        }

    return usd_pairs


def apply_liquidity_gate(usd_pairs, tickers):
    """Cut pairs that fail volume or spread thresholds."""
    survivors = {}
    cut_log = []

    for pair_key, info in usd_pairs.items():
        # Find ticker — key might differ from pair_key
        ticker = tickers.get(pair_key)
        if not ticker:
            # Try matching by altname
            for tk, tv in tickers.items():
                if tk.startswith(info['base']):
                    ticker = tv
                    break
        if not ticker:
            cut_log.append((info['altname'], "no ticker data"))
            continue

        # Volume (v = [today, 24h])
        try:
            vol_24h = float(ticker['v'][1])  # volume in base currency
            last_price = float(ticker['c'][0])
            vol_usd = vol_24h * last_price
        except (KeyError, ValueError, IndexError):
            cut_log.append((info['altname'], "bad ticker data"))
            continue

        if vol_usd < MIN_VOLUME_USD:
            cut_log.append((info['altname'], f"vol ${vol_usd:,.0f} < ${MIN_VOLUME_USD:,.0f}"))
            continue

        # Spread
        try:
            bid = float(ticker['b'][0])
            ask = float(ticker['a'][0])
            mid = (bid + ask) / 2
            spread_pct = ((ask - bid) / mid) * 100 if mid > 0 else 999
        except (KeyError, ValueError, IndexError):
            spread_pct = 999

        if spread_pct > MAX_SPREAD_PCT:
            cut_log.append((info['altname'], f"spread {spread_pct:.3f}% > {MAX_SPREAD_PCT}%"))
            continue

        info['vol_usd']    = vol_usd
        info['last_price'] = last_price
        info['spread_pct'] = spread_pct
        survivors[pair_key] = info

    return survivors, cut_log


# ═══════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS (lightweight, from daily candles)
# ═══════════════════════════════════════════════════════════════════

def compute_atr(candles, period=14):
    """Average True Range from OHLC candles."""
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        try:
            high  = float(candles[i][2])
            low   = float(candles[i][3])
            close = float(candles[i - 1][4])
            tr = max(high - low, abs(high - close), abs(low - close))
            trs.append(tr)
        except (IndexError, ValueError):
            continue
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def compute_rsi(candles, period=14):
    """RSI-14 from daily candle closes."""
    closes = []
    for c in candles:
        try:
            closes.append(float(c[4]))
        except (IndexError, ValueError):
            continue
    if len(closes) < period + 1:
        return None
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))


def compute_trend_slope(candles, window=10):
    """Normalized slope of last N closes (linear regression approximation)."""
    closes = []
    for c in candles[-window:]:
        try:
            closes.append(float(c[4]))
        except (IndexError, ValueError):
            continue
    if len(closes) < 5:
        return 0.0
    n = len(closes)
    x_mean = (n - 1) / 2
    y_mean = sum(closes) / n
    numer = sum((i - x_mean) * (closes[i] - y_mean) for i in range(n))
    denom = sum((i - x_mean) ** 2 for i in range(n))
    if denom == 0 or y_mean == 0:
        return 0.0
    slope = numer / denom
    return slope / y_mean  # normalize to price


def compute_swing_count(candles, threshold=0.03):
    """Count direction reversals > threshold in the lookback period."""
    closes = []
    for c in candles:
        try:
            closes.append(float(c[4]))
        except (IndexError, ValueError):
            continue
    if len(closes) < 3:
        return 0
    swings = 0
    direction = None
    anchor = closes[0]
    for price in closes[1:]:
        pct = (price - anchor) / anchor if anchor > 0 else 0
        if direction is None:
            if pct > threshold:
                direction = 'up'
                anchor = price
            elif pct < -threshold:
                direction = 'down'
                anchor = price
        elif direction == 'up':
            if price > anchor:
                anchor = price
            elif (anchor - price) / anchor > threshold:
                swings += 1
                direction = 'down'
                anchor = price
        elif direction == 'down':
            if price < anchor:
                anchor = price
            elif (price - anchor) / anchor > threshold:
                swings += 1
                direction = 'up'
                anchor = price
    return swings


# ═══════════════════════════════════════════════════════════════════
#  SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════

def score_pair(info, candles):
    """
    Score a pair for swing-trade viability (0-100 composite).
    Components (each 0-100, weighted):
      volume  20%  — log-scaled 24h USD volume
      spread  10%  — tighter = better
      atr     25%  — sweet spot ~3-5% daily; penalize flat or chaotic
      trend   15%  — absolute MA slope (clear direction = more setups)
      rsi     15%  — proximity to SwingTrader entry zones (≤35 or ≥65)
      swing   15%  — historical reversal count (more swings = more opportunity)
    """
    s = {}

    # ── Volume (log-scaled) ──
    vol_log = math.log10(max(info['vol_usd'], 1))
    s['volume'] = min(100, max(0, (vol_log - 5.0) * 33))

    # ── Spread ──
    s['spread'] = min(100, max(0, (MAX_SPREAD_PCT - info['spread_pct']) / MAX_SPREAD_PCT * 100))

    # ── ATR sweet spot ──
    atr = compute_atr(candles)
    if atr and info['last_price'] > 0:
        atr_pct = (atr / info['last_price']) * 100
        info['atr_pct'] = atr_pct
        center = 4.0
        if atr_pct < ATR_MIN_PCT:
            s['atr'] = 0
        elif atr_pct > ATR_MAX_PCT:
            s['atr'] = 10
        elif atr_pct <= center:
            s['atr'] = ((atr_pct - ATR_MIN_PCT) / (center - ATR_MIN_PCT)) * 100
        else:
            s['atr'] = max(10, 100 - ((atr_pct - center) / (ATR_MAX_PCT - center)) * 90)
    else:
        s['atr'] = 0
        info['atr_pct'] = 0.0

    # ── Trend clarity ──
    slope = compute_trend_slope(candles)
    info['trend_slope'] = slope
    s['trend'] = min(100, abs(slope) * 200)

    # ── RSI proximity to entry zones ──
    rsi = compute_rsi(candles)
    info['rsi'] = rsi
    if rsi is not None:
        if rsi <= 35:
            s['rsi'] = min(100, (40 - rsi) * 10)
        elif rsi >= 65:
            s['rsi'] = min(100, (rsi - 60) * 10)
        else:
            s['rsi'] = 30   # mid-range: could set up, modest score
    else:
        s['rsi'] = 0

    # ── Swing count ──
    swings = compute_swing_count(candles)
    info['swing_count'] = swings
    s['swing'] = min(100, swings * 17)

    # ── Composite ──
    weights = {
        'volume': 0.20, 'spread': 0.10, 'atr': 0.25,
        'trend':  0.15, 'rsi':    0.15, 'swing': 0.15,
    }
    s['composite'] = round(sum(s[k] * weights[k] for k in weights), 1)

    info['scores'] = s
    return info


# ═══════════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════════

def _row(p):
    """Format one table row."""
    rsi = p.get('rsi')
    rsi_str = f"{rsi:5.1f}" if rsi else "  N/A"
    flag = ""
    if rsi and rsi <= 35:
        flag = " 🟢 buy zone"
    elif rsi and rsi >= 65:
        flag = " 🟡 overbought"
    return (
        f"  {p['altname']:<12} {p['scores']['composite']:>6.1f} "
        f"${p['vol_usd']:>11,.0f} {p['spread_pct']:>7.3f}% "
        f"{p.get('atr_pct', 0):>6.2f}% {rsi_str:>6} "
        f"{p.get('swing_count', 0):>5}{flag}"
    )


def print_report(scored, cut_log, total_pairs):
    """Formatted terminal report."""
    W = 90

    # Separate active from candidates
    active = {}
    candidates_dict = {}
    for k, v in scored.items():
        alt = v.get('altname', k)
        if alt in CURRENT_PAIRS or k in CURRENT_PAIRS:
            active[k] = v
        else:
            candidates_dict[k] = v

    # Sort by composite score
    active_ranked     = sorted(active.values(), key=lambda p: p['scores']['composite'], reverse=True)
    candidates_ranked = sorted(candidates_dict.values(), key=lambda p: p['scores']['composite'], reverse=True)

    hdr = (f"  {'Pair':<12} {'Score':>6} {'24h Volume':>12} {'Spread':>8} "
           f"{'ATR%':>7} {'RSI':>6} {'Swings':>6}")
    sep = "  " + "─" * (W - 2)

    print(f"\n{'═' * W}")
    print(f"  WATCHLIST GENERATOR — SCAN RESULTS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  "
          f"{total_pairs} total → {len(scored)} scored  |  "
          f"{len(cut_log)} filtered out")
    print(f"{'═' * W}")

    # ── Your active pairs ──
    if active_ranked:
        print(f"\n{'─' * W}")
        print(f"  YOUR ACTIVE PAIRS  ({len(active_ranked)} in SwingTrader)")
        print(f"{'─' * W}")
        print(hdr); print(sep)
        for p in active_ranked:
            print(_row(p))

    # ── New candidates (score ≥ threshold) ──
    qualified   = [p for p in candidates_ranked if p['scores']['composite'] >= MIN_SCORE]
    below_cut   = [p for p in candidates_ranked if p['scores']['composite'] < MIN_SCORE]

    print(f"\n{'─' * W}")
    print(f"  QUALIFIED CANDIDATES  (score ≥ {MIN_SCORE})")
    print(f"{'─' * W}")
    if qualified:
        print(hdr); print(sep)
        for i, p in enumerate(qualified, 1):
            print(f"  {i:<4}" + _row(p)[2:])
    else:
        print(f"  No pairs scored ≥ {MIN_SCORE}. Try lowering the threshold.")

    # Below threshold (compact)
    if below_cut:
        print(f"\n  Below threshold ({len(below_cut)}): ", end="")
        snippets = [f"{p['altname']} {p['scores']['composite']:.0f}" for p in below_cut[:10]]
        print(", ".join(snippets))
        if len(below_cut) > 10:
            print(f"  ... and {len(below_cut) - 10} more")

    # ── Score breakdowns for qualified ──
    if qualified:
        print(f"\n{'─' * W}")
        print(f"  SCORE BREAKDOWNS  (qualified candidates)")
        print(f"{'─' * W}")
        for p in qualified:
            sc = p['scores']
            print(f"  {p['altname']:<12}  "
                  f"vol:{sc['volume']:4.0f}  spr:{sc['spread']:4.0f}  "
                  f"atr:{sc['atr']:4.0f}  trend:{sc['trend']:4.0f}  "
                  f"rsi:{sc['rsi']:4.0f}  swing:{sc['swing']:4.0f}  "
                  f"→ {sc['composite']:5.1f}")

    # ── Buy zone alerts ──
    buy_zone = [p for p in (qualified + active_ranked)
                if p.get('rsi') and p['rsi'] <= 35 and p['scores']['composite'] >= MIN_SCORE]
    if buy_zone:
        print(f"\n{'═' * W}")
        print(f"  🟢 BUY ZONE ALERTS  (RSI ≤ 35 AND score ≥ {MIN_SCORE})")
        print(f"{'═' * W}")
        for p in buy_zone:
            in_st = "  ✓ active" if p['altname'] in CURRENT_PAIRS else "  ← candidate"
            print(f"  {p['altname']:<12} RSI {p['rsi']:5.1f}  "
                  f"score {p['scores']['composite']:5.1f}  "
                  f"ATR {p.get('atr_pct', 0):.2f}%  "
                  f"vol ${p['vol_usd']:,.0f}{in_st}")
    else:
        print(f"\n  No buy zone alerts (RSI ≤ 35 + score ≥ {MIN_SCORE}).")

    print(f"\n{'═' * W}\n")

    return qualified + active_ranked  # return ranked list for JSON save


def save_results(ranked):
    """Save results to watchlist.json."""
    output = {
        'generated': datetime.now().isoformat(),
        'config': {
            'min_volume_usd': MIN_VOLUME_USD,
            'max_spread_pct': MAX_SPREAD_PCT,
            'min_score': MIN_SCORE,
            'atr_range': [ATR_MIN_PCT, ATR_MAX_PCT],
            'lookback_days': LOOKBACK_DAYS,
        },
        'pairs': [],
    }
    for p in ranked:
        composite = p['scores']['composite']
        output['pairs'].append({
            'pair':        p['altname'],
            'qualified':   composite >= MIN_SCORE,
            'kraken_key':  p['key'],
            'price':       p['last_price'],
            'volume_usd':  round(p['vol_usd'], 2),
            'spread_pct':  round(p['spread_pct'], 4),
            'atr_pct':     round(p.get('atr_pct', 0), 3),
            'rsi':         round(p['rsi'], 1) if p.get('rsi') else None,
            'swing_count': p.get('swing_count', 0),
            'scores':      p['scores'],
        })

    filepath = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'watchlist.json')
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"  💾 Results saved → {filepath}")


# ═══════════════════════════════════════════════════════════════════
#  INTERACTIVE MENU
# ═══════════════════════════════════════════════════════════════════

def prompt_settings():
    """Interactive launch menu — hit Enter to accept defaults."""
    global MIN_VOLUME_USD, MAX_SPREAD_PCT, MIN_SCORE

    print("\n🔍 WatchlistGenerator — Kraken USD Pair Scanner")
    print("─" * 50)
    print("  Configure filters (Enter = default)\n")

    try:
        v = input(f"  Min 24h volume USD  [{MIN_VOLUME_USD:,.0f}]: ").strip().replace(',', '')
        if v:
            MIN_VOLUME_USD = float(v)

        s = input(f"  Max spread %        [{MAX_SPREAD_PCT}]: ").strip()
        if s:
            MAX_SPREAD_PCT = float(s)

        n = input(f"  Min score to qualify [{MIN_SCORE}]: ").strip()
        if n:
            MIN_SCORE = float(n)
    except (ValueError, EOFError):
        print("  ⚠  Bad input, using defaults.")

    print(f"\n  → vol ≥ ${MIN_VOLUME_USD:,.0f}  |  spread ≤ {MAX_SPREAD_PCT}%  |  score ≥ {MIN_SCORE}")
    print("─" * 50)


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    prompt_settings()
    print("\n  Scanning Kraken...\n")

    # 1 — All pairs
    all_pairs = fetch_all_pairs()
    if not all_pairs:
        print("✗ Failed to fetch asset pairs. Exiting."); return
    print(f"  ✓ {len(all_pairs)} total trading pairs on Kraken")

    # 2 — USD only, minus blacklist
    usd_pairs = extract_usd_pairs(all_pairs)
    print(f"  ✓ {len(usd_pairs)} USD pairs after blacklist filter")

    # 3 — Tickers (batched)
    tickers = fetch_tickers(list(usd_pairs.keys()))

    # 4 — Liquidity gate
    survivors, cut_log = apply_liquidity_gate(usd_pairs, tickers)
    print(f"  ✓ {len(survivors)} pairs passed liquidity gate "
          f"(vol ≥ ${MIN_VOLUME_USD:,.0f}, spread ≤ {MAX_SPREAD_PCT}%)")

    if not survivors:
        print("\n✗ No pairs survived filtering. Try lowering MIN_VOLUME_USD or raising MAX_SPREAD_PCT.")
        return

    # 5 — OHLC fetch + scoring (rate-limited)
    print(f"\n  Fetching daily candles & scoring ({len(survivors)} pairs)...\n")
    scored = {}
    for i, (pair_key, info) in enumerate(survivors.items(), 1):
        label = f"  [{i:>3}/{len(survivors)}] {info['altname']:<12}"
        print(f"{label}", end=" ", flush=True)

        candles = fetch_ohlc(pair_key)
        if len(candles) < 15:
            print("— insufficient data, skipped")
            cut_log.append((info['altname'], "insufficient candle data"))
            continue

        scored_info = score_pair(info, candles)
        scored[pair_key] = scored_info
        print(f"→ score {scored_info['scores']['composite']:5.1f}")

        time.sleep(API_DELAY)

    # 6 — Report
    ranked = print_report(scored, cut_log, len(all_pairs))

    # 7 — Save
    if SAVE_JSON and ranked:
        save_results(ranked)

    print("  Review candidates above and add promising pairs to SwingTrader config.\n")


if __name__ == "__main__":
    main()
