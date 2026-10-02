"""
SwingTrader.py — Multi-Day Swing Trading Script (v2 — Aggressive 1-3 Day)
==========================================================================
Places real spot orders on Kraken when explicitly enabled. Requires local config/keys.json.

Philosophy:
  - Enter at pullbacks to support (swing lows, 50MA)
  - Hold for 1-3 days targeting 6-12% gains
  - Trailing stop arms after +2%, locks profit from +4.5%
  - 4h + Daily signal timeframes — ignores short-term noise
  - 4 liquid pairs, meaningful position sizes

v2 changes:
  - Tuned for 1-3 day holds (tighter exits, faster TP, shorter sideways timer)
  - Fixed: watchdog retry counter, layer-in order tracking, partial sell
    confirmation, over-allocation guard, get_base_asset USDC bug,
    _momentum_strength None check
  - Added: re-entry cooldown (60min), API rate limiter, post-only sell limits
  - Shorter poll interval (3min), faster order expiry (30min)

Required API permissions:
  - Query Funds
  - Query Open Orders & Trades
  - Create & Modify Orders
  - Cancel & Close Orders

Outputs (same folder as LiveTrader):
  - swing_state.json          : persisted state
  - swing_trade_history.csv   : completed trades
  - data/event_log/swing_events.csv: event log

DO NOT run multiple instances against the same pairs/state.
"""

import time
import json
import csv
import os
import hashlib
import hmac
import base64
import urllib.request
import urllib.parse
import sys
from datetime import datetime, timezone, timedelta
from monitor import (run_supervised, Heartbeat, send_status_summary,
                     notify_buy, notify_sell)

heartbeat = Heartbeat()
_pair_budget_base = 0.0  # set at startup: (total_capital * 0.995) / num_pairs

# =============================================================================
#  PAIRS — fewer, liquid, swing-worthy
# =============================================================================

# =============================================================================
#  CONFIG LOADER  — reads config/swingtrader_config.json + config/pairs.json
# =============================================================================

def _config_path(filename):
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", filename)


def _load_swingtrader_config():
    """Load swingtrader_config.json. Falls back to safe defaults if missing."""
    path = _config_path("swingtrader_config.json")
    defaults = {
        "budget":  {"max_budget_usd": 490.00},
        "timing":  {
            "poll_interval_secs": 180,
            "discord_status_interval_mins": 45,
            "order_expiry_secs": 1800,
            "sell_limit_secs": 180,
            "reentry_cooldown_mins": 60,
        },
        "discord": {"webhook_file": "webhook_url.txt"},
        "keys":    {"keys_file": "keys.json"},
    }
    if not os.path.exists(path):
        print("  [CONFIG] swingtrader_config.json not found — using defaults")
        return defaults
    try:
        with open(path) as f:
            cfg = json.load(f)
        print("  [CONFIG] Loaded swingtrader_config.json")
        return cfg
    except Exception as e:
        print("  [CONFIG] Failed to load swingtrader_config.json: {} — using defaults".format(e))
        return defaults


def _load_pairs_config():
    _pairs_file = _config_path("pairs.json")
    if not os.path.exists(_pairs_file):
        print("  [CONFIG] pairs.json not found — safe mode; copy config/pairs.example.json to config/pairs.json")
        return [], {}
    try:
        with open(_pairs_file, "r") as pf:
            cfg = json.load(pf)
        return cfg["active_pairs"], cfg["pairs"]
    except Exception as e:
        print("  [CONFIG] Failed to load pairs.json: {} — safe mode".format(e))
        return [], {}


_swing_cfg   = _load_swingtrader_config()
ACTIVE_PAIRS, PAIRS = _load_pairs_config()

# =============================================================================
#  CONFIG  (v2 — tuned for 1-3 day aggressive swings)
# =============================================================================

API_KEY    = "YOUR_API_KEY_HERE"
API_SECRET = "YOUR_API_SECRET_HERE"

MAX_BUDGET_USD         = _swing_cfg["budget"]["max_budget_usd"]
LIVE_TRADING_ENABLED   = bool(_swing_cfg.get("live_trading_enabled", False))
POLL_INTERVAL_SECS     = _swing_cfg["timing"]["poll_interval_secs"]

SUPPORT_LOOKBACK_4H    = 720
SUPPORT_LOOKBACK_DAILY = 720
SUPPORT_LOOKBACK_WEEKLY = 100
CANDLE_BATCH_SIZE      = 720
PULLBACK_TOLERANCE     = 0.02
MA_PERIOD              = 50
MIN_BOUNCE_PCT         = 0.005

TRAILING_STOP_ARM_PCT  = 0.02              # [v2] arm at +2% (was +3%)
TRAILING_STOP_PCT      = 0.045             # [v2] 4.5% from HWM (was 6%)
SIDEWAYS_DAYS                       = _swing_cfg["timing"].get("sideways_days", 3.0)
RSI_EXHAUSTION_COOLDOWN_HRS         = _swing_cfg["timing"].get("rsi_exhaustion_cooldown_hrs", 24)
RSI_EXHAUSTION_THRESHOLD            = _swing_cfg["timing"].get("rsi_exhaustion_rsi_threshold", 72)
TREND_MODE_RSI_EXHAUSTION_THRESHOLD = _swing_cfg["timing"].get("trend_mode_rsi_exhaustion_threshold", 80)
MAX_HOLD_DAYS          = 4.0               # hard exit if not profitable after 4 days
SWING_BAND_PCT         = 0.03             # [v2] 3% band (was 4%)

ATR_PERIOD             = 14
TS_ATR_MULTIPLIER      = 1.5              # [v2] was 2.0
TP_ATR_MULTIPLIER      = 3.0              # [v2] was 4.0
TP_ATR_TIER2_MULT      = 4.5              # [v2] was 6.0
TP_ATR_TIER3_MULT      = 6.0              # [v2] was 8.0
TS_ATR_FLOOR           = 0.03             # [v2] was 0.04
TS_ATR_CAP             = 0.08             # [v2] was 0.10
TP_ATR_FLOOR           = 0.06             # [v2] was 0.08
TP_ATR_CAP             = 0.20             # [v2] was 0.25
TS_TIGHTEN_TIER1       = 0.75
TS_TIGHTEN_TIER2       = 0.50

DIVERGENCE_LOOKBACK    = 60
DIVERGENCE_RSI_SEP     = 10
DIVERGENCE_MIN_DROP    = 3.0

TF_1H_HIST_MIN         = -0.5

DAILY_RSI_MIN          = 32               # [v2] was 35
DAILY_RSI_MAX          = 74               # [v2] was 72
TF_4H_RSI_MIN          = 35               # [v2] was 38
TF_4H_MACD_HIST_MIN    = -0.001

TP_TIER_1_SELL   = 0.50
TP_TIER_2_SELL   = 0.50

LAYER_IN_RSI_MAX = 58
LAYER_IN_RSI_MIN = 38
LAYER_IN_PULLBACK = 0.03                  # [v2] was 0.04

ORDER_EXPIRY_SECS      = _swing_cfg["timing"]["order_expiry_secs"]
SELL_LIMIT_SECS        = _swing_cfg["timing"]["sell_limit_secs"]
MIN_PROFIT_FOR_WIN     = 0.001

REENTRY_COOLDOWN_MINS  = _swing_cfg["timing"]["reentry_cooldown_mins"]

TS_AFTER_TIER_1 = TRAILING_STOP_PCT * TS_TIGHTEN_TIER1
TS_AFTER_TIER_2 = TRAILING_STOP_PCT * TS_TIGHTEN_TIER2

# =============================================================================
#  PATHS
# =============================================================================

_BASE_DIR  = os.environ.get(
    "SWINGTRADER_BASE_DIR",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_EVENT_DIR = os.path.join(_BASE_DIR, "data", "event_log")

_SECRETS_DIR      = os.environ.get("SWINGTRADER_SECRETS_DIR", "")
_KEYS_FILE        = (
    os.path.join(_SECRETS_DIR, "keys.json")
    if _SECRETS_DIR and os.path.exists(os.path.join(_SECRETS_DIR, "keys.json"))
    else os.path.join(_BASE_DIR, "config", "keys.json")
)
STATE_FILE        = os.path.join(_BASE_DIR, "data", "swing_state.json")
TRADE_HISTORY_CSV = os.path.join(_BASE_DIR, "data", "swing_trade_history.csv")
EVENTS_CSV        = os.path.join(_EVENT_DIR, "swing_events.csv")


def ensure_output_dirs():
    os.makedirs(_EVENT_DIR, exist_ok=True)


# =============================================================================
#  API  (with rate limiter)
# =============================================================================

BASE_URL = "https://api.kraken.com"


# ── [v2] Simple token-bucket rate limiter ────────────────────────────────────

class _RateLimiter:
    """
    Kraken public endpoints: ~1 call/sec sustained.
    Kraken private endpoints: decaying counter, ~15 calls then 1/sec.
    We enforce a simple delay between calls.
    """
    def __init__(self, min_interval=0.35):
        self._min_interval = min_interval
        self._last_call    = 0.0

    def wait(self):
        elapsed = time.time() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.time()


_rate_limiter = _RateLimiter(min_interval=0.35)


def kraken_public(endpoint, params=None):
    _rate_limiter.wait()
    url = BASE_URL + endpoint
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(url), timeout=15) as r:
        return json.loads(r.read().decode())


def kraken_private(endpoint, data=None):
    _rate_limiter.wait()
    if data is None:
        data = {}
    nonce         = str(int(time.time() * 1000))
    data["nonce"] = nonce
    post_data     = urllib.parse.urlencode(data)
    encoded       = (nonce + post_data).encode()
    message       = endpoint.encode() + hashlib.sha256(encoded).digest()
    secret        = base64.b64decode(API_SECRET)
    sig           = hmac.new(secret, message, hashlib.sha512)
    sig_b64       = base64.b64encode(sig.digest()).decode()
    headers = {
        "API-Key":      API_KEY,
        "API-Sign":     sig_b64,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    req = urllib.request.Request(
        BASE_URL + endpoint,
        data=post_data.encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def load_api_keys():
    global API_KEY, API_SECRET
    if not os.path.exists(_KEYS_FILE):
        print("  [ERROR] config/keys.json not found at {}".format(_KEYS_FILE))
        sys.exit(1)
    try:
        with open(_KEYS_FILE) as f:
            keys = json.load(f)
        API_KEY    = keys.get("API_KEY",    API_KEY)
        API_SECRET = keys.get("API_SECRET", API_SECRET)
        print("  [KEYS] Live keys loaded from config/keys.json")
    except Exception as e:
        print("  [ERROR] Failed to load config/keys.json: {}".format(e))
        sys.exit(1)


# =============================================================================
#  PAIR MAPS + PRICES
# =============================================================================

def norm_variants(s):
    s = s.upper().replace("/", "").replace("BTC", "XBT")
    variants = [s]
    if s.startswith("X") and len(s) > 5:
        variants.append(s[1:])
    if s.startswith("XX") and len(s) > 6:
        variants.append(s[2:])
    return variants


def build_pair_maps(friendly_pairs, pairs_config):
    result     = kraken_public("/0/public/AssetPairs")
    all_pairs  = result["result"]
    norm_index = {}
    for kraken_key, info in all_pairs.items():
        for s in [kraken_key, info.get("altname", ""), info.get("wsname", "")]:
            if s:
                for v in norm_variants(s):
                    norm_index.setdefault(v, kraken_key)

    request_map  = {}
    response_map = {}
    for friendly in friendly_pairs:
        matched_key = None
        override = pairs_config.get(friendly, {}).get("kraken_pair")
        if override:
            for v in norm_variants(override):
                if v in norm_index:
                    matched_key = norm_index[v]
                    break
        if not matched_key:
            for v in norm_variants(friendly):
                if v in norm_index:
                    matched_key = norm_index[v]
                    break
        if not matched_key:
            print("  [WARN] Could not resolve: {}".format(friendly))
            continue
        info    = all_pairs[matched_key]
        altname = info.get("altname", "")
        wsname  = info.get("wsname", "")
        request_map[friendly] = matched_key
        for s in [matched_key, altname, wsname, wsname.replace("/", "")]:
            if s:
                response_map[s] = friendly
                for v in norm_variants(s):
                    response_map.setdefault(v, friendly)
        print("  [PAIR] {} → {}  (ws={})".format(friendly, matched_key, wsname))
    return request_map, response_map


def fetch_ticker_prices(request_map, response_map):
    pair_str = ",".join(request_map.values())
    result   = kraken_public("/0/public/Ticker", {"pair": pair_str})
    prices   = {}
    for resp_key, val in result["result"].items():
        friendly = response_map.get(resp_key)
        if not friendly:
            for v in norm_variants(resp_key):
                friendly = response_map.get(v)
                if friendly:
                    break
        if friendly:
            prices[friendly] = float(val["c"][0])
    return prices


def fetch_usd_balance():
    try:
        result = kraken_private("/0/private/Balance")
        if result.get("error"):
            return None
        bal = float(result.get("result", {}).get("ZUSD", 0) or 0)
        if MAX_BUDGET_USD is not None:
            bal = min(bal, MAX_BUDGET_USD)
        return bal
    except Exception as e:
        print("  [WARN] Balance fetch failed: {}".format(e))
        return None


def fetch_spot_balances():
    try:
        result   = kraken_private("/0/private/Balance")
        raw      = result.get("result", {})
        balances = {}
        for key, val in raw.items():
            amt = float(val or 0)
            if amt <= 0:
                continue
            clean = key
            if len(key) == 4 and key[0] in ("X", "Z"):
                clean = key[1:]
            if clean == "XBT":
                clean = "BTC"
            balances[clean] = amt
        return balances
    except Exception:
        return {}


# ── [v2 FIX #5] Fixed USDC ordering bug ─────────────────────────────────────
def get_base_asset(pair):
    return pair.replace("USDC", "").replace("USD", "")


_request_map = {}
_SWING_DB = os.path.join(_BASE_DIR, "data", "swing_candles.db")


def _get_db():
    import sqlite3
    conn = sqlite3.connect(_SWING_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            pair      TEXT    NOT NULL,
            timeframe INTEGER NOT NULL,
            ts        INTEGER NOT NULL,
            open      REAL,
            high      REAL,
            low       REAL,
            close     REAL,
            volume    REAL,
            PRIMARY KEY (pair, timeframe, ts)
        )
    """)
    conn.commit()
    return conn


def _newest_ts(pair, tf_mins):
    try:
        conn = _get_db()
        row  = conn.execute(
            "SELECT MAX(ts) FROM candles WHERE pair=? AND timeframe=?",
            (pair, tf_mins)
        ).fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _save_candles(pair, tf_mins, candles):
    if not candles:
        return
    try:
        conn = _get_db()
        conn.executemany(
            "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?)",
            [(pair, tf_mins, c["ts"], c["open"], c["high"],
              c["low"], c["close"], c["volume"])
             for c in candles]
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print("  [WARN] DB save failed: {}".format(e))


def _load_candles(pair, tf_mins, limit=None):
    try:
        conn  = _get_db()
        query = ("SELECT ts,open,high,low,close,volume FROM candles "
                 "WHERE pair=? AND timeframe=? ORDER BY ts")
        rows  = conn.execute(query, (pair, tf_mins)).fetchall()
        conn.close()
        candles = [{"ts": r[0], "open": r[1], "high": r[2],
                    "low": r[3], "close": r[4], "volume": r[5]}
                   for r in rows]
        if limit:
            candles = candles[-limit:]
        return candles
    except Exception:
        return []


# =============================================================================
#  CANDLE FETCHING
# =============================================================================

def fetch_ohlc_from_kraken(kraken_key, tf_mins, since=None):
    try:
        params = {"pair": kraken_key, "interval": tf_mins}
        if since:
            params["since"] = since
        result = kraken_public("/0/public/OHLC", params)
        if result.get("error"):
            return []
        data_key = next((k for k in result["result"] if k != "last"), None)
        if not data_key:
            return []
        candles = result["result"][data_key][:-1]
        return [{
            "ts":     int(c[0]),
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": float(c[6]),
        } for c in candles]
    except Exception as e:
        print("  [WARN] OHLC fetch failed ({}/{}min): {}".format(
            kraken_key, tf_mins, e))
        return []


def fetch_ohlc(pair, kraken_key, tf_mins, limit=None):
    newest = _newest_ts(pair, tf_mins)
    if newest is None:
        candles = fetch_ohlc_from_kraken(kraken_key, tf_mins)
        if candles:
            _save_candles(pair, tf_mins, candles)
            print("  [CACHE] {}/{}min — fetched {} candles".format(
                pair, tf_mins, len(candles)))
        return _load_candles(pair, tf_mins, limit)
    else:
        since = newest - (tf_mins * 60)
        new_candles = fetch_ohlc_from_kraken(kraken_key, tf_mins, since=since)
        if new_candles:
            added = [c for c in new_candles if c["ts"] > newest]
            if added:
                _save_candles(pair, tf_mins, added)
        return _load_candles(pair, tf_mins, limit)


# =============================================================================
#  INDICATORS
# =============================================================================

def _ema_series(values, period):
    if len(values) < period:
        return []
    k      = 2 / (period + 1)
    seed   = sum(values[:period]) / period
    series = [seed]
    for v in values[period:]:
        series.append(v * k + series[-1] * (1 - k))
    return series


def calc_rsi(closes, period=14):
    if len(closes) < period + 2:
        return None
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    return 100 - (100 / (1 + avg_g / avg_l)) if avg_l != 0 else 100.0


def calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal + 1:
        return None, None, None
    fast_s    = _ema_series(closes, fast)
    slow_s    = _ema_series(closes, slow)
    offset    = slow - fast
    macd_line = [fast_s[i + offset] - slow_s[i] for i in range(len(slow_s))]
    sig_s     = _ema_series(macd_line, signal)
    if len(sig_s) < 2:
        return None, None, None
    return fast_s[-1] - slow_s[-1], sig_s[-1], (fast_s[-1] - slow_s[-1]) - sig_s[-1]


def calc_ma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def calc_heiken_ashi(candles):
    if not candles:
        return []
    ha = []
    for i, c in enumerate(candles):
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4
        if i == 0:
            ha_open = (c["open"] + c["close"]) / 2
        else:
            ha_open = (ha[i-1]["open"] + ha[i-1]["close"]) / 2
        ha_high = max(c["high"], ha_open, ha_close)
        ha_low  = min(c["low"],  ha_open, ha_close)
        ha.append({
            "ts":     c["ts"],
            "open":   ha_open,
            "high":   ha_high,
            "low":    ha_low,
            "close":  ha_close,
            "volume": c["volume"],
        })
    return ha


def classify_ha_trend(ha_candles, lookback=6):
    if len(ha_candles) < lookback:
        return "UNKNOWN", "insufficient data"
    recent = ha_candles[-lookback:]
    greens = sum(1 for c in recent if c["close"] >= c["open"])
    reds   = sum(1 for c in recent if c["close"] < c["open"])
    last3  = recent[-3:]
    lower_wicks = [min(c["open"], c["close"]) - c["low"] for c in last3]
    upper_wicks = [c["high"] - max(c["open"], c["close"]) for c in last3]
    avg_body    = sum(abs(c["close"] - c["open"]) for c in last3) / 3
    has_lower_wicks = any(w > avg_body * 0.3 for w in lower_wicks)
    has_upper_wicks = any(w > avg_body * 0.3 for w in upper_wicks)
    if greens >= 5:
        if has_lower_wicks:
            return "WEAKENING", "green run with lower wicks — momentum fading"
        return "BULLISH", "{} consecutive green candles".format(greens)
    elif reds >= 5:
        if has_upper_wicks:
            return "REVERSING", "red run with upper wicks — potential bottom"
        return "BEARISH", "{} consecutive red candles".format(reds)
    elif greens >= 4:
        return "WEAKENING", "mostly green but losing steam"
    elif reds >= 4:
        return "BEARISH", "mostly red — downtrend intact"
    else:
        return "INDECISIVE", "{} green / {} red — no clear direction".format(greens, reds)


def rsi_label(rsi):
    if rsi is None:      return "N/A"
    if rsi >= 70:        return "{:.1f} [OVERBOUGHT]".format(rsi)
    if rsi >= 55:        return "{:.1f} [BULLISH]".format(rsi)
    if rsi >= 45:        return "{:.1f} [NEUTRAL]".format(rsi)
    if rsi >= 35:        return "{:.1f} [OVERSOLD]".format(rsi)
    return                      "{:.1f} [BEAR]".format(rsi)


def macd_label(hist):
    if hist is None:     return "N/A"
    if hist > 0.001:     return "BULLISH  ({:+.4f})".format(hist)
    if hist > 0:         return "WEAK BULL({:+.4f})".format(hist)
    if hist > -0.001:    return "WEAK BEAR({:+.4f})".format(hist)
    return                      "BEARISH  ({:+.4f})".format(hist)


def calc_comparative_analysis(pair, kraken_key, now_utc):
    result = {"st_line": "", "lt_line": "", "outlook_line": ""}
    try:
        candles_4h = _load_candles(pair, 240, limit=100)
        ha_4h      = calc_heiken_ashi(candles_4h)
        closes_4h  = [c["close"] for c in candles_4h]
        rsi_4h     = calc_rsi(closes_4h)
        _, _, hist_4h = calc_macd(closes_4h)
        ha_trend_4h, ha_desc_4h = classify_ha_trend(ha_4h)

        candles_d  = _load_candles(pair, 1440, limit=200)
        ha_d       = calc_heiken_ashi(candles_d)
        closes_d   = [c["close"] for c in candles_d]
        rsi_d      = calc_rsi(closes_d)
        _, _, hist_d = calc_macd(closes_d)
        ha_trend_d, ha_desc_d = classify_ha_trend(ha_d)
        atr_pct    = calc_atr(candles_d)

        candles_w  = _load_candles(pair, 10080, limit=100)
        ha_w       = calc_heiken_ashi(candles_w)
        closes_w   = [c["close"] for c in candles_w]
        rsi_w      = calc_rsi(closes_w)
        _, _, hist_w = calc_macd(closes_w)
        ha_trend_w, ha_desc_w = classify_ha_trend(ha_w)

        obv_4h     = calc_obv(candles_4h)
        obv_rising = (len(obv_4h) >= 6 and obv_4h[-1] > obv_4h[-6])
        obv_str    = "OBV ↑" if obv_rising else "OBV ↓"
        atr_str    = "ATR {:.1f}%".format(atr_pct * 100) if atr_pct else ""

        result["st_line"] = (
            "  ST (4h) : RSI {}  MACD {}  HA {}  {}".format(
                rsi_label(rsi_4h), macd_label(hist_4h), ha_trend_4h, obv_str))
        result["lt_line"] = (
            "  LT (Wk) : RSI {}  MACD {}  HA {}  {}".format(
                rsi_label(rsi_w), macd_label(hist_w), ha_trend_w, atr_str))

        bull_signals = sum([
            rsi_4h is not None and rsi_4h > 45,
            hist_4h is not None and hist_4h > 0,
            ha_trend_4h in ("BULLISH", "WEAKENING", "REVERSING"),
            rsi_w is not None and rsi_w > 40,
            hist_w is not None and hist_w > 0,
            ha_trend_w in ("BULLISH", "WEAKENING", "REVERSING"),
            obv_rising,
        ])

        if bull_signals >= 5:
            outlook = "🟢 BULLISH — multiple timeframes aligned"
        elif bull_signals >= 4:
            outlook = "🟡 IMPROVING — short term turning, watch for entry"
        elif bull_signals == 3:
            if ha_trend_4h == "REVERSING" or ha_trend_w == "REVERSING":
                outlook = "🟡 POTENTIAL BOTTOM — HA reversal signal, monitor closely"
            else:
                outlook = "🟠 MIXED — conflicting signals, no clear edge"
        elif bull_signals == 2:
            if ha_trend_4h == "REVERSING":
                outlook = "🟠 DEAD CAT RISK — brief bounce in downtrend, HA not confirmed"
            else:
                outlook = "🔴 BEARISH — downtrend intact, wait for more signals"
        else:
            outlook = "🔴 STRONG BEAR — avoid longs, all signals negative"

        result["outlook_line"] = "  OUTLOOK  : {}".format(outlook)
    except Exception as e:
        result["st_line"]      = "  ST (4h)  : analysis error — {}".format(e)
        result["lt_line"]      = ""
        result["outlook_line"] = ""
    return result


def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        high       = candles[i]["high"]
        low        = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    current_price = candles[-1]["close"]
    return atr / current_price if current_price else None


def calc_obv(candles):
    if len(candles) < 2:
        return []
    obv = [0.0]
    for i in range(1, len(candles)):
        vol = candles[i]["volume"]
        if candles[i]["close"] > candles[i-1]["close"]:
            obv.append(obv[-1] + vol)
        elif candles[i]["close"] < candles[i-1]["close"]:
            obv.append(obv[-1] - vol)
        else:
            obv.append(obv[-1])
    return obv


def calc_vwap(candles, period=20):
    """
    Calculate Volume Weighted Average Price over the last N candles.
    Returns (vwap_price, price_vs_vwap_pct) or (None, None) if insufficient data.
    VWAP = sum(typical_price * volume) / sum(volume)
    """
    if len(candles) < period:
        return None, None
    recent = candles[-period:]
    total_vol = 0
    total_pv = 0
    for c in recent:
        typical_price = (c["high"] + c["low"] + c["close"]) / 3
        total_pv += typical_price * c["volume"]
        total_vol += c["volume"]
    if total_vol == 0:
        return None, None
    vwap = total_pv / total_vol
    current = candles[-1]["close"]
    pct_diff = (current - vwap) / vwap * 100
    return vwap, pct_diff


def detect_obv_divergence(candles_4h):
    if len(candles_4h) < DIVERGENCE_LOOKBACK:
        return False, "insufficient candles"
    candles = candles_4h[-DIVERGENCE_LOOKBACK:]
    prices  = [c["low"]  for c in candles]
    obv     = calc_obv(candles)
    if len(obv) < 20:
        return False, "insufficient OBV data"
    window = 3
    p_lows = []
    for i in range(window, len(prices) - window):
        if prices[i] <= min(prices[i-window:i] + prices[i+1:i+window+1]):
            p_lows.append((i, prices[i], obv[i]))
    if len(p_lows) < 2:
        return False, "insufficient price lows for OBV divergence"
    for j in range(len(p_lows)-1, 0, -1):
        i2, p2, o2 = p_lows[j]
        for k in range(j-1, -1, -1):
            i1, p1, o1 = p_lows[k]
            if i2 - i1 < DIVERGENCE_RSI_SEP:
                continue
            if p2 < p1 and o2 > o1:
                return (True,
                    "OBV divergence — price ${:.4f}→${:.4f} (lower) "
                    "OBV {:.0f}→{:.0f} (higher) — accumulation".format(p1, p2, o1, o2))
    return False, "no OBV divergence"


def check_volume_confirmation(candles_4h, rsi_div_candle_idx=None):
    if len(candles_4h) < 20:
        return False, "insufficient candles"
    recent  = candles_4h[-20:]
    vols    = [c["volume"] for c in recent]
    avg_vol = sum(vols) / len(vols)
    if avg_vol == 0:
        return False, "no volume data"
    last3_vol       = vols[-3:]
    last3_avg       = sum(last3_vol) / len(last3_vol)
    vol_ratio       = last3_avg / avg_vol
    selloff_vol     = sum(vols[5:15]) / 10 if len(vols) >= 15 else avg_vol
    bounce_expanding = last3_avg > selloff_vol * 0.8
    if vol_ratio >= 1.2 and bounce_expanding:
        return (True,
            "volume {:.1f}x avg on bounce — accumulation confirmed".format(vol_ratio))
    elif vol_ratio >= 0.8:
        return (True,
            "volume near avg ({:.1f}x) — neutral, not distribution".format(vol_ratio))
    else:
        return (False,
            "volume weak ({:.1f}x avg) on bounce — low conviction".format(vol_ratio))


def calc_atr_exits(candles_daily, entry_price):
    atr_pct = calc_atr(candles_daily, ATR_PERIOD)
    if not atr_pct:
        return {
            "atr_pct":      None,
            "tp_tier1_pct": 0.10,
            "tp_tier2_pct": 0.15,
            "tp_tier3_pct": 0.20,
            "ts_pct":       TRAILING_STOP_PCT,
        }
    def _clamp(val, floor, cap):
        return max(floor, min(cap, val))
    ts_pct  = _clamp(atr_pct * TS_ATR_MULTIPLIER,  TS_ATR_FLOOR, TS_ATR_CAP)
    tp1_pct = _clamp(atr_pct * TP_ATR_MULTIPLIER,  TP_ATR_FLOOR, TP_ATR_CAP)
    tp2_pct = _clamp(atr_pct * TP_ATR_TIER2_MULT,  TP_ATR_FLOOR, TP_ATR_CAP)
    tp3_pct = _clamp(atr_pct * TP_ATR_TIER3_MULT,  TP_ATR_FLOOR, TP_ATR_CAP)
    return {
        "atr_pct":      atr_pct,
        "tp_tier1_pct": tp1_pct,
        "tp_tier2_pct": tp2_pct,
        "tp_tier3_pct": tp3_pct,
        "ts_pct":       ts_pct,
    }


def calc_bear_timer(pair, kraken_key, now_utc, current_price=None):
    result = {
        "days_since_local_high":   None,
        "days_since_macd_bullish": None,
        "days_since_weekly_rsi50": None,
        "summary": "",
    }

    try:
        candles_4h = _load_candles(pair, 240)
        local_high_price = None
        if len(candles_4h) >= 30:
            highs = [c["high"] for c in candles_4h]
            local_high_idx = None
            for i in range(len(highs) - 2, max(len(highs) - 200, 0), -1):
                if highs[i] >= max(highs[i+1:], default=0):
                    local_high_idx   = i
                    local_high_price = highs[i]
                    break

            if local_high_idx is not None:
                if current_price and current_price >= local_high_price:
                    result["days_since_local_high"] = 0
                else:
                    high_ts  = candles_4h[local_high_idx]["ts"]
                    high_dt  = datetime.fromtimestamp(high_ts, tz=timezone.utc)
                    result["days_since_local_high"] = (
                        now_utc - high_dt).total_seconds() / 86400

            closes_4h    = [c["close"] for c in candles_4h]
            last_bull_idx = None
            for i in range(len(closes_4h) - 1, max(len(closes_4h) - 200, 34), -1):
                _, _, hist = calc_macd(closes_4h[:i+1])
                if hist is not None and hist > 0:
                    last_bull_idx = i
                    break
            if last_bull_idx is not None:
                bull_ts  = candles_4h[last_bull_idx]["ts"]
                bull_dt  = datetime.fromtimestamp(bull_ts, tz=timezone.utc)
                result["days_since_macd_bullish"] = (
                    now_utc - bull_dt).total_seconds() / 86400

        candles_weekly = _load_candles(pair, 10080)
        if len(candles_weekly) >= 16:
            closes_w = [c["close"] for c in candles_weekly]
            last_rsi50_idx = None
            for i in range(len(closes_w) - 1, max(len(closes_w) - 52, 14), -1):
                rsi = calc_rsi(closes_w[:i+1])
                if rsi is not None and rsi >= 50:
                    last_rsi50_idx = i
                    break
            if last_rsi50_idx is not None:
                rsi50_ts  = candles_weekly[last_rsi50_idx]["ts"]
                rsi50_dt  = datetime.fromtimestamp(rsi50_ts, tz=timezone.utc)
                result["days_since_weekly_rsi50"] = (
                    now_utc - rsi50_dt).total_seconds() / 86400

        parts = []
        dlh = result["days_since_local_high"]
        if dlh is not None:
            if dlh == 0:
                parts.append("at local high")
            elif dlh < 1:
                parts.append("local high {}h ago @ ${:.4f}".format(
                    int(dlh * 24), local_high_price or 0))
            else:
                parts.append("local high {:.0f}d ago @ ${:.4f}".format(
                    dlh, local_high_price or 0))

        if result["days_since_macd_bullish"] is not None:
            parts.append("4h MACD bearish {:.1f}d".format(
                result["days_since_macd_bullish"]))
        if result["days_since_weekly_rsi50"] is not None:
            parts.append("weekly RSI<50 for {:.0f}d".format(
                result["days_since_weekly_rsi50"]))
        result["summary"] = "  |  ".join(parts) if parts else "bear timer unavailable"

    except Exception as e:
        result["summary"] = "bear timer error: {}".format(e)

    return result


def find_swing_lows(candles, lookback=None, min_bounce=0.005):
    if lookback:
        candles = candles[-lookback:]
    lows   = []
    window = 3
    for i in range(window, len(candles) - window):
        c   = candles[i]
        low = c["low"]
        if not all(low <= candles[j]["low"] for j in range(i-window, i+window+1) if j != i):
            continue
        subsequent_high = max(candles[j]["high"] for j in range(i, min(i+10, len(candles))))
        if (subsequent_high - low) / low < min_bounce:
            continue
        lows.append((low, c["ts"]))
    result = []
    for price, ts in sorted(lows, key=lambda x: -x[1]):
        if not any(abs(price - p) / p < 0.02 for p, _ in result):
            result.append((price, ts))
        if len(result) >= 5:
            break
    return result


# =============================================================================
#  SWING SIGNAL ENGINE
# =============================================================================

def detect_rsi_divergence(candles_4h):
    if len(candles_4h) < DIVERGENCE_LOOKBACK:
        return False, None, "insufficient candles for divergence"
    candles = candles_4h[-DIVERGENCE_LOOKBACK:]
    closes  = [c["close"] for c in candles]
    lows    = [c["low"]   for c in candles]
    rsi_series = []
    for i in range(14, len(closes) + 1):
        r = calc_rsi(closes[:i])
        rsi_series.append(r if r is not None else 50)
    rsi_series = [None] * 14 + rsi_series
    rsi_lows = []
    window   = 3
    for i in range(window, len(rsi_series) - window):
        if rsi_series[i] is None:
            continue
        neighbors = [rsi_series[j] for j in range(i-window, i+window+1)
                     if j != i and rsi_series[j] is not None]
        if neighbors and rsi_series[i] <= min(neighbors):
            nearby_vals = [rsi_series[j] for j in range(max(0, i-10), i)
                           if rsi_series[j] is not None]
            nearby_high = max(nearby_vals) if nearby_vals else 0
            if nearby_high - rsi_series[i] >= DIVERGENCE_MIN_DROP:
                rsi_lows.append((i, rsi_series[i], lows[i], closes[i]))
    if len(rsi_lows) < 2:
        return False, None, "only {}/{} RSI lows found".format(len(rsi_lows), 2)
    for j in range(len(rsi_lows) - 1, 0, -1):
        i2_idx, rsi2, price_low2, _ = rsi_lows[j]
        for k in range(j - 1, -1, -1):
            i1_idx, rsi1, price_low1, _ = rsi_lows[k]
            if i2_idx - i1_idx < DIVERGENCE_RSI_SEP:
                continue
            price_diverges = price_low2 < price_low1
            rsi_diverges   = rsi2 > rsi1
            if price_diverges and rsi_diverges:
                rsi_diff   = rsi2 - rsi1
                price_diff = (price_low1 - price_low2) / price_low1 * 100
                strength   = "strong" if rsi_diff > 5 else "moderate"
                desc = ("{} bullish divergence — "
                        "price low1=${:.4f} → low2=${:.4f} ({:.1f}% lower)  "
                        "RSI low1={:.1f} → low2={:.1f} (+{:.1f} divergence)  "
                        "{} candles apart").format(
                    strength, price_low1, price_low2, price_diff,
                    rsi1, rsi2, rsi_diff, i2_idx - i1_idx)
                return True, strength, desc
    return False, None, "no RSI divergence detected in last {} 4h candles".format(
        DIVERGENCE_LOOKBACK)


def _entry_reason_code(signal_data, raw_reason):
    """
    Derive a short standardised code and human sentence from signal_data.
    Codes are intentionally general so they aggregate cleanly across sessions.

    Returns (code_str, human_str).
    """
    if not signal_data:
        return "MANUAL", raw_reason[:80] if raw_reason else "Manual entry"

    trend   = signal_data.get("trend_mode", False)
    div     = signal_data.get("divergence", False)
    div_str = signal_data.get("divergence_strength") or ""
    obv     = signal_data.get("obv_divergence", False)
    rsi_4h  = signal_data.get("rsi_4h")
    rsi_d   = signal_data.get("rsi_daily")
    hist    = signal_data.get("hist_4h")
    sup_lbl = signal_data.get("support_label", "")
    pulled  = signal_data.get("pulled_back", False)

    if trend:
        if pulled:
            code  = "TREND_PULLBACK"
            human = "Bull trend active — RSI pulled back, resuming entry"
        else:
            code  = "TREND_1H_MOMENTUM"
            human = "Bull trend active — 1h MACD just turned positive"
    elif div and div_str == "strong" and obv:
        code  = "STRONG_DIV_OBV"
        human = "Strong RSI divergence + OBV accumulation near support"
    elif div and div_str == "strong":
        code  = "STRONG_RSI_DIV"
        human = "Strong bullish RSI divergence at support"
    elif div and obv:
        code  = "DIV_OBV"
        human = "Moderate RSI divergence + OBV divergence near support"
    elif div:
        code  = "RSI_DIV"
        human = "Bullish RSI divergence at support"
    elif obv:
        code  = "OBV_SUPPORT"
        human = "OBV accumulation near support level"
    elif sup_lbl == "50MA":
        code  = "MA50_BOUNCE"
        human = "Bounce off 50MA with oversold conditions"
    elif sup_lbl == "swing_low":
        code  = "SWING_LOW_BOUNCE"
        human = "Price bouncing from swing low support"
    else:
        code  = "SUPPORT_SIGNAL"
        human = "Entry near support with volume/timing confirmation"

    # Append RSI context if available
    if rsi_4h is not None and rsi_d is not None:
        if rsi_4h < 40:
            human += " — short-term RSI oversold"
        elif rsi_4h < 50:
            human += " — short-term RSI neutral-low"
        if rsi_d < 40:
            human += ", daily RSI oversold"

    return code, human


def _pair_thresholds(cfg):
    """
    Return entry thresholds for a pair, applying any per-pair overrides
    from the 'entry_overrides' key in pairs.json. Falls back to globals.

    Overridable keys (all optional):
        4h_rsi_min        — minimum 4h RSI to allow entry
        daily_rsi_min     — minimum daily RSI
        daily_rsi_max     — maximum daily RSI
        4h_macd_hist_min  — minimum 4h MACD histogram
    """
    ov = cfg.get("entry_overrides", {})
    return {
        "daily_rsi_min":    ov.get("daily_rsi_min",    DAILY_RSI_MIN),
        "daily_rsi_max":    ov.get("daily_rsi_max",    DAILY_RSI_MAX),
        "4h_rsi_min":       ov.get("4h_rsi_min",       TF_4H_RSI_MIN),
        "4h_macd_hist_min": ov.get("4h_macd_hist_min", TF_4H_MACD_HIST_MIN),
    }


def evaluate_entry(pair, pairs_config, current_price):
    cfg        = pairs_config.get(pair, {})
    kraken_key = _request_map.get(pair) or cfg.get("kraken_pair", pair)

    # Per-pair thresholds (falls back to globals if no overrides defined)
    thr = _pair_thresholds(cfg)
    daily_rsi_min    = thr["daily_rsi_min"]
    daily_rsi_max    = thr["daily_rsi_max"]
    tf_4h_rsi_min    = thr["4h_rsi_min"]
    tf_4h_macd_min   = thr["4h_macd_hist_min"]

    candles_4h     = fetch_ohlc(pair, kraken_key, 240)
    candles_daily  = fetch_ohlc(pair, kraken_key, 1440)
    candles_weekly = fetch_ohlc(pair, kraken_key, 10080, limit=SUPPORT_LOOKBACK_WEEKLY)
    candles_1h     = fetch_ohlc(pair, kraken_key, 60, limit=48)
    if len(candles_4h) < 35 or len(candles_daily) < 14:
        return False, current_price, "insufficient candle data ({} 4h, {} daily)".format(
            len(candles_4h), len(candles_daily)), {}
    closes_4h    = [c["close"] for c in candles_4h]
    closes_daily = [c["close"] for c in candles_daily]
    rsi_4h         = calc_rsi(closes_4h)
    rsi_daily      = calc_rsi(closes_daily)
    macd_4h, sig_4h, hist_4h = calc_macd(closes_4h)
    ma50_4h        = calc_ma(closes_4h, MA_PERIOD)
    closes_weekly  = [c["close"] for c in candles_weekly] if candles_weekly else []
    rsi_weekly     = calc_rsi(closes_weekly) if len(closes_weekly) >= 16 else None
    _, _, hist_weekly = calc_macd(closes_weekly) if len(closes_weekly) >= 35 else (None, None, None)
    if any(v is None for v in (rsi_4h, rsi_daily, hist_4h, ma50_4h)):
        return False, current_price, "indicators unavailable", {}
    if rsi_weekly is not None and rsi_weekly < 30:
        return False, current_price, \
            "weekly RSI {:.1f} < 30 — macro crash, no longs".format(rsi_weekly), {}
    # VWAP — price vs volume-weighted average on 4h candles
    vwap_4h, vwap_pct = calc_vwap(candles_4h, period=20)
    below_vwap = vwap_4h is not None and vwap_pct < -0.5  # at least 0.5% below VWAP

    signal_data = {
        "vwap": vwap_4h, "vwap_pct": vwap_pct,
        "rsi_4h": rsi_4h, "rsi_daily": rsi_daily, "rsi_weekly": rsi_weekly,
        "hist_4h": hist_4h, "hist_weekly": hist_weekly, "ma50_4h": ma50_4h,
    }
    if not (daily_rsi_min <= rsi_daily <= daily_rsi_max):
        return False, current_price, "Daily RSI {:.1f} outside {}-{} range".format(
            rsi_daily, daily_rsi_min, daily_rsi_max), signal_data
    if rsi_4h < tf_4h_rsi_min:
        return False, current_price, "4h RSI {:.1f} below minimum {}".format(
            rsi_4h, tf_4h_rsi_min), signal_data
    if hist_4h < tf_4h_macd_min:
        return False, current_price, "4h MACD hist {:.4f} too negative (selling pressure)".format(
            hist_4h), signal_data
    # ── TREND MODE — bull continuation dip entry ─────────────────────────────
    # Fires when the market is in an established uptrend. Skips support proximity
    # and divergence requirements (those are bottom-picking signals). Instead
    # looks for a mild RSI pullback within a healthy trend.
    in_bull_trend = (
        rsi_4h     >= 50 and
        rsi_daily  >= 48 and
        hist_4h    >  0  and
        current_price >= ma50_4h * 0.99   # at or above 50MA
    )
    if in_bull_trend:
        # Need to see RSI actually pulled back recently (not just sitting at 50)
        rsi_4h_lag = calc_rsi(closes_4h[:-2]) if len(closes_4h) > 16 else None
        pulled_back = rsi_4h_lag is not None and rsi_4h_lag > rsi_4h + 4

        # Also accept entry if 1h MACD just turned positive (momentum resuming)
        closes_1h_tm = [c["close"] for c in fetch_ohlc(pair, kraken_key, 60, limit=48)]
        hist_1h_tm   = None
        hist_1h_prev_tm = None
        if len(closes_1h_tm) >= 35:
            _, _, hist_1h_tm      = calc_macd(closes_1h_tm)
            _, _, hist_1h_prev_tm = calc_macd(closes_1h_tm[:-1])
        momentum_resuming = (
            hist_1h_tm is not None and
            hist_1h_prev_tm is not None and
            hist_1h_tm > 0 and
            hist_1h_prev_tm <= 0
        )

        weekly_str_tm = "  Weekly RSI {:.1f}".format(rsi_weekly) if rsi_weekly else ""
        atr_exits_tm  = calc_atr_exits(candles_daily, current_price)
        atr_pct_tm    = atr_exits_tm.get("atr_pct")
        atr_str_tm    = "  ATR {:.1f}%  TS={:.1f}%  TP={:.0f}%/{:.0f}%/{:.0f}%".format(
            (atr_pct_tm or 0) * 100, atr_exits_tm["ts_pct"] * 100,
            atr_exits_tm["tp_tier1_pct"] * 100, atr_exits_tm["tp_tier2_pct"] * 100,
            atr_exits_tm["tp_tier3_pct"] * 100)
        signal_data["atr_exits"] = atr_exits_tm

        if pulled_back or momentum_resuming:
            trigger_str = "RSI pullback {:.1f}→{:.1f} ({:.1f}pts)".format(
                rsi_4h_lag or rsi_4h, rsi_4h,
                (rsi_4h_lag - rsi_4h) if rsi_4h_lag else 0
            ) if pulled_back else "1h MACD just turned positive"
            reason = (
                "🟢 TREND MODE: bull continuation — {}  "
                "4h RSI {:.1f}  Daily RSI {:.1f}  MACD {:+.4f}  "
                "price {:.1f}% above 50MA{}{}".format(
                    trigger_str,
                    rsi_4h, rsi_daily, hist_4h,
                    (current_price - ma50_4h) / ma50_4h * 100,
                    weekly_str_tm, atr_str_tm))
            signal_data["trend_mode"]     = True
            signal_data["pulled_back"]    = pulled_back
            signal_data["rsi_4h_lag"]     = rsi_4h_lag
            entry_price_tm = current_price * (1 - 0.001)  # trend mode: 0.1% below market (was 0.5% — too far in trending conditions)
            return True, entry_price_tm, reason, signal_data
        else:
            # In trend but no pullback yet — tell the user what we're waiting for
            lag_str = "RSI {:.1f} (no pullback yet, need 4pt dip)".format(rsi_4h) \
                if rsi_4h_lag is None or rsi_4h_lag <= rsi_4h + 4 else ""
            return False, current_price, (
                "TREND MODE: bull trend active but waiting for entry — "
                "{}  4h RSI {:.1f}  Daily RSI {:.1f}  MACD {:+.4f}  "
                "{:.1f}% above 50MA{}".format(
                    lag_str, rsi_4h, rsi_daily, hist_4h,
                    (current_price - ma50_4h) / ma50_4h * 100,
                    weekly_str_tm)), signal_data
    # ── END TREND MODE ────────────────────────────────────────────────────────

    swing_lows     = find_swing_lows(candles_4h, min_bounce=MIN_BOUNCE_PCT)
    support_levels = [(price, "swing_low") for price, _ in swing_lows]
    if ma50_4h:
        support_levels.append((ma50_4h, "50MA"))
    nearest_support = None
    nearest_label   = None
    nearest_dist    = float("inf")
    for level_price, label in support_levels:
        dist = (current_price - level_price) / level_price
        if -0.005 <= dist <= PULLBACK_TOLERANCE:
            if dist < nearest_dist:
                nearest_dist    = dist
                nearest_support = level_price
                nearest_label   = label
    if nearest_support is None:
        if support_levels:
            closest  = min(support_levels, key=lambda x: abs(current_price - x[0]) / x[0])
            dist_pct = (current_price - closest[0]) / closest[0] * 100
            reason   = "no support within {:.0f}% — nearest {} ({}) {:.1f}% away".format(
                PULLBACK_TOLERANCE * 100, closest[1],
                "${:,.4f}".format(closest[0]), dist_pct)
        else:
            reason = "no swing lows detected"
        return False, current_price, reason, signal_data
    last_candle = candles_4h[-1]
    bouncing    = last_candle["close"] > last_candle["open"]
    if not bouncing:
        return False, current_price, (
            "near {} ${:,.4f} but last 4h candle bearish — "
            "waiting for bounce confirmation".format(nearest_label, nearest_support)), signal_data
    div_found, div_strength, div_desc = detect_rsi_divergence(candles_4h)
    obv_found,  obv_desc              = detect_obv_divergence(candles_4h)
    vol_ok,     vol_desc              = check_volume_confirmation(candles_4h)
    signal_data["divergence"]          = div_found
    signal_data["divergence_strength"] = div_strength
    signal_data["obv_divergence"]      = obv_found
    signal_data["volume_ok"]           = vol_ok
    atr_exits = calc_atr_exits(candles_daily, current_price)
    signal_data["atr_exits"] = atr_exits
    atr_pct   = atr_exits.get("atr_pct")
    atr_str   = "  ATR {:.1f}%  TS={:.1f}%  TP={:.0f}%/{:.0f}%/{:.0f}%".format(
        (atr_pct or 0) * 100, atr_exits["ts_pct"] * 100,
        atr_exits["tp_tier1_pct"] * 100, atr_exits["tp_tier2_pct"] * 100,
        atr_exits["tp_tier3_pct"] * 100)
    closes_1h = [c["close"] for c in candles_1h] if candles_1h else []
    hist_1h   = None
    rsi_1h    = None
    if len(closes_1h) >= 35:
        _, _, hist_1h = calc_macd(closes_1h)
        rsi_1h        = calc_rsi(closes_1h)
    signal_data["hist_1h"] = hist_1h
    signal_data["rsi_1h"]  = rsi_1h
    timing_improving = (hist_1h is not None and hist_1h > TF_1H_HIST_MIN)
    timing_str = ""
    if hist_1h is not None:
        if hist_1h > 0:
            timing_str = "  1h hist {:+.4f} ↑ timing ✓".format(hist_1h)
        elif hist_1h > TF_1H_HIST_MIN:
            timing_str = "  1h hist {:+.4f} improving".format(hist_1h)
        else:
            timing_str = "  1h hist {:+.4f} still weak".format(hist_1h)
    tier_a_score = sum([
        2 if (div_found and div_strength == "strong")   else 0,
        1 if (div_found and div_strength == "moderate") else 0,
        2 if obv_found else 0,
    ])
    tier_b_score = sum([
        1 if vol_ok           else 0,
        1 if timing_improving else 0,
        1 if below_vwap       else 0,
    ])
    div_score  = tier_a_score + tier_b_score
    has_tier_a = tier_a_score >= 1
    signal_data["div_score"]    = div_score
    signal_data["tier_a_score"] = tier_a_score
    signal_data["tier_b_score"] = tier_b_score
    if div_score < 3 or not has_tier_a:
        signals_str = (
            "RSI div={} ({}pt)  OBV div={} ({}pt)  "
            "vol={} ({}pt)  1h={} ({}pt)  vwap={} ({}pt)  "
            "TierA={}/max5  TierB={}/max3  total={}/min3".format(
                div_strength or "none",
                2 if (div_found and div_strength == "strong") else
                1 if (div_found and div_strength == "moderate") else 0,
                "yes" if obv_found else "no", 2 if obv_found else 0,
                "yes" if vol_ok else "no",    1 if vol_ok else 0,
                "yes" if timing_improving else "no", 1 if timing_improving else 0,
                "yes ({:+.1f}%)".format(vwap_pct) if below_vwap else "no", 1 if below_vwap else 0,
                tier_a_score, tier_b_score, div_score))
        if not has_tier_a:
            reason = "near support but no 4h divergence signal — {}".format(signals_str)
        else:
            reason = "near support but {}/3pts minimum ({}) — waiting for stronger signal".format(
                div_score, signals_str)
        return False, current_price, reason, signal_data
    entry_price  = current_price * (1 - cfg.get("trigger_pct", 0.005))
    vwap_str   = "  VWAP ${:,.2f} ({:+.1f}%)".format(vwap_4h, vwap_pct) if vwap_4h else ""
    weekly_str = "  Weekly RSI {:.1f}".format(rsi_weekly) if rsi_weekly else ""
    div_str    = "  📊 {} RSI div".format(div_strength) if div_found else ""
    obv_str    = " + OBV div" if obv_found else ""
    vol_str    = "  vol {:.1f}x avg".format(
        sum(c["volume"] for c in candles_4h[-3:]) / 3 /
        (sum(c["volume"] for c in candles_4h[-20:]) / 20)
        if len(candles_4h) >= 20 else 1) if vol_ok else ""
    reason = ("{} support ${:,.4f} ({:.1f}% away)  "
              "4h RSI {:.1f}  Daily RSI {:.1f}  hist {:+.4f}"
              "{}{}{}{}{}{}  [A:{}/5 B:{}/2 = {}/3✓]".format(
                  nearest_label, nearest_support, nearest_dist * 100,
                  rsi_4h, rsi_daily, hist_4h,
                  weekly_str, div_str, obv_str, vol_str, timing_str, atr_str,
                  tier_a_score, tier_b_score, div_score))
    signal_data["support_level"] = nearest_support
    signal_data["support_label"] = nearest_label
    signal_data["support_dist"]  = nearest_dist
    return True, entry_price, reason, signal_data


def _momentum_strength(pair, pairs_config):
    cfg        = pairs_config.get(pair, {})
    kraken_key = _request_map.get(pair) or cfg.get("kraken_pair", pair)
    candles    = fetch_ohlc(pair, kraken_key, 240, limit=60)
    if len(candles) < 35:
        return "unknown", None
    closes = [c["close"] for c in candles]
    rsi_4h = calc_rsi(closes)
    macd, sig, hist = calc_macd(closes)
    # [v2 FIX #6] Guard None before comparison
    if rsi_4h is None or hist is None:
        return "unknown", rsi_4h
    closes_prev = closes[:-1]
    _, _, hist_prev = calc_macd(closes_prev) if len(closes_prev) >= 35 else (None, None, None)
    if rsi_4h >= 72:
        return "exhausted", rsi_4h
    if hist < 0:
        return "negative", rsi_4h
    if hist > 0 and hist_prev and hist > hist_prev:
        return "strong", rsi_4h
    if hist > 0:
        return "weakening", rsi_4h
    return "neutral", rsi_4h


def evaluate_exit(pair, ps, price, now_utc, pairs_config):
    fp       = ps.get("fill_price") or 0
    hwm      = ps.get("high_water_mark") or fp
    tp_tier  = ps.get("tp_tier", 0)
    ts_armed = ps.get("trailing_stop_armed", False)
    base_ts  = ps.get("atr_ts_pct")  or TRAILING_STOP_PCT
    base_tp1 = ps.get("atr_tp1_pct") or 0.10
    base_tp2 = ps.get("atr_tp2_pct") or 0.15
    base_tp3 = ps.get("atr_tp3_pct") or 0.20
    if tp_tier >= 2:
        ts_pct = base_ts * TS_TIGHTEN_TIER2
    elif tp_tier >= 1:
        ts_pct = base_ts * TS_TIGHTEN_TIER1
    else:
        ts_pct = base_ts
    ts_pct = ps.get("trailing_stop_pct") or ts_pct
    if not fp:
        return "hold", 0, "", ""
    pnl_pct  = (price - fp) / fp
    hwm_drop = (hwm - price) / hwm if hwm else 0
    if pnl_pct >= TRAILING_STOP_ARM_PCT and not ts_armed:
        ps["trailing_stop_armed"] = True
        ts_armed = True
        print("  ... [{}] Trailing stop ARMED @ ${:.4f}  (+{:.1f}%)".format(
            pair, price, pnl_pct * 100))
    strength, rsi_4h = _momentum_strength(pair, pairs_config)
    is_trend_entry = ps.get("entry_code", "").startswith("TREND_")
    # RSI exhaustion threshold is higher for TREND_MODE entries — trend trades
    # naturally run at elevated RSI and a low threshold causes premature exits.
    exhaustion_threshold = (TREND_MODE_RSI_EXHAUSTION_THRESHOLD
                            if is_trend_entry else RSI_EXHAUSTION_THRESHOLD)
    if strength == "exhausted" and pnl_pct > 0 and (rsi_4h or 0) >= exhaustion_threshold:
        return ("full_exit", 1.0,
                "4h RSI {:.1f} exhaustion (threshold {})  ({:+.2f}%)".format(
                    rsi_4h or 0, exhaustion_threshold, pnl_pct * 100),
                "RSI_EXHAUSTION")
    # ── MOMENTUM EXIT ── exit losing positions when trend has turned bearish
    if pnl_pct < -0.05:
        fill_ts = ps.get("fill_timestamp")
        if fill_ts:
            fill_dt = datetime.fromisoformat(fill_ts)
            days_held = (now_utc - fill_dt).total_seconds() / 86400
        else:
            days_held = 0
        if days_held >= 2:
            cfg = pairs_config.get(pair, {})
            kraken_key = _request_map.get(pair) or cfg.get("kraken_pair", pair)
            try:
                candles_4h = _load_candles(pair, 240, limit=60)
                candles_daily = _load_candles(pair, 1440, limit=60)
                closes_4h = [c["close"] for c in candles_4h]
                closes_daily = [c["close"] for c in candles_daily]
                rsi_daily = calc_rsi(closes_daily) if len(closes_daily) >= 16 else 50
                _, _, hist_4h = calc_macd(closes_4h) if len(closes_4h) >= 35 else (None, None, None)
                ma50_4h = calc_ma(closes_4h, MA_PERIOD) if len(closes_4h) >= MA_PERIOD else None
                bearish_macd = hist_4h is not None and hist_4h < 0
                bearish_rsi = rsi_daily is not None and rsi_daily < 45
                below_ma = ma50_4h is not None and price < ma50_4h
                vwap_exit, vwap_exit_pct = calc_vwap(candles_4h, period=20)
                above_vwap = vwap_exit is not None and vwap_exit_pct > 2.0
                bearish_count = sum([bearish_macd, bearish_rsi, below_ma])
                # If price is well above VWAP despite bearish signals, reduce urgency
                if above_vwap and bearish_count >= 2:
                    bearish_count -= 1
                if bearish_count >= 2:
                    reasons = []
                    if bearish_macd:
                        reasons.append("4h MACD negative ({:+.4f})".format(hist_4h))
                    if bearish_rsi:
                        reasons.append("daily RSI {:.1f}".format(rsi_daily))
                    if below_ma:
                        reasons.append("below 50MA (${:,.2f})".format(ma50_4h))
                    return ("full_exit", 1.0,
                            "MOMENTUM EXIT: {:+.2f}% after {:.1f}d | {}".format(
                                pnl_pct * 100, days_held, " + ".join(reasons)),
                            "MOMENTUM_EXIT")
            except Exception as e:
                print("  [{}] momentum exit check failed: {}".format(pair, e))

    # ── HARD MAX HOLD ── exit any position not yet profitable after MAX_HOLD_DAYS
    fill_ts_mh = ps.get("fill_timestamp")
    if fill_ts_mh:
        days_held_mh = (now_utc - datetime.fromisoformat(fill_ts_mh)).total_seconds() / 86400
        if days_held_mh >= MAX_HOLD_DAYS and not ps.get("trailing_stop_armed", False):
            return ("full_exit", 1.0,
                    "MAX HOLD {:.0f}d reached, trailing stop never armed — exiting ({:+.2f}%)".format(
                        days_held_mh, pnl_pct * 100),
                    "MAX_HOLD_EXIT")

    if ts_armed and hwm_drop >= ts_pct:
        return ("full_exit", 1.0,
                "Trailing stop {:.0f}% from HWM ${:,.4f}  ({:+.2f}%)".format(
                    ts_pct * 100, hwm, pnl_pct * 100),
                "TRAILING_STOP")
    support = ps.get("entry_support_level")
    if support and price < support * 0.97:
        return ("full_exit", 1.0,
                "Stop loss: below support ${:,.4f} - 3%  ({:+.2f}%)".format(
                    support, pnl_pct * 100),
                "STOP_LOSS")
    if tp_tier < 1 and pnl_pct >= base_tp1:
        if strength in ("strong", "weakening", "neutral"):
            return ("partial_exit", TP_TIER_1_SELL,
                    "Tier 1 TP {:.0f}% (ATR-based)  momentum={}  ({:+.2f}%)  "
                    "holding remaining 50%  next TP @ {:.0f}%".format(
                        base_tp1 * 100, strength, pnl_pct * 100, base_tp2 * 100),
                    "TAKE_PROFIT_TIER1")
        else:
            return ("full_exit", 1.0,
                    "TP {:.0f}% + momentum {}  full exit".format(base_tp1 * 100, strength),
                    "TAKE_PROFIT")
    if tp_tier < 2 and pnl_pct >= base_tp2:
        if strength in ("strong", "weakening"):
            return ("partial_exit", TP_TIER_2_SELL,
                    "Tier 2 TP {:.0f}% (ATR-based)  momentum={}  ({:+.2f}%)  "
                    "holding 25%  next TP @ {:.0f}%".format(
                        base_tp2 * 100, strength, pnl_pct * 100, base_tp3 * 100),
                    "TAKE_PROFIT_TIER2")
        else:
            return ("full_exit", 1.0,
                    "TP {:.0f}% + momentum {}  full exit".format(base_tp2 * 100, strength),
                    "TAKE_PROFIT")
    if tp_tier < 3 and pnl_pct >= base_tp3:
        return ("full_exit", 1.0,
                "Tier 3 TP {:.0f}%  ({:+.2f}%)".format(base_tp3 * 100, pnl_pct * 100),
                "TAKE_PROFIT_TIER3")
    sw_start = ps.get("sideways_clock_start")
    in_band  = abs(price - fp) / fp <= SWING_BAND_PCT if fp else False
    if in_band:
        if not sw_start:
            ps["sideways_clock_start"] = now_utc.isoformat()
    else:
        ps["sideways_clock_start"] = None
    if sw_start:
        elapsed_days = (now_utc - datetime.fromisoformat(sw_start)).total_seconds() / 86400
        if elapsed_days >= SIDEWAYS_DAYS:
            return ("full_exit", 1.0,
                    "Sideways {:.1f} days  ({:+.2f}%)".format(elapsed_days, pnl_pct * 100),
                    "SIDEWAYS_EXIT")
    return "hold", 0, "", ""


def evaluate_layer_in(pair, ps, price, pairs_config):
    tp_tier    = ps.get("tp_tier", 0)
    orig_vol   = ps.get("original_volume") or 0
    total_vol  = ps.get("total_volume") or orig_vol
    orig_alloc = ps.get("original_alloc") or ps.get("usd_allocated") or 0
    hwm        = ps.get("high_water_mark") or 0
    if tp_tier < 1:
        return False, 0, 0, "no partial exit yet — nothing to refill"
    sold_vol = orig_vol - total_vol
    if sold_vol <= 0.000001:
        return False, 0, 0, "position already at full size"
    if hwm > 0:
        pullback = (hwm - price) / hwm
        if pullback < LAYER_IN_PULLBACK:
            return False, 0, 0, "pullback {:.1f}% < required {:.0f}%".format(
                pullback * 100, LAYER_IN_PULLBACK * 100)
    cfg        = pairs_config.get(pair, {})
    kraken_key = _request_map.get(pair) or cfg.get("kraken_pair", pair)
    candles    = fetch_ohlc(pair, kraken_key, 240, limit=60)
    if len(candles) < 35:
        return False, 0, 0, "insufficient candles"
    closes = [c["close"] for c in candles]
    rsi_4h = calc_rsi(closes)
    if rsi_4h is None:
        return False, 0, 0, "RSI unavailable"
    if not (LAYER_IN_RSI_MIN <= rsi_4h <= LAYER_IN_RSI_MAX):
        return False, 0, 0, "4h RSI {:.1f} outside {}-{}".format(
            rsi_4h, LAYER_IN_RSI_MIN, LAYER_IN_RSI_MAX)
    should_enter, entry_price, entry_reason, _ = evaluate_entry(pair, pairs_config, price)
    if not should_enter:
        return False, 0, 0, "no support signal: {}".format(entry_reason[:60])
    add_alloc = orig_alloc * (sold_vol / orig_vol)
    return (True, entry_price, sold_vol,
            "refill {:.6f} ({:.0f}% of original)  "
            "4h RSI {:.1f}  pullback {:.1f}%  {}".format(
                sold_vol,
                sold_vol / orig_vol * 100 if orig_vol else 0,
                rsi_4h,
                (hwm - price) / hwm * 100 if hwm else 0,
                entry_reason[:60]))


# =============================================================================
#  ORDER MANAGEMENT
# =============================================================================

_PRICE_DECIMALS = {
    "BNBUSD": 2, "BTCUSD": 1, "ETHUSD": 2, "LTCUSD": 2,
    "SOLUSD": 2, "XRPUSD": 4, "ZECUSD": 2, "ETHUSD": 2, "LTCUSD": 2,
    "SOLUSD": 3, "XRPUSD": 5, "ZECUSD": 2,
}


def fetch_pair_decimals(active_pairs, pairs_config):
    try:
        result     = kraken_public("/0/public/AssetPairs")
        pairs_data = result.get("result", {})
        for friendly, cfg in pairs_config.items():
            if friendly not in active_pairs:
                continue
            kraken_name = cfg.get("kraken_pair", friendly)
            for key in [kraken_name, kraken_name.replace("USD", "ZUSD"), "X" + kraken_name]:
                pair_info = pairs_data.get(key)
                if pair_info:
                    decimals = pair_info.get("pair_decimals")
                    if decimals is not None:
                        _PRICE_DECIMALS[friendly] = int(decimals)
                    break
        print("  [PAIRS] Decimals: {}".format(
            {p: _PRICE_DECIMALS.get(p) for p in active_pairs}))
    except Exception as e:
        print("  [WARN] Could not fetch pair decimals: {}".format(e))


def _fmt_price(pair, price):
    decimals = _PRICE_DECIMALS.get(pair, 4)
    return "{:.{}f}".format(price, decimals)


def get_volume(pair, usd_amount, price, pairs_config):
    if usd_amount < 5 or price <= 0:
        return None
    vol = usd_amount / price
    if price > 1000:   vol = round(vol, 6)
    elif price > 1:    vol = round(vol, 4)
    else:              vol = round(vol, 2)
    return vol


def place_buy_order(pair, entry_price, volume, pairs_config):
    cfg = pairs_config.get(pair, {})
    kraken_pair = cfg.get("kraken_pair", pair)
    try:
        result = kraken_private("/0/private/AddOrder", {
            "pair":      kraken_pair,
            "type":      "buy",
            "ordertype": "limit",
            "price":     _fmt_price(pair, entry_price),
            "volume":    str(volume),
            "oflags":    "post",
        })
        if result.get("error"):
            return None, str(result["error"])
        txids = result.get("result", {}).get("txid", [])
        return (txids[0], None) if txids else (None, "no txid returned")
    except Exception as e:
        return None, str(e)


# ── [v2 FIX #20] Added oflags:post for limit sells ──────────────────────────
def place_sell_order(pair, volume, pairs_config, limit_price=None):
    cfg         = pairs_config.get(pair, {})
    kraken_pair = cfg.get("kraken_pair", pair)
    if limit_price:
        try:
            result = kraken_private("/0/private/AddOrder", {
                "pair":      kraken_pair,
                "type":      "sell",
                "ordertype": "limit",
                "price":     _fmt_price(pair, limit_price),
                "volume":    str(volume),
                "oflags":    "post",
            })
            errors = result.get("error", [])
            if not errors:
                txids = result.get("result", {}).get("txid", [])
                if txids:
                    return txids[0], None, "limit"
            elif not any("taker" in str(e).lower() or "price" in str(e).lower()
                         for e in errors):
                return None, str(errors), "limit"
        except Exception:
            pass
    try:
        result = kraken_private("/0/private/AddOrder", {
            "pair":      kraken_pair,
            "type":      "sell",
            "ordertype": "market",
            "volume":    str(volume),
        })
        if result.get("error"):
            return None, str(result["error"]), "market"
        txids = result.get("result", {}).get("txid", [])
        return (txids[0], None, "market") if txids else (None, "no txid", "market")
    except Exception as e:
        return None, str(e), "market"


def cancel_order(txid):
    try:
        result = kraken_private("/0/private/CancelOrder", {"txid": txid})
        if result.get("error"):
            return False, str(result["error"])
        return True, None
    except Exception as e:
        return False, str(e)


def query_order(txid):
    try:
        result = kraken_private("/0/private/QueryOrders", {
            "txid": txid, "trades": "true"
        })
        if result.get("error"):
            print("  [WARN] query_order API error for {}: {}".format(txid, result["error"]))
            return None, None, None, None
        order      = result.get("result", {}).get(txid, {})
        if not order:
            print("  [WARN] query_order: txid {} not found in response".format(txid))
            return None, None, None, None
        status     = order.get("status")
        fill_price = float(order.get("price", 0) or 0)
        vol_exec   = float(order.get("vol_exec", 0) or 0)
        vol_total  = float(order.get("vol", 0) or 0)
        close_ts   = order.get("closetm")
        fill_time  = (datetime.fromtimestamp(float(close_ts), tz=timezone.utc)
                      if close_ts else None)
        if vol_total > 0 and vol_exec >= vol_total * 0.999:
            status = "closed"
        return status, fill_price, vol_exec, fill_time
    except Exception as e:
        print("  [WARN] query_order exception for {}: {}".format(txid, e))
        return None, None, None, None


# =============================================================================
#  STATE
# =============================================================================

DEFAULT_PAIR_STATE = {
    "status":               "watching",
    "order_placed_at":      None,
    "usd_allocated":        None,
    "fill_price":           None,
    "fill_volume":          None,
    "fill_timestamp":       None,
    "high_water_mark":      None,
    "sideways_clock_start": None,
    "trailing_stop_armed":  False,
    "entry_support_level":  None,
    "entry_support_label":  None,
    "txid":                 None,
    "sell_txid":            None,
    "sell_reason":          None,
    "sell_placed_at":       None,
    "sell_urgent":          False,
    "sell_is_partial":      False,         # [v2] NEW — partial sell tracking
    "sell_partial_fraction": 0,            # [v2] NEW
    "signal_data":          None,
    "tp_tier":              0,
    "total_volume":         None,
    "original_volume":      None,
    "original_alloc":       None,
    "layers_added":         0,
    "trailing_stop_pct":    None,
    "atr_pct":              None,
    "atr_ts_pct":           None,
    "atr_tp1_pct":          None,
    "atr_tp2_pct":          None,
    "atr_tp3_pct":          None,
    "layer_txid":           None,          # [v2] NEW — layer-in order tracking
    "layer_volume":         None,          # [v2] NEW
    "layer_price":          None,          # [v2] NEW
    "layer_placed_at":      None,          # [v2] NEW
    "last_exit_time":       None,          # [v2] NEW — re-entry cooldown
}

carried_from_state = set()


def load_state(active_pairs):
    base = {pair: dict(DEFAULT_PAIR_STATE) for pair in active_pairs}
    if os.path.exists(STATE_FILE):
        try:
            saved = json.load(open(STATE_FILE))
            for pair in active_pairs:
                if pair in saved:
                    # Merge saved into defaults so new keys get defaults
                    merged = dict(DEFAULT_PAIR_STATE)
                    merged.update(saved[pair])
                    base[pair] = merged
                    if saved[pair].get("status", "watching") != "watching":
                        carried_from_state.add(pair)
        except Exception as e:
            print("  [WARN] Could not load state: {}".format(e))
    return base


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def reset_to_watching(ps, pair, now_utc, skip_cooldown=False,
                      extended_cooldown=False, exit_price=None):
    # Preserve last exit price before wiping state — used to gate TREND_MODE
    # re-entries above the previous sell price.
    prev_exit_price = exit_price or ps.get("fill_price")
    ps.update({k: v for k, v in DEFAULT_PAIR_STATE.items()})
    if prev_exit_price:
        ps["last_exit_price"] = prev_exit_price
    if skip_cooldown:
        ps["last_exit_time"] = None
        print("  ... [{}] Reset to watching (no cooldown)".format(pair))
    elif extended_cooldown:
        ps["last_exit_time"] = now_utc.isoformat()
        ps["_extended_cooldown"] = True
        print("  ... [{}] Reset to watching (24h extended cooldown)".format(pair))
    else:
        ps["last_exit_time"] = now_utc.isoformat()
        print("  ... [{}] Reset to watching (60min cooldown)".format(pair))


# =============================================================================
#  OVER-ALLOCATION GUARD  [v2 FIX #4]
# =============================================================================

def calc_available_balance(usd_bal, state, active_pairs, current_pair):
    """
    Subtract capital locked in open positions from available balance.
    Only the budget for `current_pair` is available; other pairs' allocations
    are reserved.
    """
    if usd_bal is None:
        return 0
    locked = 0.0
    for pair in active_pairs:
        if pair == current_pair:
            continue
        ps = state.get(pair, {})
        status = ps.get("status", "watching")
        if status in ("order_open", "filled", "sell_pending"):
            locked += ps.get("usd_allocated") or 0
    available = max(0, usd_bal - locked)
    return available


# =============================================================================
#  LOGGING
# =============================================================================

TRADE_HEADERS = [
    "trade_num", "pair", "support_label", "support_level",
    "fill_timestamp", "exit_timestamp",
    "fill_price", "exit_price", "exit_type",
    "pnl_pct", "pnl_usd", "usd_allocated",
    "4h_rsi_entry", "daily_rsi_entry", "4h_hist_entry",
    "days_held",
    "entry_code", "entry_human",
]

EVENTS_HEADERS = [
    "utc_timestamp", "pair", "event_type", "price",
    "usd_allocated", "usd_balance", "note",
]

_trade_count = {}


def init_csvs():
    ensure_output_dirs()
    for path, headers in [(TRADE_HISTORY_CSV, TRADE_HEADERS),
                          (EVENTS_CSV, EVENTS_HEADERS)]:
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(headers)


def log_event(now_utc, pair, event_type, price, usd_alloc, usd_bal, note=""):
    with open(EVENTS_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            now_utc.strftime("%Y-%m-%d %H:%M:%S"),
            pair, event_type,
            "{:.4f}".format(price) if price else "",
            "{:.2f}".format(usd_alloc) if usd_alloc else "",
            "{:.2f}".format(usd_bal) if usd_bal else "",
            note,
        ])


def log_trade(pair, ps, exit_price, exit_type, now_utc, usd_bal):
    fp        = ps.get("fill_price") or 0
    alloc     = ps.get("usd_allocated") or 0
    fill_ts   = ps.get("fill_timestamp")
    sig       = ps.get("signal_data") or {}
    pnl_pct   = (exit_price - fp) / fp if fp else 0
    pnl_usd   = pnl_pct * alloc
    fill_dt   = datetime.fromisoformat(fill_ts) if fill_ts else now_utc
    days_held = (now_utc - fill_dt).total_seconds() / 86400
    _trade_count[pair] = _trade_count.get(pair, 0) + 1
    with open(TRADE_HISTORY_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            _trade_count[pair], pair,
            ps.get("entry_support_label", ""),
            "{:.4f}".format(ps.get("entry_support_level") or 0),
            fill_ts or "",
            now_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "{:.4f}".format(fp),
            "{:.4f}".format(exit_price),
            exit_type,
            "{:+.2f}%".format(pnl_pct * 100),
            "{:+.2f}".format(pnl_usd),
            "{:.2f}".format(alloc),
            "{:.1f}".format(sig.get("rsi_4h") or 0),
            "{:.1f}".format(sig.get("rsi_daily") or 0),
            "{:+.4f}".format(sig.get("hist_4h") or 0),
            "{:.2f}".format(days_held),
            ps.get("entry_code", ""),
            ps.get("entry_human", ""),
        ])
    # Record in session tracker immediately — this is the ONLY place record_trade
    # is called for new trades. load_history() on startup handles historical ones.
    # notify_sell() no longer calls record_trade to prevent double-counting.
    from monitor import session as _session
    _session.record_trade(pair, pnl_usd, pnl_pct * 100, alloc)
    return pnl_usd


# =============================================================================
#  COLORS
# =============================================================================

try:
    from colorama import init as _colorama_init, Style, Fore
    _colorama_init(autoreset=False)
    def _ansi256(n):      return "\033[38;5;{}m".format(n)
    def _ansi_bg(bg, fg): return "\033[48;5;{}m\033[38;5;{}m".format(bg, fg)
    PAIR_COLORS = {
        "BTCUSD": _ansi_bg(214, 15),
        "ETHUSD": _ansi_bg(33,  15),
        "SOLUSD": _ansi_bg(55,  45),
        "XRPUSD": _ansi_bg(250, 0),
    }
    _R     = Style.RESET_ALL
    _GREEN = Fore.GREEN
    _RED   = Fore.RED
    _DIM   = Style.DIM
    _BOLD  = Style.BRIGHT
    def _pnl_color(val):
        return _GREEN if val > 0 else (_RED if val < 0 else "")
except ImportError:
    PAIR_COLORS = {}
    _R = _GREEN = _RED = _DIM = _BOLD = ""
    def _pnl_color(val): return ""


def _c(pair):
    return PAIR_COLORS.get(pair, "")


# =============================================================================
#  SOUNDS
# =============================================================================

SOUNDS_ENABLED = True

# Mac: system sound name  |  Windows: (frequency_hz, duration_ms)
_SOUNDS = {
    "ORDER_PLACED":    ("Tink",   (440,  120)),
    "ORDER_FILLED":    ("Glass",  (880,  180)),
    "TAKE_PROFIT":     ("Hero",   (1047, 300)),
    "TRAILING_STOP":   ("Purr",   (330,  200)),
    "RSI_EXHAUSTION":  ("Hero",   (880,  300)),
    "SIDEWAYS_EXIT":   ("Funk",   (330,  200)),
    "MAX_HOLD_EXIT":   ("Funk",   (330,  200)),
    "STOP_LOSS":       ("Basso",  (200,  400)),
    "ORDER_CANCELLED": ("Sosumi", (220,  250)),
    "SELL_CONFIRMED":  ("Glass",  (660,  180)),
}


def play_sound(event_type):
    if not SOUNDS_ENABLED:
        return
    import subprocess, threading
    entry = _SOUNDS.get(event_type)
    if not entry:
        return
    mac_sound, (win_freq, win_dur) = entry
    def _play():
        try:
            if sys.platform == "darwin":
                path = "/System/Library/Sounds/{}.aiff".format(mac_sound)
                if os.path.exists(path):
                    subprocess.Popen(["afplay", path],
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
            elif sys.platform == "win32":
                import winsound
                winsound.Beep(win_freq, win_dur)
        except Exception:
            pass
    threading.Thread(target=_play, daemon=True).start()


# =============================================================================
#  DISPLAY
# =============================================================================

_update_count  = 0
_pair_snapshot = {}


def print_header(now_local, prices, usd_bal, state, active_pairs, pairs_config,
                 first_cycle=False):
    global _update_count, _pair_snapshot
    import io
    buf         = io.StringIO()
    now_utc     = datetime.now(timezone.utc)
    any_changed = False

    for pair in active_pairs:
        cfg    = pairs_config.get(pair, {})
        price  = prices.get(pair)
        ps     = state.get(pair, {})
        col    = _c(pair)
        status = ps.get("status", "watching")
        _last_reason = ps.get("_last_reason", "")
        _div_score   = ps.get("signal_data", {}).get("div_score", 0) if ps.get("signal_data") else 0
        _tier_a      = ps.get("signal_data", {}).get("tier_a_score", 0) if ps.get("signal_data") else 0
        fingerprint  = (status, ps.get("tp_tier", 0), ps.get("trailing_stop_armed", False),
                        _div_score, _tier_a, _last_reason[:80] if _last_reason else "")
        prev         = _pair_snapshot.get(pair)
        score_changed = (prev != fingerprint)
        _pair_snapshot[pair] = fingerprint
        any_changed = True

        if status == "watching":
            short_reason = (_last_reason or "scanning for swing setup")[:72]
            bear_timer   = ps.get("_bear_timer", "")
            st_line      = ps.get("_analysis_st", "")
            lt_line      = ps.get("_analysis_lt", "")
            out_line     = ps.get("_analysis_out", "")
            # [v2] Show cooldown if active
            cooldown_str = ""
            if ps.get("last_exit_time"):
                exit_dt  = datetime.fromisoformat(ps["last_exit_time"])
                elapsed  = (now_utc - exit_dt).total_seconds() / 60
                if elapsed < REENTRY_COOLDOWN_MINS:
                    cooldown_str = "  ⏳ cooldown {:.0f}min remaining".format(
                        REENTRY_COOLDOWN_MINS - elapsed)
            buf.write("{}  {:<8}{}  ${:>12,.4f}   [WATCHING]{}\n".format(
                col, pair, _R, price or 0, cooldown_str))
            buf.write("{}             {}{}\n".format(col, short_reason, _R))
            if bear_timer:
                buf.write("{}             📉 {}{}\n".format(col, bear_timer, _R))
            if st_line:
                buf.write("{}{}{}\n".format(col, st_line, _R))
            if lt_line:
                buf.write("{}{}{}\n".format(col, lt_line, _R))
            if out_line:
                buf.write("{}{}{}\n".format(col, out_line, _R))

        elif status == "order_open":
            ep        = ps.get("entry_price") or 0
            placed    = ps.get("order_placed_at", "")
            placed_dt = datetime.fromisoformat(placed) if placed else now_utc
            rem_s     = max(int(ORDER_EXPIRY_SECS - (now_utc - placed_dt).total_seconds()), 0)
            sup       = ps.get("entry_support_label", "?")
            buf.write("{}  {:<8}{}  ${:>12,.4f}   [BUY PENDING @ ${:,.4f}  {}  "
                      "expires {:02d}:{:02d}]\n".format(
                col, pair, _R, price or 0, ep, sup, rem_s // 60, rem_s % 60))

        elif status == "filled":
            fp      = ps.get("fill_price") or 0
            hwm     = ps.get("high_water_mark") or fp
            alloc   = ps.get("usd_allocated") or 0
            pnl_pct = (price - fp) / fp * 100 if fp and price else 0
            pnl_usd = alloc * ((price - fp) / fp) if fp and price and alloc else 0
            fill_ts = ps.get("fill_timestamp", "")
            fill_dt = datetime.fromisoformat(fill_ts) if fill_ts else now_utc
            days    = (now_utc - fill_dt).total_seconds() / 86400
            armed   = ps.get("trailing_stop_armed", False)
            tp_tier = ps.get("tp_tier", 0)
            layers  = ps.get("layers_added", 0)
            pc      = _pnl_color(pnl_usd)
            ts_base = ps.get("atr_ts_pct") or ps.get("trailing_stop_pct") or TRAILING_STOP_PCT
            tp1_d   = ps.get("atr_tp1_pct") or 0.10
            tp2_d   = ps.get("atr_tp2_pct") or 0.15
            tp3_d   = ps.get("atr_tp3_pct") or 0.20
            atr_d   = ps.get("atr_pct")
            ts_str  = "  🔒TS({:.0f}%)".format(ts_base*100) if armed else ""
            tier_str = "  T{}/3".format(tp_tier) if tp_tier > 0 else ""
            lay_str  = "  +{}L".format(layers) if layers > 0 else ""
            atr_str  = "  ATR{:.1f}%".format(atr_d*100) if atr_d else ""
            tp_str   = "  TP:{:.0f}%/{:.0f}%/{:.0f}%".format(
                tp1_d*100, tp2_d*100, tp3_d*100)
            # [v2] Show pending layer-in
            layer_str = ""
            if ps.get("layer_txid"):
                layer_str = "  📦 layer-in pending"
            buf.write("{}  {:<8}{}  ${:>12,.4f}   {}P/L:{:+.2f}% ({:+,.2f} USD){}  "
                      "held={:.1f}d  hwm=${:,.4f}{}{}{}{}{}{}\n".format(
                col, pair, _R, price or 0,
                pc, pnl_pct, pnl_usd, _R,
                days, hwm, ts_str, tier_str, lay_str, atr_str, tp_str, layer_str))
            if score_changed or first_cycle:
                sup = ps.get("entry_support_label", "")
                sig = ps.get("signal_data") or {}
                if sig:
                    buf.write("{}             entry: {} support ${:,.4f}  "
                              "4h RSI {:.1f}  Daily RSI {:.1f}  "
                              "fill ${:,.4f}{}\n".format(
                        col, sup,
                        ps.get("entry_support_level") or 0,
                        sig.get("rsi_4h") or 0,
                        sig.get("rsi_daily") or 0,
                        fp, _R))

        elif status == "sell_pending":
            sell_txid  = ps.get("sell_txid", "?")
            fp         = ps.get("fill_price") or 0
            pnl_pct    = (price - fp) / fp * 100 if fp and price else 0
            reason     = (ps.get("sell_reason") or "?").replace("_", " ").upper()
            placed     = ps.get("sell_placed_at", "")
            placed_dt  = datetime.fromisoformat(placed) if placed else now_utc
            wait_s     = int((now_utc - placed_dt).total_seconds())
            pc         = _pnl_color(pnl_pct)
            partial    = " (PARTIAL)" if ps.get("sell_is_partial") else ""
            buf.write("{}  {:<8}{}  ${:>12,.4f}   [SELL PENDING{}  {}P/L:{:+.2f}%{}  "
                      "reason={}  wait={}s  txid={}]\n".format(
                col, pair, _R, price or 0, partial,
                pc, pnl_pct, _R,
                reason, wait_s, sell_txid))

        buf.write("-" * 78 + "\n")

    if not (any_changed or first_cycle):
        return

    _update_count += 1
    now_str = now_local.strftime("%Y-%m-%d %H:%M:%S")
    tz_name = datetime.now().astimezone().tzname() or "local"
    _effective_budget = (min(usd_bal, MAX_BUDGET_USD) if usd_bal is not None
                         and MAX_BUDGET_USD is not None else usd_bal or MAX_BUDGET_USD)
    print("\n" + "=" * 78)
    print("  Kraken Swing Trader v2  |  {} {}  |  Update #{}".format(
        now_str, tz_name, _update_count))
    print("  USD balance: {}  |  Budget: ${}  |  Poll: {}min  |  Mode: 1-3d aggressive".format(
        "${:,.2f}".format(usd_bal) if usd_bal is not None else "?",
        "{:,.2f}".format(_effective_budget) if _effective_budget else "?",
        POLL_INTERVAL_SECS // 60))
    output = buf.getvalue()
    if output:
        print(output, end="")


# =============================================================================
#  PROCESS PAIR
# =============================================================================

def process_pair(pair, price, usd_bal, state, now_utc, pairs_config, active_pairs):
    cfg = pairs_config.get(pair, {})
    ps  = state[pair]

    # ── [v2 FIX #2] Check pending layer-in orders ────────────────────────────
    if ps.get("layer_txid") and ps.get("status") == "filled":
        layer_txid = ps["layer_txid"]
        l_status, l_fill_price, l_vol_exec, _ = query_order(layer_txid)

        if l_status == "closed" and l_vol_exec:
            actual_fill = l_fill_price if l_fill_price else (ps.get("layer_price") or price)
            old_vol  = ps.get("total_volume") or 0
            old_fp   = ps.get("fill_price") or actual_fill
            new_vol  = old_vol + l_vol_exec
            new_fp   = ((old_fp * old_vol + actual_fill * l_vol_exec) / new_vol
                        if new_vol else actual_fill)
            old_alloc = ps.get("usd_allocated") or 0
            add_alloc = actual_fill * l_vol_exec
            ps["fill_price"]    = new_fp
            ps["total_volume"]  = new_vol
            ps["layers_added"]  = ps.get("layers_added", 0) + 1
            ps["usd_allocated"] = old_alloc + add_alloc
            ps["layer_txid"]    = None
            ps["layer_volume"]  = None
            ps["layer_price"]   = None
            ps["layer_placed_at"] = None
            play_sound("ORDER_FILLED")
            print("  >>> [{}] LAYER-IN FILLED — vol={:.6f} @ ${:,.4f}  "
                  "avg_entry=${:,.4f}  txid={}".format(
                pair, l_vol_exec, actual_fill, new_fp, layer_txid))
            log_event(now_utc, pair, "LAYER_FILLED", actual_fill,
                      add_alloc, usd_bal,
                      "vol={:.6f} avg={:.4f} txid={}".format(
                          l_vol_exec, new_fp, layer_txid))

        elif l_status in ("canceled", "expired", "cancelled"):
            ps["layer_txid"]      = None
            ps["layer_volume"]    = None
            ps["layer_price"]     = None
            ps["layer_placed_at"] = None
            print("  ... [{}] Layer-in order cancelled/expired".format(pair))

        elif ps.get("layer_placed_at"):
            elapsed = (now_utc - datetime.fromisoformat(
                ps["layer_placed_at"])).total_seconds()
            if elapsed > ORDER_EXPIRY_SECS:
                cancel_order(layer_txid)
                ps["layer_txid"]      = None
                ps["layer_volume"]    = None
                ps["layer_price"]     = None
                ps["layer_placed_at"] = None
                print("  ... [{}] Layer-in expired after {:.0f}min".format(
                    pair, elapsed / 60))

    # ── sell_pending: both full AND partial sells ─────────────────────────────
    if ps["status"] == "sell_pending":
        sell_txid   = ps.get("sell_txid")
        sell_placed = ps.get("sell_placed_at")
        is_partial  = ps.get("sell_is_partial", False)

        if sell_txid:
            s_status, s_fill_price, s_vol, _ = query_order(sell_txid)

            if s_status == "closed":
                actual_exit = s_fill_price if s_fill_price else price
                fp          = ps.get("fill_price") or price
                alloc       = ps.get("usd_allocated") or 0
                sell_reason = ps.get("sell_reason") or "exit"

                if is_partial:
                    # [v2 FIX #3] Partial sell confirmed — NOW update volumes
                    fraction    = ps.get("sell_partial_fraction", 0.5)
                    total_vol   = ps.get("_pre_sell_total_volume") or ps.get("total_volume") or 0
                    sell_vol    = s_vol or round(total_vol * fraction, 8)
                    new_tier    = ps.get("tp_tier", 0) + 1
                    ps["tp_tier"]       = new_tier
                    ps["total_volume"]  = round(total_vol - sell_vol, 8)
                    ps["usd_allocated"] = alloc * (1 - fraction)
                    if new_tier == 1:
                        ps["trailing_stop_pct"] = TS_AFTER_TIER_1
                    elif new_tier >= 2:
                        ps["trailing_stop_pct"] = TS_AFTER_TIER_2
                    ps["status"]             = "filled"
                    ps["sell_txid"]          = None
                    ps["sell_reason"]        = None
                    ps["sell_placed_at"]     = None
                    ps["sell_is_partial"]    = False
                    ps["sell_partial_fraction"] = 0
                    ps["_pre_sell_total_volume"] = None
                    play_sound("SELL_CONFIRMED")
                    pnl_pct = (actual_exit - fp) / fp * 100 if fp else 0
                    print("  >>> [{}] PARTIAL SELL CONFIRMED T{} — {:.0f}% sold @ ${:,.4f}  "
                          "({:+.2f}%)  remaining vol={:.6f}  "
                          "TS tightened to {:.0f}%".format(
                        pair, new_tier, fraction * 100, actual_exit, pnl_pct,
                        ps["total_volume"],
                        (ps["trailing_stop_pct"] or TRAILING_STOP_PCT) * 100))
                    log_event(now_utc, pair, "PARTIAL_SELL_CONFIRMED", actual_exit,
                              alloc * fraction, usd_bal,
                              "tier={} vol_sold={:.6f} txid={}".format(
                                  new_tier, sell_vol, sell_txid))
                else:
                    # Full sell confirmed
                    pnl_usd = log_trade(pair, ps, actual_exit,
                                         sell_reason.upper(), now_utc, usd_bal)
                    play_sound("SELL_CONFIRMED")
                    print("  >>> [{}] SELL CONFIRMED @ ${:,.4f}  "
                          "({:+,.2f} USD)  txid={}".format(
                        pair, actual_exit, pnl_usd, sell_txid))
                    log_event(now_utc, pair, "SELL_CONFIRMED", actual_exit,
                              alloc, usd_bal,
                              "pnl={:+.2f} txid={}".format(pnl_usd, sell_txid))
                    reset_to_watching(ps, pair, now_utc, exit_price=actual_exit)

            elif s_status in ("canceled", "expired", "cancelled"):
                ps["status"]             = "filled"
                ps["sell_txid"]          = None
                ps["sell_is_partial"]    = False
                ps["sell_partial_fraction"] = 0
                ps["_pre_sell_total_volume"] = None
                print("  ... [{}] Sell cancelled — retrying exit".format(pair))

            elif s_status is None:
                if sell_placed:
                    elapsed = (now_utc - datetime.fromisoformat(
                        sell_placed)).total_seconds()
                    if elapsed > 120:
                        spot  = fetch_spot_balances()
                        base  = get_base_asset(pair)
                        held  = spot.get(base, 0)
                        fvol  = ps.get("fill_volume") or 0
                        if held < fvol * 0.10:
                            # Require 3 consecutive zero-balance confirmations
                            # before inferring the sell filled — prevents false
                            # triggers from stale/slow balance API responses.
                            confirm_count = ps.get("_zero_bal_confirms", 0) + 1
                            ps["_zero_bal_confirms"] = confirm_count
                            print("  ... [{}] Zero balance check {}/3 — "
                                  "held={:.6f} vs fill={:.6f}".format(
                                pair, confirm_count, held, fvol))
                            if confirm_count >= 3:
                                ps["_zero_bal_confirms"] = 0
                                pnl_usd = log_trade(pair, ps, price,
                                                      "SELL_CONFIRMED", now_utc, usd_bal)
                                print("  >>> [{}] SELL inferred from zero balance "
                                      "(confirmed 3x)  ({:+,.2f} USD)".format(
                                    pair, pnl_usd))
                                reset_to_watching(ps, pair, now_utc)
                        else:
                            # Balance still present — reset counter
                            ps["_zero_bal_confirms"] = 0
            else:
                if sell_placed:
                    elapsed = (now_utc - datetime.fromisoformat(
                        sell_placed)).total_seconds()
                    limit   = (SELL_LIMIT_SECS if ps.get("sell_urgent")
                               else SELL_LIMIT_SECS * 2)
                    if elapsed > limit:
                        cancel_order(sell_txid)
                        print("  ... [{}] Sell limit timed out — retrying".format(pair))
                        ps["status"]             = "filled"
                        ps["sell_txid"]          = None
                        ps["sell_is_partial"]    = False
                        ps["sell_partial_fraction"] = 0
                        ps["_pre_sell_total_volume"] = None
        else:
            ps["status"] = "filled"
            ps["sell_is_partial"] = False
        return

    if ps["status"] == "watching":
        # [v2 FIX #18] Re-entry cooldown check
        if ps.get("last_exit_time"):
            exit_dt = datetime.fromisoformat(ps["last_exit_time"])
            elapsed_mins = (now_utc - exit_dt).total_seconds() / 60
            if ps.get("_extended_cooldown"):
                cooldown_mins = 1440  # 24h after momentum exit
            elif ps.get("_rsi_cooldown"):
                cooldown_mins = RSI_EXHAUSTION_COOLDOWN_HRS * 60
            else:
                cooldown_mins = REENTRY_COOLDOWN_MINS
            if elapsed_mins < cooldown_mins:
                remaining = cooldown_mins - elapsed_mins
                if remaining > 60:
                    time_str = "{:.1f}h".format(remaining / 60)
                else:
                    time_str = "{:.0f}min".format(remaining)
                if ps.get("_extended_cooldown"):
                    cd_type = "momentum exit cooldown"
                elif ps.get("_rsi_cooldown"):
                    cd_type = "RSI exhaustion cooldown"
                else:
                    cd_type = "trade cooldown"
                ps["_last_reason"] = "{} — {} remaining".format(cd_type, time_str)
                return
            if ps.get("_extended_cooldown"):
                ps["_extended_cooldown"] = False
            if ps.get("_rsi_cooldown"):
                ps["_rsi_cooldown"] = False

        should_enter, entry_price, reason, signal_data = evaluate_entry(
            pair, pairs_config, price)

        # ── TREND_MODE re-entry price gate ───────────────────────────────────
        # If TREND_MODE fired but the current price is above the last exit price,
        # block the entry. Re-entering above your own exit means chasing —
        # two unnecessary trades and profit left on the table.
        # Support/divergence entries at lower prices are still allowed.
        if should_enter and signal_data.get("trend_mode"):
            last_exit_price = ps.get("last_exit_price")
            if last_exit_price and price > last_exit_price * 1.005:  # 0.5% buffer
                should_enter = False
                reason = ("TREND_MODE blocked — current ${:.2f} above last exit "
                          "${:.2f} ({:+.1f}%) — waiting for pullback below exit price".format(
                              price, last_exit_price,
                              (price - last_exit_price) / last_exit_price * 100))

        if not should_enter:
            if reason != ps.get("_last_reason"):
                ps["_last_reason"] = reason
            last_bt  = ps.get("_bear_timer_ts")
            bt_stale = (not last_bt or
                        (now_utc - datetime.fromisoformat(last_bt)
                         ).total_seconds() > 1800)
            cached_timer = ps.get("_bear_timer", "")
            if not bt_stale and "local high" in cached_timer and "at local high" not in cached_timer:
                bt_stale = True
            if bt_stale or "_bear_timer" not in ps:
                kraken_key = _request_map.get(pair) or cfg.get("kraken_pair", pair)
                bt = calc_bear_timer(pair, kraken_key, now_utc, current_price=price)
                ps["_bear_timer"]    = bt.get("summary", "")
                ps["_bear_timer_ts"] = now_utc.isoformat()
                ca = calc_comparative_analysis(pair, kraken_key, now_utc)
                ps["_analysis_st"]  = ca.get("st_line", "")
                ps["_analysis_lt"]  = ca.get("lt_line", "")
                ps["_analysis_out"] = ca.get("outlook_line", "")
            return

        # [v2 FIX #4] Use available balance (subtract locked capital)
        avail_bal = calc_available_balance(usd_bal, state, active_pairs, pair)
        if not avail_bal or avail_bal < 5:
            print("  ... [{}] Signal fired but insufficient available balance "
                  "(${:.2f} available, ${:.2f} locked)".format(
                pair, avail_bal or 0,
                (usd_bal or 0) - (avail_bal or 0)))
            return

        from monitor import session
        pair_realized = session.realized.get(pair, 0)
        # Fixed per-pair budget: even split of total capital at session start,
        # adjusted by this pair's realized P&L. Available cash is a safety cap.
        # This ensures every pair gets the same starting allocation regardless
        # of entry order, and winners grow their budget over time.
        pair_budget = _pair_budget_base + pair_realized
        alloc  = min(avail_bal, max(0, pair_budget))
        volume = get_volume(pair, alloc, entry_price, pairs_config)
        if not volume:
            print("  ... [{}] Volume too small (${:.2f})".format(pair, alloc))
            return
        txid, err = place_buy_order(pair, entry_price, volume, pairs_config)
        if err:
            print("  >>> [{}] BUY FAILED: {}".format(pair, err))
            log_event(now_utc, pair, "ORDER_FAILED", price, alloc, usd_bal,
                      "error={}".format(err))
            ps.setdefault("_order_stats", {})
            ps["_order_stats"]["failed"] = ps["_order_stats"].get("failed", 0) + 1
            from monitor import send_discord
            send_discord("❌  **{pair}** BUY FAILED\n"
                         "  Price ${price:,.2f}  |  alloc ${alloc:,.2f}\n"
                         "  _{err}_".format(
                             pair=pair, price=price, alloc=alloc, err=err))
            return
        play_sound("ORDER_PLACED")
        # Track order stats per pair
        ps.setdefault("_order_stats", {})
        ps["_order_stats"]["placed"] = ps["_order_stats"].get("placed", 0) + 1
        entry_code, entry_human = _entry_reason_code(signal_data, reason)
        print("  >>> [{}] SWING BUY ORDER  limit=${:,.4f}  vol={}  "
              "alloc=${:.2f}  txid={}".format(
            pair, entry_price, volume, alloc, txid))
        print("       {}".format(reason))
        ps.update({
            "status":              "order_open",
            "order_placed_at":     now_utc.isoformat(),
            "entry_price":         entry_price,
            "usd_allocated":       alloc,
            "txid":                txid,
            "entry_support_level": signal_data.get("support_level"),
            "entry_support_label": signal_data.get("support_label"),
            "entry_code":          entry_code,
            "entry_human":         entry_human,
            "signal_data":         signal_data,
        })
        log_event(now_utc, pair, "ORDER_PLACED", entry_price, alloc, usd_bal,
                  "support={} @ {:.4f} txid={} code={}".format(
                      signal_data.get("support_label"),
                      signal_data.get("support_level") or 0, txid, entry_code))
        from monitor import send_discord
        send_discord("📥  **{pair}** BUY ORDER PLACED\n"
                     "  Limit ${price:,.2f}  |  ${alloc:,.2f} allocated\n"
                     "  📋 {code} — {human}\n"
                     "  Orders: {placed} placed / {cancelled} cancelled / {filled} filled".format(
                         pair=pair, price=entry_price, alloc=alloc,
                         code=entry_code, human=entry_human,
                         placed=ps["_order_stats"].get("placed", 0),
                         cancelled=ps["_order_stats"].get("cancelled", 0),
                         filled=ps["_order_stats"].get("filled", 0)))

    elif ps["status"] == "order_open":
        placed_at   = datetime.fromisoformat(ps["order_placed_at"])
        elapsed_s   = (now_utc - placed_at).total_seconds()
        txid        = ps.get("txid")
        entry_price = ps.get("entry_price") or price
        if txid:
            status, fill_price, vol_exec, fill_time = query_order(txid)
        else:
            status, fill_price, vol_exec, fill_time = None, None, None, None
        if status == "closed" and vol_exec:
            actual_fill = fill_price if fill_price else entry_price
            play_sound("ORDER_FILLED")
            print("  >>> [{}] FILLED @ ${:,.4f}  vol={}  txid={}".format(
                pair, actual_fill, vol_exec, txid))
            log_event(now_utc, pair, "ORDER_FILLED", actual_fill,
                      ps.get("usd_allocated"), usd_bal,
                      "vol={} txid={}".format(vol_exec, txid))
            ft         = fill_time or now_utc
            kraken_key = pairs_config.get(pair, {}).get("kraken_pair", pair)
            kraken_key = _request_map.get(pair) or kraken_key
            c_daily    = fetch_ohlc(pair, kraken_key, 1440)
            atr_exits  = calc_atr_exits(c_daily, actual_fill)
            # Apply per-pair trailing stop floor/ceiling if configured.
            # This lets volatile pairs (ZEC, SOL, BCH) breathe without being
            # shaken out by normal price swings, while stable pairs keep the
            # default 8% global cap. Both are optional per-pair overrides.
            pair_overrides = pairs_config.get(pair, {}).get("entry_overrides", {})
            ts_floor = pair_overrides.get("ts_pct_min")
            ts_ceil  = pair_overrides.get("ts_pct_max")
            orig_ts  = atr_exits["ts_pct"]
            if ts_floor and atr_exits["ts_pct"] < ts_floor:
                atr_exits["ts_pct"] = ts_floor
            if ts_ceil and atr_exits["ts_pct"] > ts_ceil:
                atr_exits["ts_pct"] = ts_ceil
            if atr_exits["ts_pct"] != orig_ts:
                print("  ... [{}] TS adjusted by pair override: {:.1f}% → {:.1f}%".format(
                    pair, orig_ts * 100, atr_exits["ts_pct"] * 100))
            print("  ... [{}] ATR {:.1f}%  TS={:.1f}%  "
                  "TP={:.0f}%/{:.0f}%/{:.0f}%".format(
                pair,
                (atr_exits["atr_pct"] or 0) * 100,
                atr_exits["ts_pct"] * 100,
                atr_exits["tp_tier1_pct"] * 100,
                atr_exits["tp_tier2_pct"] * 100,
                atr_exits["tp_tier3_pct"] * 100))
            ps.update({
                "status":               "filled",
                "fill_price":           actual_fill,
                "fill_volume":          vol_exec,
                "fill_timestamp":       ft.isoformat(),
                "high_water_mark":      actual_fill,
                "sideways_clock_start": ft.isoformat(),
                "trailing_stop_armed":  False,
                "order_placed_at":      None,
                "atr_pct":              atr_exits["atr_pct"],
                "atr_ts_pct":           atr_exits["ts_pct"],
                "atr_tp1_pct":          atr_exits["tp_tier1_pct"],
                "atr_tp2_pct":          atr_exits["tp_tier2_pct"],
                "atr_tp3_pct":          atr_exits["tp_tier3_pct"],
            })
            ps.setdefault("_order_stats", {})
            ps["_order_stats"]["filled"] = ps["_order_stats"].get("filled", 0) + 1
            notify_buy(pair, actual_fill, ps.get("usd_allocated") or 0,
                       ps.get("entry_support_label", ""))
            from monitor import send_discord
            send_discord("✅  **{pair}** BUY FILLED\n"
                         "  Fill ${price:,.2f}  |  vol {vol}  |  ${alloc:,.2f} deployed\n"
                         "  📋 {code} — {human}\n"
                         "  Orders: {placed} placed / {cancelled} cancelled / {filled} filled".format(
                             pair=pair, price=actual_fill, vol=vol_exec,
                             alloc=ps.get("usd_allocated") or 0,
                             code=ps.get("entry_code", "—"),
                             human=ps.get("entry_human", ""),
                             placed=ps["_order_stats"].get("placed", 0),
                             cancelled=ps["_order_stats"].get("cancelled", 0),
                             filled=ps["_order_stats"].get("filled", 0)))
        elif status in ("canceled", "expired"):
            play_sound("ORDER_CANCELLED")
            print("  >>> [{}] ORDER CANCELLED (external)  txid={}".format(pair, txid))
            ps.setdefault("_order_stats", {})
            ps["_order_stats"]["cancelled"] = ps["_order_stats"].get("cancelled", 0) + 1
            from monitor import send_discord
            send_discord("🚫  **{pair}** ORDER CANCELLED (external)\n"
                         "  Was limit ${price:,.2f}  |  txid {txid}\n"
                         "  Orders: {placed} placed / {cancelled} cancelled / {filled} filled".format(
                             pair=pair, price=ps.get("entry_price", 0),
                             txid=txid,
                             placed=ps["_order_stats"].get("placed", 0),
                             cancelled=ps["_order_stats"].get("cancelled", 0),
                             filled=ps["_order_stats"].get("filled", 0)))
            reset_to_watching(ps, pair, now_utc, skip_cooldown=True)
        elif elapsed_s >= ORDER_EXPIRY_SECS and txid:
            ok, err = cancel_order(txid)
            play_sound("ORDER_CANCELLED")
            print("  >>> [{}] ORDER EXPIRED ({:.0f}min)  txid={}  cancel_ok={}".format(
                pair, ORDER_EXPIRY_SECS / 60, txid, not err))
            ps.setdefault("_order_stats", {})
            ps["_order_stats"]["cancelled"] = ps["_order_stats"].get("cancelled", 0) + 1
            log_event(now_utc, pair, "ORDER_CANCELLED", price,
                      ps.get("usd_allocated"), usd_bal,
                      "expired txid={}".format(txid))
            from monitor import send_discord
            send_discord("⏰  **{pair}** ORDER EXPIRED ({mins:.0f}min)\n"
                         "  Was limit ${price:,.2f}  |  txid {txid}\n"
                         "  Orders: {placed} placed / {cancelled} cancelled / {filled} filled".format(
                             pair=pair, mins=ORDER_EXPIRY_SECS / 60,
                             price=ps.get("entry_price", 0), txid=txid,
                             placed=ps["_order_stats"].get("placed", 0),
                             cancelled=ps["_order_stats"].get("cancelled", 0),
                             filled=ps["_order_stats"].get("filled", 0)))
            reset_to_watching(ps, pair, now_utc, skip_cooldown=True)

    elif ps["status"] == "filled":
        fp    = ps.get("fill_price") or 0
        if not fp:
            print("  [WARN] {} filled but no fill_price — clearing".format(pair))
            reset_to_watching(ps, pair, now_utc)
            return
        # Fire trailing stop that was breached while bot was offline.
        # Override evaluate_exit by injecting the flag into ps so the
        # exit block below picks it up naturally this poll.
        if ps.get("_force_exit"):
            print("  ⚠️  [{}] Forcing exit: {}".format(
                pair, ps.get("_force_exit_reason", "offline breach")))
            ps["_force_exit"] = False
            # Arm trailing stop at current price so evaluate_exit fires it
            ps["trailing_stop_armed"]   = True
            ps["high_water_mark"]       = fp  # reset HWM to entry so drop triggers
            ps["trailing_stop_pct"]     = 0.001  # 0.1% — fires immediately
        hwm   = ps.get("high_water_mark") or fp
        alloc = ps.get("usd_allocated") or 0
        txid  = ps.get("txid")
        if ps.get("original_volume") is None and ps.get("fill_volume"):
            ps["original_volume"] = ps["fill_volume"]
            ps["total_volume"]    = ps["fill_volume"]
        if ps.get("original_alloc") is None and alloc:
            ps["original_alloc"] = alloc
        if price > hwm:
            ps["high_water_mark"] = price
            hwm = price

        # [v2 FIX #2] Layer-in: place order but DON'T update position until confirmed
        if ps.get("tp_tier", 0) >= 1 and not ps.get("layer_txid"):
            should_add, add_price, add_vol, add_reason = evaluate_layer_in(
                pair, ps, price, pairs_config)
            add_alloc = add_vol * add_price if add_vol and add_price else 0
            avail_bal = calc_available_balance(usd_bal, state, active_pairs, pair)
            if should_add and add_vol > 0 and avail_bal and avail_bal >= add_alloc:
                add_txid, err = place_buy_order(pair, add_price, add_vol, pairs_config)
                if not err:
                    play_sound("ORDER_PLACED")
                    # Just record the pending order — don't touch position yet
                    ps["layer_txid"]      = add_txid
                    ps["layer_volume"]    = add_vol
                    ps["layer_price"]     = add_price
                    ps["layer_placed_at"] = now_utc.isoformat()
                    print("  >>> [{}] LAYER-IN ORDER PLACED — {:.6f} @ ${:,.4f}  "
                          "txid={}  (pending confirmation)".format(
                        pair, add_vol, add_price, add_txid))
                    print("       {}".format(add_reason))
                    log_event(now_utc, pair, "LAYER_IN_PLACED", add_price,
                              add_alloc, usd_bal,
                              "vol={:.6f} txid={}".format(add_vol, add_txid))

        exit_action, fraction, exit_reason, exit_type = evaluate_exit(
            pair, ps, price, now_utc, pairs_config)
        if exit_action == "hold":
            return

        # Cancel any pending layer-in before selling
        if ps.get("layer_txid"):
            cancel_order(ps["layer_txid"])
            ps["layer_txid"]      = None
            ps["layer_volume"]    = None
            ps["layer_price"]     = None
            ps["layer_placed_at"] = None
            print("  ... [{}] Cancelled pending layer-in for exit".format(pair))

        total_vol = ps.get("total_volume") or ps.get("fill_volume")
        if not total_vol:
            spot      = fetch_spot_balances()
            base      = get_base_asset(pair)
            total_vol = spot.get(base) or None
        if not total_vol:
            print("  [WARN] {} exit blocked — no volume".format(pair))
            return
        sell_vol    = round(total_vol * fraction, 8)
        is_full     = exit_action == "full_exit"
        limit_price = price * 1.001
        urgent      = exit_type in ("TRAILING_STOP", "STOP_LOSS")
        sell_txid, err, order_type = place_sell_order(
            pair, sell_vol, pairs_config, limit_price=limit_price)
        if err:
            print("  >>> [{}] SELL FAILED: {}".format(pair, err))
            log_event(now_utc, pair, "SELL_FAILED", price, alloc, usd_bal,
                      "error={}".format(err))
            return
        pnl_pct = (price - fp) / fp * 100 if fp else 0
        pct_str = "{:.0f}% of position".format(fraction * 100)
        sound_map = {
            "TAKE_PROFIT_TIER1": "TAKE_PROFIT",
            "TAKE_PROFIT_TIER2": "TAKE_PROFIT",
            "TAKE_PROFIT_TIER3": "TAKE_PROFIT",
            "TAKE_PROFIT":       "TAKE_PROFIT",
            "TRAILING_STOP":     "TRAILING_STOP",
            "RSI_EXHAUSTION":    "RSI_EXHAUSTION",
            "STOP_LOSS":         "STOP_LOSS",
            "SIDEWAYS_EXIT":     "SIDEWAYS_EXIT",
            "MAX_HOLD_EXIT":     "SIDEWAYS_EXIT",
        }
        play_sound(sound_map.get(exit_type, "ORDER_CANCELLED"))
        print("  >>> [{}] {} — selling {}  @ ${:,.4f}  ({:+.2f}%)  "
              "[{}]  sell_txid={}".format(
            pair, exit_type.replace("_", " "), pct_str,
            price, pnl_pct, order_type, sell_txid))
        print("       {}".format(exit_reason))
        log_event(now_utc, pair, exit_type, price, alloc, usd_bal,
                  "fill={:.4f} pnl={:+.2f}% vol={} frac={:.0f}% "
                  "txid={} sell={}".format(
                      fp, pnl_pct, sell_vol, fraction * 100,
                      txid or "?", sell_txid))
        pnl_usd = alloc * ((price - fp) / fp) if fp else 0
        notify_sell(pair, price, fp, pnl_pct, pnl_usd, exit_type, fraction, alloc)
        # Tag momentum exits for extended cooldown
        if exit_type == "MOMENTUM_EXIT":
            ps["_pending_extended_cooldown"] = True
        # Tag RSI exhaustion cooldown. TREND_MODE exits use extended cooldown
        # (same as momentum exit) to prevent immediate re-entry at a worse price.
        if exit_type == "RSI_EXHAUSTION":
            if ps.get("entry_code", "").startswith("TREND_"):
                ps["_pending_extended_cooldown"] = True  # 24h — don't re-enter trend chases
            else:
                ps["_pending_rsi_cooldown"] = True
        ps["_exit_type_for_cooldown"] = exit_type

        # [v2 FIX #3] ALL sells go through sell_pending for confirmation
        ps.update({
            "status":                  "sell_pending",
            "sell_txid":               sell_txid,
            "sell_reason":             exit_type.lower(),
            "sell_placed_at":          now_utc.isoformat(),
            "sell_urgent":             urgent,
            "sell_is_partial":         not is_full,
            "sell_partial_fraction":   fraction if not is_full else 0,
            "_pre_sell_total_volume":  total_vol if not is_full else None,
        })


# =============================================================================
#  MAIN
# =============================================================================

def main():
    if not LIVE_TRADING_ENABLED:
        print("[SAFE MODE] live_trading_enabled is false. No live orders will be placed.")
        return
    if not ACTIVE_PAIRS:
        print("[SAFE MODE] No active pairs configured. Copy pairs.example.json to pairs.json and configure it locally.")
        return
    print("=" * 78)
    print("  Kraken Swing Trader v2 — Aggressive 1-3 Day Mode")
    print("  !! REAL ORDERS WILL BE PLACED — SWING TRADE MODE !!")
    print("  Targeting {:.0f}-{:.0f}% gains over 1-3 day holds".format(
        min(cfg.get("tp_target", 0.10) for cfg in PAIRS.values()) * 100,
        max(cfg.get("tp_target", 0.12) for cfg in PAIRS.values()) * 100))
    print("  TS arm: +{:.0f}%  |  TS width: {:.1f}%  |  Sideways: {:.1f}d".format(
        TRAILING_STOP_ARM_PCT * 100, TRAILING_STOP_PCT * 100, SIDEWAYS_DAYS))
    print("  Poll: {}min  |  Order expiry: {}min  |  Cooldown: {}min".format(
        POLL_INTERVAL_SECS // 60, ORDER_EXPIRY_SECS // 60, REENTRY_COOLDOWN_MINS))
    print("=" * 78)
    print()
    try:
        ans = input("  Type SWING to confirm: ").strip()
    except EOFError:
        ans = ""
    if ans != "SWING":
        print("  Aborted.")
        return
    print()
    load_api_keys()
    ensure_output_dirs()
    init_csvs()
    active_pairs = list(ACTIVE_PAIRS)
    pairs_config = dict(PAIRS)
    print("  Resolving pair names...")
    request_map, response_map = build_pair_maps(active_pairs, pairs_config)
    global _request_map
    _request_map = request_map
    fetch_pair_decimals(active_pairs, pairs_config)
    print("  Pre-fetching candles (first run may take a moment)...")
    for pair in active_pairs:
        kkey = request_map.get(pair, pair)
        for tf, label in [(60, "1h"), (240, "4h"), (1440, "daily"), (10080, "weekly")]:
            cached  = _newest_ts(pair, tf)
            candles = fetch_ohlc(pair, kkey, tf)
            n       = len(candles)
            status  = "({} cached)".format(n) if cached else "({} fetched)".format(n)
            print("    {:<8} {}  {}".format(pair, label, status))
            time.sleep(0.5)
    print()
    print("  Loading state...")
    state = load_state(active_pairs)
    print("  Validating positions against Kraken balances...")
    spot = fetch_spot_balances()
    if spot:
        for pair in active_pairs:
            ps = state.get(pair, {})
            if ps.get("status") not in ("filled", "sell_pending"):
                continue
            base     = get_base_asset(pair)
            held     = spot.get(base, 0)
            fill_vol = ps.get("fill_volume") or 0
            if held < (fill_vol * 0.10 if fill_vol else 0.000001):
                print("  [SYNC] {} — no {} balance, clearing".format(pair, base))
                reset_to_watching(ps, pair, datetime.now(timezone.utc))
                carried_from_state.discard(pair)
    filled_carried = [p for p in active_pairs
                      if p in carried_from_state
                      and state[p].get("status") == "filled"]
    if filled_carried:
        print()
        print("  !! CARRIED SWING POSITIONS:")
        for pair in filled_carried:
            ps = state[pair]
            print("    {}{:<10}{}  fill=${:,.4f}  days held={}".format(
                _c(pair), pair, _R,
                ps.get("fill_price") or 0,
                "{:.1f}".format(
                    (datetime.now(timezone.utc) -
                     datetime.fromisoformat(ps["fill_timestamp"])).total_seconds() / 86400)
                if ps.get("fill_timestamp") else "?"))
        print()
        print("  [1] RESUME  — continue with existing exits")
        print("  [2] RIDE    — reset HWM + sideways timer to now (default)")
        print("  [3] FRESH   — reset all to watching")
        print()
        try:
            ans = input("  Choice [1/2/3] (default=2): ").strip() or "2"
        except EOFError:
            ans = "2"
        now_c = datetime.now(timezone.utc)
        try:
            _cp = fetch_ticker_prices(request_map, response_map)
        except Exception:
            _cp = {}
        for pair in filled_carried:
            ps = state[pair]
            if ans == "1":
                print("  [CARRY] {} — resuming".format(pair))
            elif ans == "3":
                reset_to_watching(ps, pair, now_c)
                carried_from_state.discard(pair)
            else:
                cur = _cp.get(pair) or ps.get("fill_price") or 0
                ps["high_water_mark"]      = cur
                ps["sideways_clock_start"] = now_c.isoformat()
                ps["trailing_stop_armed"]  = False
                print("  [CARRY] {} — RIDE mode, HWM reset to ${:,.4f}".format(pair, cur))
    # ── Startup trailing stop check ──────────────────────────────────────────
    # If the bot was off and a trailing stop was breached while it was down,
    # flag those positions for immediate exit on the first poll rather than
    # letting them sit with stale state.
    try:
        _startup_prices = fetch_ticker_prices(request_map, response_map)
        for pair in active_pairs:
            ps = state.get(pair, {})
            if ps.get("status") != "filled":
                continue
            if not ps.get("trailing_stop_armed"):
                continue
            fp  = ps.get("fill_price") or 0
            hwm = ps.get("high_water_mark") or fp
            cur = _startup_prices.get(pair)
            ts_pct = ps.get("atr_ts_pct") or ps.get("trailing_stop_pct") or TRAILING_STOP_PCT
            if cur and hwm and (hwm - cur) / hwm >= ts_pct:
                pnl = (cur - fp) / fp * 100 if fp else 0
                print("  ⚠️  [{}] TRAILING STOP BREACHED WHILE BOT WAS OFF — "
                      "HWM ${:.2f}  current ${:.2f}  ({:+.1f}%)  "
                      "flagging for immediate exit".format(pair, hwm, cur, pnl))
                ps["_force_exit"] = True
                ps["_force_exit_reason"] = (
                    "TRAILING_STOP (breached while bot was offline: "
                    "HWM ${:.2f} → ${:.2f}, {:+.1f}%)".format(hwm, cur, pnl))
    except Exception as e:
        print("  [WARN] Startup trailing stop check failed: {}".format(e))

    save_state(state)
    # Backdate session start to earliest carried position
    from monitor import session
    earliest = None
    for pair in active_pairs:
        ps = state.get(pair, {})
        if ps.get("status") in ("filled", "sell_pending") and ps.get("fill_timestamp"):
            ft = datetime.fromisoformat(ps["fill_timestamp"])
            if earliest is None or ft < earliest:
                earliest = ft
    # Always load completed trades from THIS session (since boot time).
    # Using boot_time (not earliest) ensures we never double-count trades
    # that completed during a prior run of the bot — only this run's trades
    # are counted by load_history; future trades in this run are counted by
    # log_trade → record_trade directly.
    boot_time = session.start_time
    session.load_history(TRADE_HISTORY_CSV, boot_time)
    if session.total_trades() > 0:
        print("  [SESSION] Loaded {} trade(s) since boot: {:+,.2f} USD realized".format(
            session.total_trades(), session.total_realized()))
    if earliest:
        session.set_start_time(earliest - timedelta(minutes=1))
        print("  [SESSION] Display backdated to earliest fill: {}".format(
            earliest.strftime("%Y-%m-%d %H:%M")))
    # Calculate even split per pair with 0.5% buffer
    global _pair_budget_base
    _startup_bal = fetch_usd_balance() or 0
    _total_capital = _startup_bal + sum(
        state[p].get("usd_allocated", 0) for p in active_pairs
        if state[p].get("status") in ("filled", "order_open", "sell_pending"))
    _pair_budget_base = (_total_capital * 0.97) / len(active_pairs)
    print("  [ALLOC] Total capital: ${:.2f} | Ref per-pair: ${:.2f} (3% buffer, dynamic at entry)".format(
        _total_capital, _pair_budget_base))
    print()
    first_cycle = True
    print("  Starting swing trade loop ({}min intervals)...".format(
        POLL_INTERVAL_SECS // 60))
    while True:
        now_local = datetime.now()
        now_utc   = datetime.now(timezone.utc)
        try:
            prices = fetch_ticker_prices(request_map, response_map)
        except Exception as e:
            print("  [ERROR] Ticker fetch: {}".format(e))
            prices = {}
        usd_bal = fetch_usd_balance()
        print_header(now_local, prices, usd_bal, state, active_pairs, pairs_config,
                     first_cycle=first_cycle)
        from monitor import session
        locked = sum(state[p].get("usd_allocated", 0)
                     for p in active_pairs
                     if state[p].get("status") in ("filled", "order_open", "sell_pending"))
        session.set_starting_usd((usd_bal or 0) + locked)
        heartbeat.pulse(extra={"positions": sum(
            1 for p in active_pairs if state[p].get("status") == "filled")})
        send_status_summary(state, prices, usd_bal, active_pairs)
        first_cycle = False
        for pair in active_pairs:
            price = prices.get(pair)
            if not price:
                continue
            process_pair(pair, price, usd_bal, state, now_utc, pairs_config, active_pairs)
        save_state(state)
        time.sleep(POLL_INTERVAL_SECS)


if __name__ == "__main__":
    run_supervised(main)
