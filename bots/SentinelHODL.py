"""
SentinelHODL.py — Bitcoin Macro Cycle Monitor & DCA Signal Bot
==============================================================
Monitors BTC's position in the macro market cycle using technical indicators,
market sentiment, and halving cycle data. Surfaces DCA-in/out signals for
manual execution. Does not place exchange orders. It generates heuristic signals for manual review.

Cycle Score (0–100):
  0–20  : Deep bear / capitulation  → 🟢 Strong accumulate
  20–40 : Early recovery            → 🟡 DCA in
  40–60 : Mid bull / hold           → ⚪ Hold
  60–80 : Late bull                 → 🟠 Start trimming
  80–100: Distribution / euphoria   → 🔴 Exit

Data sources (all free, no paid subscriptions required):
  - Kraken OHLC (weekly, daily)
  - Kraken Futures public API (BTC perpetual funding rate)
  - Alternative.me Fear & Greed Index
  - Mempool.space (current block height / halving cycle position)

Indicators:
  - Weekly RSI            (weight 20%)
  - Monthly RSI           (weight 15%)
  - Price vs 200-Week MA  (weight 22%)
  - Fear & Greed Index    (weight 15%)
  - Funding Rate          (weight 13%)
  - Pi Cycle Top          (weight 10%)
  - Halving Cycle Pos.    (weight  5%)

Cost basis tracking:
  Only tracks trades triggered by SENTINEL signals. Pre-existing BTC holdings
  are shown as live balance + value only — no P&L against unknown cost basis.

Outputs:
  - data/sentinel_state.json        : persisted state + SENTINEL trade log
  - data/event_log/sentinel_events.csv
  - Discord webhook (via monitor.py) : cycle updates + threshold crossings

Config:
  POLL_INTERVAL_SECS = 60    during testing
  POLL_INTERVAL_SECS = 28800 for production (8h)
"""

import time
import json
import csv
import os
import argparse
import hashlib
import hmac
import base64
import urllib.request
import urllib.parse
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import monitor as _monitor_mod
from monitor import (run_supervised, Heartbeat, send_discord)

heartbeat = Heartbeat()

# ---------------------------------------------------------------------------
# Per-pair config — loaded from --config argument if provided
# ---------------------------------------------------------------------------
def _load_sentinel_cfg():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=None)
    args, _ = parser.parse_known_args()
    if args.config and os.path.exists(args.config):
        with open(args.config) as f:
            return json.load(f)
    return {}

_sentinel_cfg = _load_sentinel_cfg()

# =============================================================================
#  CONFIG
# =============================================================================

POLL_INTERVAL_SECS  = 3600     # 3600 (1h) for now | 28800 (8h) for production
DISCORD_EVERY_N     = 1        # Post to Discord every N polls (1 = every poll)

SENTINEL_BUDGET_USD = 100.00   # Total capital SENTINEL is responsible for

BTC_PAIR            = "BTCUSD"
BTC_KRAKEN_KEY      = "XXBTZUSD"
DISPLAY_NAME        = "BTC"

# Override defaults from --config if provided (phase 1 — no _BASE_DIR needed yet)
if _sentinel_cfg:
    POLL_INTERVAL_SECS  = _sentinel_cfg.get("timing", {}).get("poll_interval_secs", POLL_INTERVAL_SECS)
    DISCORD_EVERY_N     = _sentinel_cfg.get("timing", {}).get("discord_every_n",    DISCORD_EVERY_N)
    SENTINEL_BUDGET_USD = _sentinel_cfg.get("budget_usd",                           SENTINEL_BUDGET_USD)
    BTC_PAIR            = _sentinel_cfg.get("pair",                                 BTC_PAIR)
    BTC_KRAKEN_KEY      = _sentinel_cfg.get("kraken_pair",                          BTC_KRAKEN_KEY)
    DISPLAY_NAME        = _sentinel_cfg.get("display_name",                         DISPLAY_NAME)

MAX_DAILY_CANDLES   = 1500     # ~4 years of daily data
MAX_WEEKLY_CANDLES  = 300      # ~5.7 years of weekly data

# Pi Cycle Top
PI_CYCLE_FAST       = 111      # 111-day MA
PI_CYCLE_SLOW       = 350      # 350-day MA (x2 = signal line)

# 200-week MA
MA_200W_PERIOD      = 200

# Halving
HALVING_INTERVAL    = 210_000
LAST_HALVING_BLOCK  = 840_000  # April 2024
NEXT_HALVING_BLOCK  = 1_050_000

# External data cache TTLs (seconds)
FNG_CACHE_SECS      = 3600     # F&G updates once daily; refresh hourly is fine
FUNDING_CACHE_SECS  = 3600
BLOCK_CACHE_SECS    = 600

# =============================================================================
#  PATHS
# =============================================================================

_BASE_DIR = os.environ.get(
    "SENTINELHODL_BASE_DIR",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_EVENT_DIR    = os.path.join(_BASE_DIR, "data", "event_log")
_SECRETS_DIR  = os.environ.get("SWINGTRADER_SECRETS_DIR", "")
_KEYS_FILE    = (
    os.path.join(_SECRETS_DIR, "keys.json")
    if _SECRETS_DIR and os.path.exists(os.path.join(_SECRETS_DIR, "keys.json"))
    else os.path.join(_BASE_DIR, "config", "keys.json")
)
STATE_FILE    = os.path.join(_BASE_DIR, "data", "sentinel_state.json")
EVENTS_CSV    = os.path.join(_EVENT_DIR, "sentinel_events.csv")
_SENTINEL_DB  = os.path.join(_BASE_DIR, "data", "sentinel_candles.db")

# Override paths + wire webhook (phase 2 — _BASE_DIR now available)
if _sentinel_cfg:
    _data = _sentinel_cfg.get("data", {})
    if _data.get("state_file"):
        STATE_FILE   = os.path.join(_BASE_DIR, _data["state_file"])
    if _data.get("events_csv"):
        EVENTS_CSV   = os.path.join(_BASE_DIR, _data["events_csv"])
    if _data.get("db_file"):
        _SENTINEL_DB = os.path.join(_BASE_DIR, _data["db_file"])
    # Wire per-pair webhook + interval into monitor module
    _wh_name    = _sentinel_cfg.get("discord", {}).get("webhook_name", "sentinel_btc")
    _config_dir = Path(_BASE_DIR) / "config"
    from monitor import _resolve_webhook, _resolve_webhook_interval
    _monitor_mod.DISCORD_WEBHOOK_URL = _resolve_webhook(_wh_name, _config_dir)
    _wh_interval = _resolve_webhook_interval(_wh_name, _config_dir)
    if _wh_interval:
        _monitor_mod.STATUS_INTERVAL_MIN = _wh_interval

API_KEY    = ""
API_SECRET = ""


def ensure_output_dirs():
    os.makedirs(_EVENT_DIR, exist_ok=True)


# =============================================================================
#  API — Kraken (same pattern as SWINGTRADER)
# =============================================================================

BASE_URL = "https://api.kraken.com"


class _RateLimiter:
    def __init__(self, min_interval=0.5):
        self._min_interval = min_interval
        self._last_call    = 0.0

    def wait(self):
        elapsed = time.time() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.time()


_rate_limiter = _RateLimiter(min_interval=0.5)


def _get(url, timeout=12):
    """Simple unauthenticated GET with a user-agent."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "SentinelHODL/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def kraken_public(endpoint, params=None):
    _rate_limiter.wait()
    url = BASE_URL + endpoint
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _get(url)


def kraken_private(endpoint, data=None):
    _rate_limiter.wait()
    if not API_KEY or not API_SECRET:
        raise PermissionError("API key not configured — signal-only mode")
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
        "User-Agent":   "SentinelHODL/1.0",
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
        print("  [KEYS] No keys file found — running in signal-only mode")
        return
    try:
        with open(_KEYS_FILE) as f:
            keys = json.load(f)
        API_KEY    = keys.get("API_KEY", "")
        API_SECRET = keys.get("API_SECRET", "")
        if API_KEY:
            print("  [KEYS] Loaded from config/keys.json")
        else:
            print("  [KEYS] Keys file found but API_KEY empty — signal-only mode")
    except Exception as e:
        print("  [KEYS] Load failed: {} — signal-only mode".format(e))


# =============================================================================
#  CANDLE CACHE (SQLite — separate DB from SWINGTRADER)
# =============================================================================

def _get_db():
    import sqlite3
    conn = sqlite3.connect(_SENTINEL_DB)
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
        print("  [WARN] DB save: {}".format(e))


def _load_candles(pair, tf_mins, limit=None):
    try:
        conn  = _get_db()
        rows  = conn.execute(
            "SELECT ts,open,high,low,close,volume FROM candles "
            "WHERE pair=? AND timeframe=? ORDER BY ts",
            (pair, tf_mins)
        ).fetchall()
        conn.close()
        candles = [{"ts": r[0], "open": r[1], "high": r[2],
                    "low": r[3], "close": r[4], "volume": r[5]}
                   for r in rows]
        return candles[-limit:] if limit else candles
    except Exception:
        return []


def fetch_ohlc(pair, kraken_key, tf_mins, limit=None):
    newest = _newest_ts(pair, tf_mins)
    try:
        if newest is None:
            result = kraken_public("/0/public/OHLC",
                                   {"pair": kraken_key, "interval": tf_mins})
            if not result.get("error"):
                data_key = next((k for k in result["result"] if k != "last"), None)
                if data_key:
                    raw = result["result"][data_key][:-1]
                    candles = [{"ts": int(c[0]), "open": float(c[1]),
                                "high": float(c[2]), "low": float(c[3]),
                                "close": float(c[4]), "volume": float(c[6])}
                               for c in raw]
                    _save_candles(pair, tf_mins, candles)
                    print("  [CACHE] {}/{}min — {} candles fetched".format(
                        pair, tf_mins, len(candles)))
        else:
            since  = newest - (tf_mins * 60)
            result = kraken_public("/0/public/OHLC",
                                   {"pair": kraken_key, "interval": tf_mins,
                                    "since": since})
            if not result.get("error"):
                data_key = next((k for k in result["result"] if k != "last"), None)
                if data_key:
                    new = [{"ts": int(c[0]), "open": float(c[1]),
                            "high": float(c[2]), "low": float(c[3]),
                            "close": float(c[4]), "volume": float(c[6])}
                           for c in result["result"][data_key][:-1]
                           if int(c[0]) > newest]
                    if new:
                        _save_candles(pair, tf_mins, new)
    except Exception as e:
        print("  [WARN] OHLC {}/{}min: {}".format(pair, tf_mins, e))
    return _load_candles(pair, tf_mins, limit)


# =============================================================================
#  INDICATORS
# =============================================================================

def _ema_series(values, period):
    if len(values) < period:
        return []
    k      = 2 / (period + 1)
    series = [sum(values[:period]) / period]
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


def calc_ma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def daily_to_monthly_closes(daily_candles):
    """Aggregate daily candles → one close per calendar month."""
    months = {}
    for c in daily_candles:
        dt  = datetime.fromtimestamp(c["ts"], tz=timezone.utc)
        key = (dt.year, dt.month)
        months[key] = c["close"]  # last day of month wins
    return [months[k] for k in sorted(months.keys())]


def calc_pi_cycle(daily_closes):
    """
    Pi Cycle Top Indicator.
    Returns (fast_ma, slow_ma_x2, crossed, pct_gap).
    crossed=True  →  111DMA above 2×350DMA (historically signals cycle top).
    pct_gap       →  how far 111DMA is from crossing (negative = not yet).
    """
    if len(daily_closes) < PI_CYCLE_SLOW + 5:
        return None, None, False, None
    fast_ma  = calc_ma(daily_closes, PI_CYCLE_FAST)
    slow_ma  = calc_ma(daily_closes, PI_CYCLE_SLOW)
    if not fast_ma or not slow_ma:
        return None, None, False, None
    slow_x2  = slow_ma * 2
    crossed  = fast_ma >= slow_x2
    pct_gap  = (fast_ma - slow_x2) / slow_x2 * 100
    return fast_ma, slow_x2, crossed, pct_gap


# =============================================================================
#  EXTERNAL DATA (all free, cached)
# =============================================================================

_fng_cache     = {"ts": None, "value": None, "label": None}
_block_cache   = {"ts": None, "height": None}
_funding_cache = {"ts": None, "rate": None}


def fetch_fear_greed():
    """Alternative.me Fear & Greed Index. Returns (int 0-100, label str)."""
    now = time.time()
    if _fng_cache["ts"] and now - _fng_cache["ts"] < FNG_CACHE_SECS:
        return _fng_cache["value"], _fng_cache["label"]
    try:
        data  = _get("https://api.alternative.me/fng/?limit=1", timeout=10)
        entry = data["data"][0]
        val   = int(entry["value"])
        label = entry["value_classification"]
        _fng_cache.update({"ts": now, "value": val, "label": label})
        return val, label
    except Exception as e:
        print("  [WARN] F&G fetch: {}".format(e))
        return _fng_cache["value"], _fng_cache["label"]


def fetch_block_height():
    """Current BTC block height from Mempool.space."""
    now = time.time()
    if _block_cache["ts"] and now - _block_cache["ts"] < BLOCK_CACHE_SECS:
        return _block_cache["height"]
    try:
        req = urllib.request.Request(
            "https://mempool.space/api/blocks/tip/height",
            headers={"User-Agent": "SentinelHODL/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            height = int(r.read().decode().strip())
        _block_cache.update({"ts": now, "height": height})
        return height
    except Exception as e:
        print("  [WARN] Block height fetch: {}".format(e))
        return _block_cache["height"]


def fetch_funding_rate():
    """
    BTC perpetual funding rate from Kraken Futures public API.
    Returns float (e.g. 0.0001 = 0.01% per 4h) or None.
    """
    now = time.time()
    if _funding_cache["ts"] and now - _funding_cache["ts"] < FUNDING_CACHE_SECS:
        return _funding_cache["rate"]
    try:
        data = _get("https://futures.kraken.com/derivatives/api/v3/tickers",
                    timeout=10)
        rate = None
        for ticker in data.get("tickers", []):
            if ticker.get("symbol") == "PF_XBTUSD":
                raw = ticker.get("fundingRate")
                if raw is not None:
                    rate = float(raw)
                break
        _funding_cache.update({"ts": now, "rate": rate})
        return rate
    except Exception as e:
        print("  [WARN] Funding rate fetch: {}".format(e))
        return _funding_cache["rate"]


def calc_halving_position(block_height):
    """
    Position in current halving cycle as 0.0–1.0.
    Returns (position, blocks_since_halving, blocks_until_next).
    """
    if block_height is None:
        return None, None, None
    blocks_since = max(0, block_height - LAST_HALVING_BLOCK)
    blocks_until = max(0, NEXT_HALVING_BLOCK - block_height)
    position     = min(1.0, blocks_since / HALVING_INTERVAL)
    return position, blocks_since, blocks_until


# =============================================================================
#  POSITION TRACKING
# =============================================================================

def fetch_btc_position():
    """
    Live BTC + USD balances from Kraken.
    Returns (btc_amount, usd_balance) or (None, None) in signal-only mode.
    """
    try:
        result = kraken_private("/0/private/Balance")
        if result.get("error"):
            errs = result["error"]
            if any("ermission" in e or "nvalid" in e for e in errs):
                print("  [POSITION] API key lacks balance permission — signal-only")
                return None, None
        raw     = result.get("result", {})
        btc_amt = float(raw.get("XXBT", raw.get("XBT", 0)) or 0)
        usd_bal = float(raw.get("ZUSD", raw.get("USD", 0)) or 0)
        return btc_amt, usd_bal
    except PermissionError:
        return None, None
    except Exception as e:
        print("  [WARN] Balance fetch: {}".format(e))
        return None, None


def calc_sentinel_slice(btc_balance, usd_balance, price, state):
    """
    Determine SENTINEL's $100 slice of total holdings.

    Logic:
      - SENTINEL budget is SENTINEL_BUDGET_USD ($100).
      - First, count any USD available (up to budget) as deployable cash.
      - Remaining budget is expressed as BTC already held.
      - Any BTC value beyond the budget is "pre-existing HODL" — SENTINEL
        does not manage it and does not count it toward P&L.

    Returns dict with keys:
      sentinel_btc_slice   : BTC SENTINEL considers "its" position
      preexisting_btc      : BTC outside SENTINEL's scope
      sentinel_usd_cash    : USD available within the budget
      sentinel_usd_in_btc  : USD value of sentinel_btc_slice
      budget_utilisation   : 0.0–1.0
    """
    if btc_balance is None or price is None or price == 0:
        return None

    total_btc_usd   = btc_balance * price
    avail_usd       = min(usd_balance or 0, SENTINEL_BUDGET_USD)
    btc_budget_usd  = SENTINEL_BUDGET_USD - avail_usd          # USD of budget already in BTC
    sentinel_btc    = min(btc_balance, btc_budget_usd / price)  # BTC slice SENTINEL owns
    preexisting_btc = max(0.0, btc_balance - sentinel_btc)
    sentinel_usd_val = sentinel_btc * price
    utilisation     = min(1.0, (sentinel_usd_val + avail_usd) / SENTINEL_BUDGET_USD)

    return {
        "sentinel_btc_slice":  sentinel_btc,
        "preexisting_btc":     preexisting_btc,
        "sentinel_usd_cash":   avail_usd,
        "sentinel_usd_in_btc": sentinel_usd_val,
        "budget_utilisation":  utilisation,
    }


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "sentinel_trades": [],   # only trades SENTINEL has signaled
        "sentinel_btc":    0.0,  # BTC acquired via SENTINEL signals
        "cost_basis":      None, # weighted avg cost of sentinel_btc
        "last_score":      None,
        "last_signal":     None,
        "poll_count":      0,
    }


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print("  [WARN] State save: {}".format(e))


def record_sentinel_trade(state, action, btc_amount, price):
    """Update SENTINEL cost basis on a triggered trade."""
    cur_btc  = state.get("sentinel_btc", 0.0)
    cur_cost = state.get("cost_basis") or 0.0

    if action == "buy":
        new_btc  = cur_btc + btc_amount
        new_cost = ((cur_btc * cur_cost) + (btc_amount * price)) / new_btc \
                   if new_btc > 0 else price
        state["sentinel_btc"]  = new_btc
        state["cost_basis"]    = new_cost
    elif action == "sell":
        state["sentinel_btc"] = max(0.0, cur_btc - btc_amount)
        # Cost basis stays fixed after partial sell

    state.setdefault("sentinel_trades", []).append({
        "ts":         datetime.now(timezone.utc).isoformat(),
        "action":     action,
        "btc":        btc_amount,
        "price":      price,
        "cost_basis": state.get("cost_basis"),
    })


# =============================================================================
#  CYCLE SCORING ENGINE
# =============================================================================

def _score_weekly_rsi(rsi):
    if rsi is None:  return 50, "N/A"
    if rsi < 20:     return  5, "Weekly RSI {:.1f} — extreme oversold".format(rsi)
    if rsi < 30:     return 15, "Weekly RSI {:.1f} — oversold".format(rsi)
    if rsi < 40:     return 28, "Weekly RSI {:.1f} — below midpoint".format(rsi)
    if rsi < 50:     return 42, "Weekly RSI {:.1f} — neutral low".format(rsi)
    if rsi < 60:     return 55, "Weekly RSI {:.1f} — neutral high".format(rsi)
    if rsi < 70:     return 68, "Weekly RSI {:.1f} — bullish".format(rsi)
    if rsi < 80:     return 82, "Weekly RSI {:.1f} — overbought".format(rsi)
    return               95, "Weekly RSI {:.1f} — extreme overbought".format(rsi)


def _score_monthly_rsi(rsi):
    if rsi is None:  return 50, "N/A"
    if rsi < 25:     return  5, "Monthly RSI {:.1f} — deep bear".format(rsi)
    if rsi < 35:     return 18, "Monthly RSI {:.1f} — oversold".format(rsi)
    if rsi < 45:     return 35, "Monthly RSI {:.1f} — recovering".format(rsi)
    if rsi < 55:     return 50, "Monthly RSI {:.1f} — neutral".format(rsi)
    if rsi < 65:     return 62, "Monthly RSI {:.1f} — bullish".format(rsi)
    if rsi < 75:     return 78, "Monthly RSI {:.1f} — late bull".format(rsi)
    return               92, "Monthly RSI {:.1f} — euphoria zone".format(rsi)


def _score_200w_ma(price, ma):
    if not ma:  return 50, "N/A"
    ratio = price / ma
    pct   = (ratio - 1) * 100
    if ratio < 0.80:  return  2, "Price {:.0f}% BELOW 200W MA — generational buy zone".format(abs(pct))
    if ratio < 1.00:  return 10, "Price {:.1f}% below 200W MA — historically strongest entry".format(abs(pct))
    if ratio < 1.50:  return 25, "Price {:.0f}% above 200W MA — early bull".format(pct)
    if ratio < 2.00:  return 42, "Price {:.0f}% above 200W MA — mid cycle".format(pct)
    if ratio < 3.00:  return 60, "Price {:.0f}% above 200W MA — bull run".format(pct)
    if ratio < 4.00:  return 75, "Price {:.0f}% above 200W MA — late cycle warning".format(pct)
    if ratio < 6.00:  return 87, "Price {:.0f}% above 200W MA — distribution zone".format(pct)
    return                96, "Price {:.0f}% above 200W MA — blow-off top territory".format(pct)


def _score_fear_greed(fng):
    if fng is None:  return 50, "N/A"
    if fng < 15:     label = "Extreme Fear"
    elif fng < 35:   label = "Fear"
    elif fng < 55:   label = "Neutral"
    elif fng < 75:   label = "Greed"
    else:            label = "Extreme Greed"
    return fng, "F&G {} — {}".format(fng, label)


def _score_funding(rate):
    if rate is None:  return 50, "N/A"
    p = rate * 100
    if rate < -0.05:  return  5, "Funding {:+.4f}% — shorts paying, capitulation signal".format(p)
    if rate < -0.01:  return 20, "Funding {:+.4f}% — negative, bearish bias".format(p)
    if rate <  0.01:  return 45, "Funding {:+.4f}% — near neutral".format(p)
    if rate <  0.03:  return 60, "Funding {:+.4f}% — elevated, bullish bias".format(p)
    if rate <  0.06:  return 75, "Funding {:+.4f}% — high, longs dominant".format(p)
    if rate <  0.10:  return 88, "Funding {:+.4f}% — very high, overheated".format(p)
    return                97, "Funding {:+.4f}% — extreme, blow-off risk".format(p)


def _score_pi_cycle(pct_gap, crossed):
    if pct_gap is None:   return 50, "N/A"
    if crossed:           return 97, "Pi Cycle CROSSED — historical top signal active ⚠️"
    if pct_gap > -5:      return 88, "Pi Cycle gap {:.1f}% — approaching top signal, watch closely".format(pct_gap)
    if pct_gap > -15:     return 72, "Pi Cycle gap {:.1f}% — narrowing".format(pct_gap)
    if pct_gap > -30:     return 55, "Pi Cycle gap {:.1f}% — mid cycle".format(pct_gap)
    if pct_gap > -50:     return 38, "Pi Cycle gap {:.1f}% — early cycle".format(pct_gap)
    return                     20, "Pi Cycle gap {:.1f}% — deep bear / bottom range".format(pct_gap)


def _score_halving(position):
    """
    Halving cycle position as a context modifier.
    Weighted lightly (5%) since influence diminishes each cycle.
    Peak historically at 50–65% through the cycle.
    """
    if position is None:  return 50, "N/A"
    pct = position * 100
    if position < 0.15:   return 30, "{:.0f}% through cycle — very early, accumulation phase".format(pct)
    if position < 0.35:   return 45, "{:.0f}% through cycle — early bull".format(pct)
    if position < 0.60:   return 65, "{:.0f}% through cycle — historically near peak window".format(pct)
    if position < 0.80:   return 52, "{:.0f}% through cycle — post-peak consolidation likely".format(pct)
    return                     35, "{:.0f}% through cycle — late cycle, pre-halving setup forming".format(pct)


def calc_cycle_score(weekly_rsi, monthly_rsi, price, ma_200w,
                     fng, funding_rate, pi_gap, pi_crossed, halving_pos):
    """
    Weighted composite score 0–100.
    Returns (score, scores_dict, descs_dict).
    """
    weights = {
        "weekly_rsi":   0.20,
        "monthly_rsi":  0.15,
        "ma_200w":      0.22,
        "fear_greed":   0.15,
        "funding":      0.13,
        "pi_cycle":     0.10,
        "halving":      0.05,
    }

    raw_scores, descs = {}, {}
    raw_scores["weekly_rsi"],  descs["weekly_rsi"]  = _score_weekly_rsi(weekly_rsi)
    raw_scores["monthly_rsi"], descs["monthly_rsi"] = _score_monthly_rsi(monthly_rsi)
    raw_scores["ma_200w"],     descs["ma_200w"]     = _score_200w_ma(price, ma_200w)
    raw_scores["fear_greed"],  descs["fear_greed"]  = _score_fear_greed(fng)
    raw_scores["funding"],     descs["funding"]     = _score_funding(funding_rate)
    raw_scores["pi_cycle"],    descs["pi_cycle"]    = _score_pi_cycle(pi_gap, pi_crossed)
    raw_scores["halving"],     descs["halving"]     = _score_halving(halving_pos)

    composite = sum(raw_scores[k] * weights[k] for k in weights)
    return round(composite), raw_scores, descs


def cycle_stage_info(score):
    """Returns (stage_name, emoji, action_str)."""
    if score < 20:  return "DEEP BEAR / CAPITULATION", "🟢", "STRONG ACCUMULATE"
    if score < 40:  return "EARLY RECOVERY",           "🟡", "DCA IN"
    if score < 60:  return "MID BULL",                 "⚪", "HOLD"
    if score < 80:  return "LATE BULL",                "🟠", "START TRIMMING"
    return               "DISTRIBUTION / EUPHORIA",   "🔴", "EXIT"


# =============================================================================
#  THREE-HORIZON OUTLOOK
# =============================================================================

def generate_outlook(score, weekly_rsi, monthly_rsi, fng, funding_rate,
                     pi_gap, pi_crossed, halving_pos, price, ma_200w):
    """
    Returns (now_str, near_str, long_str).
    Plain language. Not financial advice.
    """
    halving_pct = (halving_pos or 0) * 100
    ma_ratio    = (price / ma_200w) if ma_200w else None

    # ── NOW ──────────────────────────────────────────────────────────────────
    if score < 20:
        now = "Extreme fear + deep oversold. Market in or near capitulation — patience rewarded historically."
    elif score < 35:
        now = "Oversold conditions with early accumulation signals. Risk/reward tilts long."
    elif score < 50:
        now = "Neutral-to-recovering. No extreme readings. Trend needs confirmation."
    elif score < 65:
        now = "Momentum present, mid-cycle conditions. Hold existing positions."
    elif score < 80:
        now = "Elevated sentiment + technicals. Risk/reward shifting — reduce new entries."
    elif score < 90:
        now = "Multiple topping signals active. Consider trimming into strength."
    else:
        now = "Extreme greed + overextended. Blow-off top risk — protect capital."

    # ── NEAR TERM (4–12 weeks) ────────────────────────────────────────────────
    if weekly_rsi is not None and fng is not None:
        if weekly_rsi < 30 and fng < 25:
            near = "Continued weakness possible before reversal. Watch F&G < 15 + weekly RSI < 25 as capitulation markers."
        elif weekly_rsi < 45 and score < 40:
            near = "Recovery forming but not confirmed. Look for weekly MACD crossover and F&G > 30 as entry triggers."
        elif weekly_rsi > 70 and fng > 70:
            near = "Momentum elevated — near-term continuation possible but expect sharp corrections of 20–30%."
        elif weekly_rsi > 80:
            near = "Overbought on weekly timeframe. Pullback of 20–35% likely within 4–10 weeks."
        elif score < 50 and weekly_rsi > 40:
            near = "Stabilization likely. Range-bound action as market builds a base."
        else:
            near = "No strong directional signal near-term. Trend continuation with normal volatility."
    else:
        near = "Insufficient indicator data for near-term outlook."

    # ── LONG TERM ─────────────────────────────────────────────────────────────
    if pi_crossed:
        long = ("Pi Cycle Top crossed — historically within days-weeks of cycle peak. "
                "Halving cycle {:.0f}% complete. Capital preservation priority.".format(halving_pct))
    elif halving_pos is not None and halving_pos < 0.20:
        long = ("Early in halving cycle ({:.0f}%). Historical peak: ~12–18 months post-halving. "
                "Each cycle less extreme than the last. Accumulation window if macro supports.".format(halving_pct))
    elif halving_pos is not None and 0.20 <= halving_pos < 0.65:
        long = ("Halving cycle {:.0f}% complete — historically the peak window. "
                "Pi Cycle gap {:.1f}% from top signal. "
                "200W MA: ${:,.0f} ({:.1f}x below current price).".format(
                    halving_pct,
                    pi_gap if pi_gap is not None else 0,
                    ma_200w or 0,
                    ma_ratio or 0))
    elif ma_ratio is not None and ma_ratio < 1.0:
        long = ("Price below 200W MA — historically one of the rarest and most rewarding "
                "entry conditions in BTC's history. Long-term risk/reward strongly favors accumulation.")
    else:
        long = ("Halving cycle {:.0f}% complete. 200W MA at ${:,.0f} "
                "({:.1f}x below current). "
                "Long-term uptrend intact — cycle top indicators need continued monitoring.".format(
                    halving_pct, ma_200w or 0, ma_ratio or 0))

    return now, near, long


# =============================================================================
#  SIGNAL DETECTION  (threshold crossings)
# =============================================================================

# Thresholds where meaningful regime changes occur
_THRESHOLDS = [
    (20, "score dropped INTO deep bear — strong accumulation zone",  "DCA_STRONG",  "drop"),
    (20, "score rose OUT OF deep bear — early recovery underway",    "RECOVERY",    "rise"),
    (40, "score dropped — early recovery invalidated, bear resumes", "BEAR_RESUME", "drop"),
    (40, "score rose INTO early recovery — DCA IN signal",           "DCA_IN",      "rise"),
    (60, "score dropped — bull trend losing momentum",               "BULL_FADING", "drop"),
    (60, "score rose INTO mid bull — hold, reduce new buys",         "HOLD",        "rise"),
    (80, "score dropped from late bull — relief rally possible",     "PULLBACK",    "drop"),
    (80, "score rose INTO late bull — begin trimming positions",     "TRIM",        "rise"),
]

def check_signals(score, prev_score):
    """Returns list of (code, message) for any threshold crossings this poll."""
    if prev_score is None:
        return []
    signals = []
    for threshold, msg, code, direction in _THRESHOLDS:
        if direction == "drop" and prev_score >= threshold > score:
            signals.append((code, "⬇️  {}".format(msg)))
        if direction == "rise" and prev_score < threshold <= score:
            signals.append((code, "⬆️  {}".format(msg)))
    return signals


# =============================================================================
#  EVENT LOGGING
# =============================================================================

def log_event(event_type, score, price, detail=""):
    try:
        exists = os.path.exists(EVENTS_CSV)
        with open(EVENTS_CSV, "a", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["timestamp", "event_type", "score", "price", "detail"])
            w.writerow([datetime.now(timezone.utc).isoformat(),
                        event_type, score, "{:.2f}".format(price), detail])
    except Exception as e:
        print("  [WARN] Event log: {}".format(e))


# =============================================================================
#  DISCORD / TERMINAL OUTPUT
# =============================================================================

def _score_bar(score):
    """Visual 10-block progress bar for a 0–100 score."""
    filled = round(score / 10)
    return "█" * filled + "░" * (10 - filled)


def _ind_emoji(score):
    """Traffic-light emoji based on how bearish/bullish a score is."""
    if score < 30:  return "\U0001f7e2"  # green
    if score < 50:  return "\U0001f7e1"  # yellow
    if score < 70:  return "\U0001f7e0"  # orange
    return                 "\U0001f534"  # red


def format_status(score, raw_scores, descs, price, ma_200w,
                  fng, fng_label, funding_rate,
                  pi_fast, pi_slow_x2, pi_crossed, pi_gap,
                  block_height, halving_pos, blocks_until,
                  btc_balance, usd_balance,
                  state, now_out, near_out, long_out,
                  signals, now_utc, poll_count):

    stage, emoji, action = cycle_stage_info(score)

    # ── Header ────────────────────────────────────────────────────────────────
    lines = [
        "\U0001f6e1️  **SENTINEL HODL**  |  {}  |  Poll #{}".format(
            now_utc.strftime("%Y-%m-%d %H:%M UTC"), poll_count),
        "",
        "**{}/USD: ${:,.2f}**".format(DISPLAY_NAME, price),
        "{}  **Cycle Score: {}/100**  —  {}".format(emoji, score, stage),
        "```",
        "  [{}] {}/100".format(_score_bar(score), score),
        "  ACCUMULATE <-------------------------------> EXIT",
        "```",
        "\U0001f4cb  **Signal: {}**  _(not financial advice)_".format(action),
    ]

    # ── Indicators ────────────────────────────────────────────────────────────
    lines += ["", "\U0001f4ca  **Cycle Indicators**", "```"]
    label_map = [
        ("weekly_rsi",  "Weekly RSI",    20),
        ("monthly_rsi", "Monthly RSI",   15),
        ("ma_200w",     "200-Week MA",   22),
        ("fear_greed",  "Fear & Greed",  15),
        ("funding",     "Funding Rate",  13),
        ("pi_cycle",    "Pi Cycle Top",  10),
        ("halving",     "Halving Cycle",  5),
    ]
    for key, label, wt in label_map:
        lines.append("  {:<14} [{}] {:>3}/100  wt:{:>2}%".format(
            label, _score_bar(raw_scores[key]), raw_scores[key], wt))
    lines.append("```")

    # ── Key levels ────────────────────────────────────────────────────────────
    lines += ["", "\U0001f4d0  **Key Levels**", "```"]
    if ma_200w:
        lines.append("  200W MA    : ${:>10,.2f}  ({:.2f}x below current)".format(
            ma_200w, price / ma_200w))
    if pi_fast and pi_slow_x2:
        crossed_str = "  *** CROSSED - TOP SIGNAL ***" if pi_crossed else ""
        lines.append("  Pi 111DMA  : ${:>10,.2f}".format(pi_fast))
        lines.append("  Pi 2x350MA : ${:>10,.2f}  Gap: {:+.1f}%{}".format(
            pi_slow_x2, pi_gap or 0, crossed_str))
    if block_height and halving_pos is not None:
        lines.append("  Block      :  {:>9,}  ({:.1f}% through cycle | ~{:,} to next)".format(
            block_height, halving_pos * 100, blocks_until or 0))
    lines.append("```")

    # ── Position ──────────────────────────────────────────────────────────────
    lines += ["", "\U0001f4bc  **Position**  _(budget: ${:.2f})_".format(SENTINEL_BUDGET_USD), "```"]
    if btc_balance is not None:
        slc = calc_sentinel_slice(btc_balance, usd_balance, price, state)
        lines.append("  Total BTC    : {:.8f} BTC  = ${:,.2f}".format(
            btc_balance, btc_balance * price))
        lines.append("  USD Cash     : ${:,.2f}".format(usd_balance or 0))
        if slc:
            lines.append("  SENTINEL     : {:.8f} BTC  = ${:,.2f}  | cash ${:,.2f}  | {:.0f}% deployed".format(
                slc["sentinel_btc_slice"], slc["sentinel_usd_in_btc"],
                slc["sentinel_usd_cash"], slc["budget_utilisation"] * 100))
            if slc["preexisting_btc"] > 0.000001:
                lines.append("  Pre-existing : {:.8f} BTC  = ${:,.2f}  (outside scope)".format(
                    slc["preexisting_btc"], slc["preexisting_btc"] * price))
    else:
        lines.append("  [signal-only mode — no API balance permission]")

    s_btc  = state.get("sentinel_btc", 0.0)
    s_cost = state.get("cost_basis")
    if s_btc and s_btc > 0 and s_cost:
        pnl = (price - s_cost) / s_cost * 100
        lines.append("  P&L          : {:.8f} BTC @ avg ${:,.2f}  | {:+.1f}%".format(
            s_btc, s_cost, pnl))
    else:
        lines.append("  P&L          : no trades yet")
    lines.append("```")

    # ── Signals ───────────────────────────────────────────────────────────────
    if signals:
        lines += ["", "⚡  **THRESHOLD CROSSING**"]
        for _, msg in signals:
            lines.append("> {}".format(msg))

    # ── Outlook ───────────────────────────────────────────────────────────────
    lines += [
        "",
        "\U0001f52d  **Outlook**  _(not financial advice)_",
        "> \U0001f550  **Now:**  {}".format(now_out),
        "> \U0001f4c5  **Near-term:**  {}".format(near_out),
        "> \U0001f5d3️  **Long-term:**  {}".format(long_out),
        "",
        "─" * 35,
    ]

    return "\n".join(lines)


# =============================================================================
#  MAIN LOOP
# =============================================================================

_poll_count = 0


def run_cycle(state):
    global _poll_count
    _poll_count += 1
    now_utc = datetime.now(timezone.utc)

    # ── Candles ───────────────────────────────────────────────────────────────
    daily_candles  = fetch_ohlc(BTC_PAIR, BTC_KRAKEN_KEY, 1440,
                                limit=MAX_DAILY_CANDLES)
    weekly_candles = fetch_ohlc(BTC_PAIR, BTC_KRAKEN_KEY, 10080,
                                limit=MAX_WEEKLY_CANDLES)

    if len(daily_candles) < 60 or len(weekly_candles) < 30:
        print("  [WARN] Insufficient candle data (daily={}, weekly={}) — retrying next poll".format(
            len(daily_candles), len(weekly_candles)))
        return

    daily_closes  = [c["close"] for c in daily_candles]
    weekly_closes = [c["close"] for c in weekly_candles]
    price         = daily_closes[-1]

    # ── Indicators ────────────────────────────────────────────────────────────
    weekly_rsi     = calc_rsi(weekly_closes)
    monthly_closes = daily_to_monthly_closes(daily_candles)
    monthly_rsi    = calc_rsi(monthly_closes) if len(monthly_closes) >= 16 else None
    ma_200w        = calc_ma(weekly_closes, MA_200W_PERIOD)
    pi_fast, pi_slow_x2, pi_crossed, pi_gap = calc_pi_cycle(daily_closes)

    # ── External data ─────────────────────────────────────────────────────────
    fng, fng_label   = fetch_fear_greed()
    funding_rate     = fetch_funding_rate()
    block_height     = fetch_block_height()
    halving_pos, blocks_since, blocks_until = calc_halving_position(block_height)

    # ── Cycle score ───────────────────────────────────────────────────────────
    score, raw_scores, descs = calc_cycle_score(
        weekly_rsi, monthly_rsi, price, ma_200w,
        fng, funding_rate, pi_gap, pi_crossed, halving_pos)

    prev_score = state.get("last_score")

    # ── Signals + paper trades ────────────────────────────────────────────────
    signals = check_signals(score, prev_score)
    for sig_code, sig_msg in signals:
        print("  [SIGNAL] {} | Score {}/100 | BTC ${:,.2f}".format(
            sig_msg, score, price))
        log_event(sig_code, score, price, sig_msg)

        # Paper trade tracking ─────────────────────────────────────────────────
        # While SENTINEL has no order permission, hypothetical trades are recorded
        # as if executed at the signal price. Small size + limit order pricing
        # means real fills would land very close to market anyway.
        #
        # DCA_STRONG / DCA_IN  → BUY  25% of remaining budget
        # TRIM                 → SELL 50% of current sentinel position
        # BEAR_RESUME          → SELL 100% (stop out of position)
        paper_btc  = state.get("sentinel_btc", 0.0)
        paper_cost = state.get("cost_basis") or 0.0
        budget_used = (paper_btc * price) if paper_btc else 0.0
        budget_remain = max(0.0, SENTINEL_BUDGET_USD - budget_used)

        if sig_code in ("DCA_STRONG", "DCA_IN"):
            dca_usd = min(budget_remain, SENTINEL_BUDGET_USD * 0.25)
            if dca_usd >= 1.0:
                dca_btc = dca_usd / price
                record_sentinel_trade(state, "buy", dca_btc, price)
                note = "[PAPER BUY]  {:.8f} BTC @ ${:,.2f}  (${:.2f} | new avg ${:,.2f})".format(
                    dca_btc, price, dca_usd,
                    state.get("cost_basis") or price)
                print("  {}".format(note))
                log_event("PAPER_BUY", score, price, note)

        elif sig_code == "TRIM" and paper_btc > 0.000001:
            sell_btc = paper_btc * 0.50
            record_sentinel_trade(state, "sell", sell_btc, price)
            pnl = (price - paper_cost) / paper_cost * 100 if paper_cost else 0
            note = "[PAPER SELL] {:.8f} BTC @ ${:,.2f}  (50% trim | P&L {:+.1f}%)".format(
                sell_btc, price, pnl)
            print("  {}".format(note))
            log_event("PAPER_SELL", score, price, note)

        elif sig_code == "BEAR_RESUME" and paper_btc > 0.000001:
            sell_btc = paper_btc
            record_sentinel_trade(state, "sell", sell_btc, price)
            pnl = (price - paper_cost) / paper_cost * 100 if paper_cost else 0
            note = "[PAPER SELL] {:.8f} BTC @ ${:,.2f}  (full exit on bear resume | P&L {:+.1f}%)".format(
                sell_btc, price, pnl)
            print("  {}".format(note))
            log_event("PAPER_SELL", score, price, note)

    # ── Position ──────────────────────────────────────────────────────────────
    btc_balance, usd_balance = fetch_btc_position()
    # Cache balances so paper trade logic can reference them next poll
    if btc_balance is not None:
        state["_last_btc_balance"] = btc_balance
        state["_last_usd_balance"] = usd_balance or 0.0
    # Log slice breakdown on first poll or when score changes
    if btc_balance is not None:
        slc = calc_sentinel_slice(btc_balance, usd_balance, price, state)
        if slc and _poll_count == 1:
            print("  [SENTINEL] Budget: ${:.2f}  |  Slice: {:.8f} BTC (${:.2f})  |  "
                  "Pre-existing: {:.8f} BTC (${:.2f})".format(
                      SENTINEL_BUDGET_USD,
                      slc["sentinel_btc_slice"], slc["sentinel_usd_in_btc"],
                      slc["preexisting_btc"],    slc["preexisting_btc"] * price))

    # ── Outlook ───────────────────────────────────────────────────────────────
    now_out, near_out, long_out = generate_outlook(
        score, weekly_rsi, monthly_rsi, fng, funding_rate,
        pi_gap, pi_crossed, halving_pos, price, ma_200w)

    # ── Format + print ────────────────────────────────────────────────────────
    msg = format_status(
        score, raw_scores, descs, price, ma_200w,
        fng, fng_label, funding_rate,
        pi_fast, pi_slow_x2, pi_crossed, pi_gap,
        block_height, halving_pos, blocks_until,
        btc_balance, usd_balance,
        state, now_out, near_out, long_out,
        signals, now_utc, _poll_count)
    print(msg)

    # ── Discord ───────────────────────────────────────────────────────────────
    if _poll_count % DISCORD_EVERY_N == 0 or signals:
        try:
            send_discord(msg)
        except Exception as e:
            print("  [WARN] Discord send: {}".format(e))

    # ── Persist state ─────────────────────────────────────────────────────────
    state["last_score"] = score
    state["last_signal"] = signals[-1][0] if signals else state.get("last_signal")
    state["poll_count"]  = _poll_count
    save_state(state)

    log_event("POLL", score, price,
              "stage={} w_rsi={} m_rsi={} fng={} funding={} halving={:.1f}%".format(
                  cycle_stage_info(score)[0],
                  "{:.1f}".format(weekly_rsi) if weekly_rsi else "N/A",
                  "{:.1f}".format(monthly_rsi) if monthly_rsi else "N/A",
                  fng, funding_rate,
                  (halving_pos or 0) * 100))


def main():
    print("\n" + "=" * 78)
    print("  SENTINEL HODL — {} Macro Cycle Monitor v1.0".format(DISPLAY_NAME))
    print("  Started : {}  |  PID: {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), os.getpid()))
    print("  Poll    : {}s  |  Discord every {} poll(s)".format(
        POLL_INTERVAL_SECS, DISCORD_EVERY_N))
    print("  DB      : {}".format(_SENTINEL_DB))
    print("=" * 78 + "\n")

    ensure_output_dirs()
    load_api_keys()

    state = load_state()

    def _loop():
        while True:
            try:
                run_cycle(state)
            except KeyboardInterrupt:
                print("\n  [SENTINEL] Stopped by user.")
                sys.exit(0)
            except Exception as e:
                print("  [ERROR] Poll failed: {}".format(e))
                import traceback
                traceback.print_exc()
            time.sleep(POLL_INTERVAL_SECS)

    try:
        run_supervised(_loop)
    except Exception:
        _loop()


if __name__ == "__main__":
    main()
