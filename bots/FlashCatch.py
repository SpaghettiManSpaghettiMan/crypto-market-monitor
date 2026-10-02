"""
FlashCatch.py — Flash Crash Bounce Catcher
==========================================
Detects sudden flash crashes and enters staged positions to capture the bounce.
Runs alongside SWINGTRADER — one instance per pair via --config.

Strategy:
  1. Monitor 1H candles for crash (drop% + RSI oversold + volume spike)
  2. Crash detected → place TOE order (small, speculative, checks live balance)
  3. Confirmation signal (HA hammer / green candle) → place LAYER order
  4. Exit at target retracement OR stop below crash wick OR max hold timeout

Budget:
  - Shared flash_state.json pool ($500 default total)
  - Per-pair allocation weighted by historical win rate (bootstrap 50%)
  - Floor: $15 minimum per pair — no ceiling, winners earn more
  - Insufficient funds → Discord alert every 5 minutes until funded or signal expires

Usage:
  python FlashCatch.py --config config/flashcatch_ZECUSD_config.json
"""

import os
import sys
import json
import time
import argparse
import hashlib
import hmac
import base64
import sqlite3
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

import monitor as _monitor_mod
from monitor import run_supervised, Heartbeat, send_discord

heartbeat = Heartbeat()


# =============================================================================
#  CONFIG LOADING
# =============================================================================

def _load_cfg():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=None)
    args, _ = parser.parse_known_args()
    if args.config and os.path.exists(args.config):
        with open(args.config) as f:
            return json.load(f)
    print("[FlashCatch] ERROR: --config required.")
    print("  Usage: python FlashCatch.py --config config/flashcatch_ZECUSD_config.json")
    sys.exit(1)

CFG = _load_cfg()

PAIR           = CFG["pair"]
KRAKEN_PAIR    = CFG["kraken_pair"]
DISPLAY_NAME   = CFG.get("display_name", PAIR.replace("USD", ""))
CRASH_TRIGGER  = CFG.get("crash_trigger_pct",   12.0)
RSI_OVERSOLD   = CFG.get("rsi_oversold",         30)
VOL_MULT       = CFG.get("volume_multiplier",     2.0)
TOE_PCT        = CFG.get("toe_pct",               0.15)
LAYER_PCT      = CFG.get("layer_pct",             0.20)
TARGET_RETRACE = CFG.get("target_retracement",    0.60)
STOP_BUFFER    = CFG.get("stop_buffer_pct",       0.02)
MAX_HOLD_HRS   = CFG.get("max_hold_hours",        48)
ENTRY_SIGNAL   = CFG.get("entry_signal",          "hammer")
POLL_SECS      = CFG.get("poll_interval_secs",    300)
TOTAL_BUDGET   = CFG.get("total_budget",          500.0)
BUDGET_FLOOR   = CFG.get("budget_floor",          15.0)
ALERT_SECS     = CFG.get("alert_interval_secs",   300)
CRASH_WINDOW   = CFG.get("crash_window_candles",  8)
WEBHOOK_NAME   = CFG.get("discord", {}).get("webhook_name", "flashcatch")
LIVE_TRADING_ENABLED = bool(CFG.get("live_trading_enabled", False))


# =============================================================================
#  PATHS
# =============================================================================

_BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SECRETS   = os.environ.get("SWINGTRADER_SECRETS_DIR", "")
_KEYS_FILE = (
    os.path.join(_SECRETS, "keys.json")
    if _SECRETS and os.path.exists(os.path.join(_SECRETS, "keys.json"))
    else os.path.join(_BASE_DIR, "config", "keys.json")
)

_DATA      = CFG.get("data", {})
STATE_FILE = os.path.join(_BASE_DIR, _DATA.get("state_file",
             "data/flash_state.json"))
DB_FILE    = os.path.join(_BASE_DIR, _DATA.get("db_file",
             "data/flashcatch_{}_candles.db".format(PAIR)))

os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE),    exist_ok=True)


# =============================================================================
#  WIRE WEBHOOK INTO MONITOR
# =============================================================================

_config_dir = Path(_BASE_DIR) / "config"
from monitor import _resolve_webhook, _resolve_webhook_interval
_wh_url = _resolve_webhook(WEBHOOK_NAME, _config_dir)
if _wh_url:
    _monitor_mod.DISCORD_WEBHOOK_URL = _wh_url
_wh_interval = _resolve_webhook_interval(WEBHOOK_NAME, _config_dir)
if _wh_interval:
    _monitor_mod.STATUS_INTERVAL_MIN = _wh_interval


# =============================================================================
#  KRAKEN API
# =============================================================================

BASE_URL   = "https://api.kraken.com"
API_KEY    = ""
API_SECRET = ""


class _RateLimiter:
    def __init__(self, min_gap=0.5):
        self._last = 0.0
        self._gap  = min_gap

    def wait(self):
        elapsed = time.time() - self._last
        if elapsed < self._gap:
            time.sleep(self._gap - elapsed)
        self._last = time.time()

_rl = _RateLimiter()


def kraken_public(endpoint, params=None):
    _rl.wait()
    url = BASE_URL + endpoint
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(url), timeout=15) as r:
        return json.loads(r.read().decode())


def kraken_private(endpoint, data=None):
    _rl.wait()
    data          = data or {}
    nonce         = str(int(time.time() * 1000))
    data["nonce"] = nonce
    post          = urllib.parse.urlencode(data)
    encoded       = (nonce + post).encode()
    message       = endpoint.encode() + hashlib.sha256(encoded).digest()
    secret        = base64.b64decode(API_SECRET)
    sig           = hmac.new(secret, message, hashlib.sha512)
    headers = {
        "API-Key":      API_KEY,
        "API-Sign":     base64.b64encode(sig.digest()).decode(),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    req = urllib.request.Request(
        BASE_URL + endpoint, data=post.encode(), headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def load_api_keys():
    global API_KEY, API_SECRET
    if not os.path.exists(_KEYS_FILE):
        print("  [ERROR] Keys file not found: {}".format(_KEYS_FILE))
        sys.exit(1)
    with open(_KEYS_FILE) as f:
        keys = json.load(f)
    API_KEY    = keys.get("API_KEY",    "")
    API_SECRET = keys.get("API_SECRET", "")
    print("  [KEYS] Loaded.")


def fetch_usd_balance():
    try:
        result = kraken_private("/0/private/Balance")
        if result.get("error"):
            return None
        return float(result.get("result", {}).get("ZUSD", 0) or 0)
    except Exception as e:
        print("  [WARN] Balance fetch failed: {}".format(e))
        return None


# =============================================================================
#  OHLC CACHE (SQLite)
# =============================================================================

def _init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            pair TEXT, timeframe INTEGER, ts INTEGER,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (pair, timeframe, ts)
        )""")
    conn.commit()
    conn.close()


def _newest_ts(tf):
    conn = sqlite3.connect(DB_FILE)
    row  = conn.execute(
        "SELECT MAX(ts) FROM candles WHERE pair=? AND timeframe=?", (PAIR, tf)
    ).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def _save_candles(tf, candles):
    conn = sqlite3.connect(DB_FILE)
    conn.executemany(
        "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?,?)",
        [(PAIR, tf, c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"])
         for c in candles]
    )
    conn.commit()
    conn.close()


def _load_candles(tf, limit=None):
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute(
        "SELECT ts,open,high,low,close,volume FROM candles "
        "WHERE pair=? AND timeframe=? ORDER BY ts", (PAIR, tf)
    ).fetchall()
    conn.close()
    candles = [{"ts":r[0],"open":r[1],"high":r[2],"low":r[3],
                "close":r[4],"volume":r[5]} for r in rows]
    return candles[-limit:] if limit else candles


def fetch_ohlc(tf, limit=None):
    newest = _newest_ts(tf)
    params = {"pair": KRAKEN_PAIR, "interval": tf}
    if newest:
        params["since"] = newest - tf * 60
    try:
        result   = kraken_public("/0/public/OHLC", params)
        data_key = next((k for k in result["result"] if k != "last"), None)
        if data_key:
            raw     = result["result"][data_key][:-1]
            candles = [{"ts":int(c[0]),"open":float(c[1]),"high":float(c[2]),
                        "low":float(c[3]),"close":float(c[4]),"volume":float(c[6])}
                       for c in raw]
            new = [c for c in candles if not newest or c["ts"] > newest]
            if new:
                _save_candles(tf, new)
    except Exception as e:
        print("  [WARN] OHLC {}/{}min: {}".format(PAIR, tf, e))
    return _load_candles(tf, limit)


# =============================================================================
#  INDICATORS
# =============================================================================

def calc_rsi(closes, period=14):
    if len(closes) < period + 2:
        return None
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    ag     = sum(gains[:period])  / period
    al     = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i])  / period
        al = (al * (period - 1) + losses[i]) / period
    return 100 - (100 / (1 + ag / al)) if al != 0 else 100.0


def calc_ha(candles):
    ha = []
    for i, c in enumerate(candles):
        ha_c = (c["open"] + c["high"] + c["low"] + c["close"]) / 4
        ha_o = ((ha[i-1]["open"] + ha[i-1]["close"]) / 2) if i > 0 else ((c["open"] + c["close"]) / 2)
        ha.append({
            "open":  ha_o,
            "high":  max(c["high"], ha_o, ha_c),
            "low":   min(c["low"],  ha_o, ha_c),
            "close": ha_c,
        })
    return ha


def is_hammer(ha):
    body    = abs(ha["close"] - ha["open"])
    bot     = min(ha["open"], ha["close"])
    lo_wick = bot - ha["low"]
    hi_wick = ha["high"] - max(ha["open"], ha["close"])
    if body == 0:
        return False
    return lo_wick >= body * 1.5 and hi_wick <= body * 0.5


def is_green(ha):
    return ha["close"] > ha["open"]


# =============================================================================
#  BUDGET (weighted by win rate)
# =============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"total_budget": TOTAL_BUDGET, "pairs": {}}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_pair_state(state):
    if PAIR not in state["pairs"]:
        state["pairs"][PAIR] = {
            "wins": 0, "losses": 0,
            "status": "watching",
            "position": None,
            "last_alert_ts": None,
            "trades": [],
        }
    return state["pairs"][PAIR]


def calc_pair_budget(state):
    """
    Equal share baseline (total / num_pairs), scaled by this pair's win rate
    relative to the average. Bootstrap at 50% so day-one allocation is
    simply total_budget / active_pairs_count.

    Example (7 pairs, $500, all at bootstrap):
      base  = $500 / 7 = $71.43
      scale = 0.5 / 0.5 = 1.0  →  budget = $71.43

    As ZEC wins more than average, scale > 1.0 and it earns a larger slice.
    No ceiling — winners grow uncapped. Floor: BUDGET_FLOOR ($15).
    """
    def wr(p):
        w, l = p.get("wins", 0), p.get("losses", 0)
        return w / (w + l) if (w + l) > 0 else 0.5

    num_pairs = CFG.get("active_pairs_count", 7)
    pairs     = state.get("pairs", {})
    my_wr     = wr(pairs.get(PAIR, {}))
    avg_wr    = sum(wr(p) for p in pairs.values()) / len(pairs) if pairs else 0.5
    base      = state.get("total_budget", TOTAL_BUDGET) / num_pairs
    scale     = my_wr / avg_wr if avg_wr > 0 else 1.0
    return max(base * scale, BUDGET_FLOOR)


# =============================================================================
#  ORDERS
# =============================================================================

def place_buy_limit(price, usd_amount):
    volume = round(usd_amount / price, 8)
    try:
        result = kraken_private("/0/private/AddOrder", {
            "pair": KRAKEN_PAIR, "type": "buy", "ordertype": "limit",
            "price": str(round(price, 4)), "volume": str(volume),
        })
        if result.get("error"):
            return None, str(result["error"])
        txids = result.get("result", {}).get("txid", [])
        return (txids[0], None) if txids else (None, "no txid")
    except Exception as e:
        return None, str(e)


def place_sell_limit(price, volume):
    try:
        result = kraken_private("/0/private/AddOrder", {
            "pair": KRAKEN_PAIR, "type": "sell", "ordertype": "limit",
            "price": str(round(price, 4)), "volume": str(volume),
        })
        if result.get("error"):
            return None, str(result["error"])
        txids = result.get("result", {}).get("txid", [])
        return (txids[0], None) if txids else (None, "no txid")
    except Exception as e:
        return None, str(e)


def place_sell_market(volume):
    try:
        result = kraken_private("/0/private/AddOrder", {
            "pair": KRAKEN_PAIR, "type": "sell", "ordertype": "market",
            "volume": str(volume),
        })
        if result.get("error"):
            return None, str(result["error"])
        txids = result.get("result", {}).get("txid", [])
        return (txids[0], None) if txids else (None, "no txid")
    except Exception as e:
        return None, str(e)


def query_order(txid):
    try:
        result    = kraken_private("/0/private/QueryOrders", {"txid": txid, "trades": "true"})
        order     = result.get("result", {}).get(txid, {})
        status    = order.get("status")
        fill_px   = float(order.get("price",    0) or 0)
        vol_exec  = float(order.get("vol_exec", 0) or 0)
        vol_total = float(order.get("vol",      0) or 0)
        if vol_total > 0 and vol_exec >= vol_total * 0.999:
            status = "closed"
        return status, fill_px, vol_exec
    except Exception:
        return None, None, None


# =============================================================================
#  CRASH DETECTION
# =============================================================================

def detect_crash(candles):
    """
    Returns (detected, crash_high, crash_low, drop_pct, current_price).
    Scans last CRASH_WINDOW candles for drop + RSI oversold + volume spike.
    """
    if len(candles) < CRASH_WINDOW + 14:
        return False, None, None, None, None

    window   = candles[-CRASH_WINDOW:]
    crash_hi = max(c["high"]   for c in window)
    crash_lo = min(c["low"]    for c in window)
    curr_px  = candles[-1]["close"]
    drop_pct = (crash_hi - crash_lo) / crash_hi * 100

    closes   = [c["close"] for c in candles]
    rsi      = calc_rsi(closes)

    vols     = [c["volume"] for c in candles]
    avg_vol  = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else (sum(vols) / len(vols))
    peak_vol = max(c["volume"] for c in window)

    detected = (
        drop_pct  >= CRASH_TRIGGER and
        rsi is not None and rsi <= RSI_OVERSOLD and
        peak_vol  >= avg_vol * VOL_MULT
    )
    return detected, crash_hi, crash_lo, drop_pct, curr_px


# =============================================================================
#  TRADE RECORDING
# =============================================================================

def record_trade(state, pos, exit_px, reason, pnl=None):
    ps  = get_pair_state(state)
    win = (exit_px > (pos.get("fill_px") or 0)) if pnl is None else (pnl > 0)
    if win:
        ps["wins"] = ps.get("wins", 0) + 1
    else:
        ps["losses"] = ps.get("losses", 0) + 1
    ps.setdefault("trades", []).append({
        "date":     datetime.now(timezone.utc).isoformat(),
        "entry_px": pos.get("fill_px"),
        "exit_px":  exit_px,
        "reason":   reason,
        "pnl":      round(pnl, 4) if pnl is not None else None,
        "win":      win,
    })
    ps["status"]   = "watching"
    ps["position"] = None


# =============================================================================
#  MAIN CYCLE
# =============================================================================

def run_cycle(state):
    ps      = get_pair_state(state)
    now_utc = datetime.now(timezone.utc)
    status  = ps.get("status", "watching")

    candles = fetch_ohlc(60, limit=120)
    if len(candles) < CRASH_WINDOW + 14:
        print("  [{}] Warming up — {} candles so far.".format(DISPLAY_NAME, len(candles)))
        return

    ha      = calc_ha(candles)
    curr_px = candles[-1]["close"]
    closes  = [c["close"] for c in candles]
    rsi     = calc_rsi(closes) or 0

    # ── WATCHING ─────────────────────────────────────────────────────────────
    if status == "watching":
        detected, crash_hi, crash_lo, drop_pct, _ = detect_crash(candles)
        if not detected:
            print("  [{}] Watching... price={:.2f}  RSI={:.1f}".format(
                DISPLAY_NAME, curr_px, rsi))
            return

        pair_budget = calc_pair_budget(state)
        toe_usd     = round(pair_budget * TOE_PCT, 2)
        usd_avail   = fetch_usd_balance() or 0
        stop_px     = round(crash_lo * (1 - STOP_BUFFER), 4)

        header = (
            "⚡ **FlashCatch {name}** — Crash detected!\n"
            "```\n"
            "  Drop    : {drop:.1f}%  ({win}h window)\n"
            "  High    : ${hi:,.2f}    Low: ${lo:,.2f}\n"
            "  Current : ${curr:,.2f}    RSI: {rsi:.1f}\n"
            "  Stop    : ${stop:,.4f}\n"
            "  Toe     : ${toe:.2f}    Available: ${avail:.2f}\n"
            "```"
        ).format(name=DISPLAY_NAME, drop=drop_pct, win=CRASH_WINDOW,
                 hi=crash_hi, lo=crash_lo, curr=curr_px, rsi=rsi,
                 stop=stop_px, toe=toe_usd, avail=usd_avail)

        if usd_avail >= toe_usd:
            limit_px  = round(curr_px * 1.001, 4)
            txid, err = place_buy_limit(limit_px, toe_usd)
            if err:
                send_discord(header + "\n❌ Order failed: {}".format(err))
            else:
                ps["status"]   = "toe_placed"
                ps["position"] = {
                    "phase":      "toe",
                    "txid":       txid,
                    "usd_spent":  toe_usd,
                    "crash_high": crash_hi,
                    "crash_low":  crash_lo,
                    "drop_pct":   drop_pct,
                    "entry_ts":   now_utc.isoformat(),
                    "limit_px":   limit_px,
                    "fill_px":    None,
                    "volume":     None,
                    "target_px":  None,
                    "stop_px":    stop_px,
                    "sell_txid":  None,
                    "layer_txid": None,
                    "layer_vol":  None,
                }
                send_discord(header + "\n✅ Toe order placed @ **${:.4f}**  |  txid: `{}`".format(
                    limit_px, txid))
        else:
            ps["status"]        = "waiting_funds"
            ps["last_alert_ts"] = now_utc.isoformat()
            ps["position"]      = {
                "phase":      "waiting",
                "crash_high": crash_hi,
                "crash_low":  crash_lo,
                "drop_pct":   drop_pct,
                "entry_ts":   now_utc.isoformat(),
                "usd_needed": toe_usd,
                "stop_px":    stop_px,
            }
            send_discord(header + "\n⚠️  **Need ${:.2f} — only ${:.2f} available.** Alerting every {}min.".format(
                toe_usd, usd_avail, ALERT_SECS // 60))

    # ── WAITING FOR FUNDS ─────────────────────────────────────────────────────
    elif status == "waiting_funds":
        pos      = ps.get("position", {})
        stop_px  = pos.get("stop_px", 0)

        if curr_px <= stop_px:
            send_discord("🚫 **FlashCatch {}** — Signal expired (price hit stop ${:.4f}). Back to watching.".format(
                DISPLAY_NAME, stop_px))
            ps["status"]   = "watching"
            ps["position"] = None
            save_state(state)
            return

        pair_budget = calc_pair_budget(state)
        toe_usd     = round(pair_budget * TOE_PCT, 2)
        usd_avail   = fetch_usd_balance() or 0

        if usd_avail >= toe_usd:
            limit_px  = round(curr_px * 1.001, 4)
            txid, err = place_buy_limit(limit_px, toe_usd)
            if not err:
                pos.update({"phase":"toe","txid":txid,"usd_spent":toe_usd,"limit_px":limit_px,
                             "fill_px":None,"volume":None,"target_px":None,"sell_txid":None,"layer_txid":None,"layer_vol":None})
                ps["status"] = "toe_placed"
                send_discord("✅ **FlashCatch {}** — Funded! Toe placed @ **${:.4f}**".format(
                    DISPLAY_NAME, limit_px))
        else:
            last = ps.get("last_alert_ts")
            if last:
                elapsed = (now_utc - datetime.fromisoformat(last)).total_seconds()
                if elapsed < ALERT_SECS:
                    return
            ps["last_alert_ts"] = now_utc.isoformat()
            send_discord("⏳ **FlashCatch {}** — Still waiting. Need **${:.2f}**, have **${:.2f}**. "
                         "Signal alive — price ${:.2f}, stop ${:.2f}.".format(
                DISPLAY_NAME, toe_usd, usd_avail, curr_px, stop_px))

    # ── TOE PLACED: wait for fill ─────────────────────────────────────────────
    elif status == "toe_placed":
        pos            = ps.get("position", {})
        st, fill, vol  = query_order(pos["txid"])

        if st == "closed" and fill and vol:
            crash_move       = pos["crash_high"] - pos["crash_low"]
            pos["fill_px"]   = fill
            pos["volume"]    = vol
            pos["target_px"] = round(pos["crash_low"] + crash_move * TARGET_RETRACE, 4)
            ps["status"]     = "toe_filled"
            send_discord(
                "✅ **FlashCatch {}** — Toe filled!\n"
                "```\n"
                "  Fill   : ${:.4f}\n"
                "  Volume : {:.6f}\n"
                "  Target : ${:.4f}  ({:.0f}% retrace)\n"
                "  Stop   : ${:.4f}\n"
                "```\nWatching for **{}** to layer in...".format(
                DISPLAY_NAME, fill, vol,
                pos["target_px"], TARGET_RETRACE * 100,
                pos["stop_px"], ENTRY_SIGNAL.replace("_", " ")))

        elif curr_px > (pos.get("limit_px", 0) or 0) * 1.05:
            send_discord("❌ **FlashCatch {}** — Toe missed (price ran away). Back to watching.".format(DISPLAY_NAME))
            ps["status"]   = "watching"
            ps["position"] = None

    # ── TOE FILLED: watch for confirmation ────────────────────────────────────
    elif status == "toe_filled":
        pos       = ps.get("position", {})
        stop_px   = pos.get("stop_px", 0)
        target_px = pos.get("target_px", float("inf"))
        entry_ts  = datetime.fromisoformat(pos["entry_ts"]).replace(tzinfo=timezone.utc)
        elapsed_h = (now_utc - entry_ts).total_seconds() / 3600

        if curr_px <= stop_px:
            txid, _ = place_sell_market(pos.get("volume", 0))
            send_discord("🛑 **FlashCatch {}** — Stop hit ${:.4f}. Market exit.".format(DISPLAY_NAME, curr_px))
            record_trade(state, pos, curr_px, "stop")
            save_state(state)
            return

        if elapsed_h >= MAX_HOLD_HRS:
            place_sell_market(pos.get("volume", 0))
            send_discord("⏰ **FlashCatch {}** — {}h timeout. Exiting @ ${:.4f}.".format(
                DISPLAY_NAME, MAX_HOLD_HRS, curr_px))
            record_trade(state, pos, curr_px, "timeout")
            save_state(state)
            return

        if curr_px >= target_px:
            txid, err = place_sell_limit(target_px, pos.get("volume", 0))
            if not err:
                pos["sell_txid"] = txid
                ps["status"]     = "sell_placed"
                send_discord("🎯 **FlashCatch {}** — Target reached! Sell @ **${:.4f}**".format(
                    DISPLAY_NAME, target_px))
            save_state(state)
            return

        confirmed = False
        if len(ha) >= 2:
            if ENTRY_SIGNAL == "hammer":
                confirmed = is_hammer(ha[-1])
            elif ENTRY_SIGNAL == "green_candle":
                confirmed = is_green(ha[-1]) and not is_green(ha[-2])

        if confirmed:
            pair_budget = calc_pair_budget(state)
            layer_usd   = round(pair_budget * LAYER_PCT, 2)
            usd_avail   = fetch_usd_balance() or 0
            if usd_avail >= layer_usd:
                limit_px  = round(curr_px * 1.001, 4)
                txid, err = place_buy_limit(limit_px, layer_usd)
                if not err:
                    pos["layer_txid"]    = txid
                    pos["layer_usd"]     = layer_usd
                    pos["layer_limit_px"]= limit_px
                    ps["status"]         = "layered"
                    send_discord("📈 **FlashCatch {}** — {} confirmed! Layer placed @ **${:.4f}** (${:.2f})".format(
                        DISPLAY_NAME, ENTRY_SIGNAL.replace("_", " ").title(),
                        limit_px, layer_usd))
            else:
                send_discord("⚠️ **FlashCatch {}** — {} confirmed but only ${:.2f} available for layer (need ${:.2f}). Holding toe.".format(
                    DISPLAY_NAME, ENTRY_SIGNAL.replace("_"," ").title(), usd_avail, layer_usd))

        print("  [{}] Toe filled — price={:.2f}  target={:.2f}  stop={:.2f}  RSI={:.1f}  {:.1f}h held".format(
            DISPLAY_NAME, curr_px, target_px, stop_px, rsi, elapsed_h))

    # ── LAYERED: monitor full position ────────────────────────────────────────
    elif status == "layered":
        pos       = ps.get("position", {})
        stop_px   = pos.get("stop_px", 0)
        target_px = pos.get("target_px", float("inf"))
        entry_ts  = datetime.fromisoformat(pos["entry_ts"]).replace(tzinfo=timezone.utc)
        elapsed_h = (now_utc - entry_ts).total_seconds() / 3600

        # Check if layer order filled
        if pos.get("layer_txid") and not pos.get("layer_vol"):
            st, fill, vol = query_order(pos["layer_txid"])
            if st == "closed" and vol:
                pos["layer_vol"]   = vol
                pos["layer_fill"]  = fill
                send_discord("✅ **FlashCatch {}** — Layer filled @ **${:.4f}** ({:.6f} coins)".format(
                    DISPLAY_NAME, fill or 0, vol))

        total_vol = (pos.get("volume") or 0) + (pos.get("layer_vol") or 0)

        if curr_px <= stop_px:
            place_sell_market(total_vol)
            send_discord("🛑 **FlashCatch {}** — Stop hit ${:.4f}. Full exit ({:.6f} coins).".format(
                DISPLAY_NAME, curr_px, total_vol))
            cost     = (pos.get("usd_spent") or 0) + (pos.get("layer_usd") or 0)
            pnl      = curr_px * total_vol - cost
            record_trade(state, pos, curr_px, "stop", pnl=pnl)
            save_state(state)
            return

        if elapsed_h >= MAX_HOLD_HRS:
            place_sell_market(total_vol)
            send_discord("⏰ **FlashCatch {}** — {}h timeout. Full exit @ ${:.4f}.".format(
                DISPLAY_NAME, MAX_HOLD_HRS, curr_px))
            cost = (pos.get("usd_spent") or 0) + (pos.get("layer_usd") or 0)
            pnl  = curr_px * total_vol - cost
            record_trade(state, pos, curr_px, "timeout", pnl=pnl)
            save_state(state)
            return

        if curr_px >= target_px and total_vol > 0:
            txid, err = place_sell_limit(target_px, total_vol)
            if not err:
                pos["sell_txid"] = txid
                ps["status"]     = "sell_placed"
                send_discord("🎯 **FlashCatch {}** — Target! Full sell placed @ **${:.4f}** ({:.6f} coins)".format(
                    DISPLAY_NAME, target_px, total_vol))

        print("  [{}] Layered — price={:.2f}  target={:.2f}  stop={:.2f}  {:.1f}h held".format(
            DISPLAY_NAME, curr_px, target_px, stop_px, elapsed_h))

    # ── SELL PLACED: wait for fill ────────────────────────────────────────────
    elif status == "sell_placed":
        pos              = ps.get("position", {})
        st, fill, vol    = query_order(pos.get("sell_txid", ""))

        if st == "closed" and fill:
            total_vol = (pos.get("volume") or 0) + (pos.get("layer_vol") or 0)
            cost      = (pos.get("usd_spent") or 0) + (pos.get("layer_usd") or 0)
            pnl       = fill * total_vol - cost
            result_str = "WIN 🏆" if pnl > 0 else "LOSS"
            send_discord(
                "{} **FlashCatch {}** — Trade closed!\n"
                "```\n"
                "  Exit   : ${:.4f}\n"
                "  PnL    : ${:+.2f}\n"
                "  Result : {}\n"
                "  Wins   : {}   Losses: {}\n"
                "```".format(
                "✅" if pnl > 0 else "❌",
                DISPLAY_NAME, fill, pnl, result_str,
                get_pair_state(state).get("wins", 0) + (1 if pnl > 0 else 0),
                get_pair_state(state).get("losses", 0) + (1 if pnl <= 0 else 0),
            ))
            record_trade(state, pos, fill, "target", pnl=pnl)

    save_state(state)


# =============================================================================
#  ENTRY POINT
# =============================================================================

def main():
    print("\n" + "=" * 72)
    print("  FlashCatch — {} | Crash Bounce Catcher".format(DISPLAY_NAME))
    print("  Crash trigger : {:.0f}%  RSI floor : {}  Vol mult : {:.1f}x".format(
        CRASH_TRIGGER, RSI_OVERSOLD, VOL_MULT))
    print("  Toe : {:.0f}%   Layer : {:.0f}%   Target : {:.0f}% retrace   Max : {}h".format(
        TOE_PCT * 100, LAYER_PCT * 100, TARGET_RETRACE * 100, MAX_HOLD_HRS))
    print("  Poll : {}s   Signal : {}".format(POLL_SECS, ENTRY_SIGNAL))
    print("=" * 72 + "\n")

    if not LIVE_TRADING_ENABLED:
        print("  [SAFE MODE] live_trading_enabled is false. No live orders will be placed.")
        return

    _init_db()
    load_api_keys()

    state  = load_state()
    budget = calc_pair_budget(state)
    print("  [BUDGET] {} allocation: ${:.2f}  (wins={} losses={})".format(
        DISPLAY_NAME, budget,
        state.get("pairs", {}).get(PAIR, {}).get("wins", 0),
        state.get("pairs", {}).get(PAIR, {}).get("losses", 0),
    ))

    def _loop():
        while True:
            try:
                run_cycle(state)
            except KeyboardInterrupt:
                print("\n  [FlashCatch] Stopped by user.")
                sys.exit(0)
            except Exception as e:
                import traceback
                print("  [ERROR] {}".format(e))
                traceback.print_exc()
            time.sleep(POLL_SECS)

    try:
        run_supervised(_loop)
    except Exception:
        _loop()


if __name__ == "__main__":
    main()
