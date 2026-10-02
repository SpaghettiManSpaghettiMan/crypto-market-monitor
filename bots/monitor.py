"""
monitor.py -- Drop-in monitoring for SwingTrader.py
Place in the bots/ directory of this repository

Features:
  1. Heartbeat file -- updated every loop cycle
  2. Crash logging -- full tracebacks with timestamps to crash.log
  3. Discord webhook -- push notifications for:
     - Startup / shutdown / crash / recovery
     - Periodic status summary (configurable interval)
     - Trade events (fills, exits, sell triggers)

Setup:
  1. Copy this file into your KRAKENMONITOR folder
  2. Create a Discord webhook:
     - Server -> Channel -> Edit -> Integrations -> Webhooks -> New Webhook
     - Copy the webhook URL
  3. Copy config/webhooks.example.json to config/webhooks.json and add your local webhook URLs
  4. Run the bot from the repository root or use the documented commands
"""

import os
import re
import time
import traceback
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import urllib.request
except ImportError:
    pass

# ----------------------------------------------
# CONFIG
# ----------------------------------------------

# Webhook URL and Discord interval load from swingtrader_config.json.
# Webhook filename is read from config — the actual URL lives in that file
# and is never exposed in code or config JSON.
DISCORD_WEBHOOK_URL = ""
STATUS_INTERVAL_MIN = 45     # default; overridden by swingtrader_config.json


def _resolve_webhook(webhook_name, config_dir):
    """
    Look up a webhook name in webhooks.json, return the URL string.

    Supports three formats in webhooks.json:
      1. {"key": {"note": "...", "url": "https://..."}}   <- new unified format
      2. {"key": "https://..."}                           <- URL string directly
      3. {"key": "filename.txt"}                          <- old file-reference format

    Falls back to webhook_url.txt if webhooks.json is missing or name not found.
    """
    webhooks_path = config_dir / "webhooks.json"
    secrets_path  = config_dir.parent / "secrets" / "webhooks.json"
    env_secrets   = os.environ.get("SWINGTRADER_SECRETS_DIR", "")
    env_path      = Path(env_secrets) / "webhooks.json" if env_secrets else None

    # Priority: env var → KRAKENMONITOR/secrets/ → KRAKENMONITOR/config/
    candidates = [p for p in [env_path, secrets_path, webhooks_path] if p]
    for wh_path in candidates:
        if not wh_path.exists():
            continue
        try:
            registry = json.loads(wh_path.read_text())
            entry = registry.get(webhook_name)
            if not entry:
                continue
            # Format 1: dict with 'url' key
            if isinstance(entry, dict):
                url = entry.get("url", "").strip()
                if url and url.startswith("http"):
                    return url
            # Format 2: bare URL string
            if isinstance(entry, str) and entry.startswith("http"):
                return entry.strip()
            # Format 3: filename reference (legacy)
            if isinstance(entry, str) and not entry.startswith("PASTE"):
                url_path = config_dir / entry
                if url_path.exists():
                    url = url_path.read_text().strip()
                    if url and url.startswith("http"):
                        return url
        except Exception:
            pass

    # Final fallback: webhook_url.txt
    fallback = config_dir / "webhook_url.txt"
    if fallback.exists():
        url = fallback.read_text().strip()
        if url:
            return url
    return ""


def _resolve_webhook_interval(webhook_name, config_dir):
    """
    Return the interval_mins for a webhook entry from webhooks.json (secrets/ first, then config/).
    Returns None if not set — caller should warn and leave interval unchanged.
    """
    webhooks_path = config_dir / "webhooks.json"
    secrets_path  = config_dir.parent / "secrets" / "webhooks.json"
    env_secrets   = os.environ.get("SWINGTRADER_SECRETS_DIR", "")
    env_path      = Path(env_secrets) / "webhooks.json" if env_secrets else None
    candidates    = [p for p in [env_path, secrets_path, webhooks_path] if p]
    for wh_path in candidates:
        if not wh_path.exists():
            continue
        try:
            registry = json.loads(wh_path.read_text())
            entry = registry.get(webhook_name)
            if isinstance(entry, dict) and "interval_mins" in entry:
                return int(entry["interval_mins"])
        except Exception:
            pass
    return None


def _load_monitor_config():
    global DISCORD_WEBHOOK_URL, STATUS_INTERVAL_MIN
    cfg_path   = Path(__file__).parent / "config" / "swingtrader_config.json"
    config_dir = Path(__file__).parent / "config"
    if cfg_path.exists():
        try:
            cfg          = json.loads(cfg_path.read_text())
            webhook_name = cfg.get("discord", {}).get("webhook_name", "swingtrader")
            DISCORD_WEBHOOK_URL = _resolve_webhook(webhook_name, config_dir)
            # interval comes exclusively from webhooks.json — no fallback
            wh_interval = _resolve_webhook_interval(webhook_name, config_dir)
            if wh_interval is not None:
                STATUS_INTERVAL_MIN = wh_interval
            else:
                print(f"[monitor] WARNING: interval_mins not set for '{webhook_name}' in webhooks.json — status updates disabled")
        except Exception:
            pass
    if not DISCORD_WEBHOOK_URL:
        # last-resort: try the old webhook_url.txt directly
        fallback = config_dir / "webhook_url.txt"
        if fallback.exists():
            DISCORD_WEBHOOK_URL = fallback.read_text().strip()


_load_monitor_config()

HEARTBEAT_FILE      = "logs/heartbeat.txt"
CRASH_LOG_FILE      = "logs/crash.log"
RESTART_DELAY_SEC   = 60
MAX_CRASH_LOG_MB    = 10


# ----------------------------------------------
# SESSION TRACKER
# ----------------------------------------------
class SessionTracker:
    """Tracks session duration and realized P/L across the session."""

    def __init__(self):
        self.start_time = datetime.now(timezone.utc)
        self.realized = {}       # pair -> total realized USD
        self.trade_count = {}    # pair -> number of completed trades
        self.wins = 0
        self.losses = 0
        self.volume = {}
        self.total_volume = 0
        self.starting_usd = None

    def record_trade(self, pair, pnl_usd, pnl_pct, usd_allocated=0):
        """Call after a sell completes to accumulate realized P/L."""
        self.realized[pair] = self.realized.get(pair, 0) + pnl_usd
        self.trade_count[pair] = self.trade_count.get(pair, 0) + 1
        self.volume[pair] = self.volume.get(pair, 0) + usd_allocated
        self.total_volume += usd_allocated
        if pnl_pct >= 0:
            self.wins += 1
        else:
            self.losses += 1

    def set_starting_usd(self, bal):
        if self.starting_usd is None:
            self.starting_usd = bal

    def set_start_time(self, dt):
        """Backdate session start to the earliest carried position."""
        self.start_time = dt

    def duration_str(self):
        elapsed = datetime.now(timezone.utc) - self.start_time
        hours, rem = divmod(int(elapsed.total_seconds()), 3600)
        mins, _ = divmod(rem, 60)
        return "{}h {}m".format(hours, mins)

    def total_realized(self):
        return sum(self.realized.values())

    def total_trades(self):
        return self.wins + self.losses

    def win_rate(self):
        total = self.total_trades()
        if total == 0:
            return 0
        return self.wins / total * 100

    def load_history(self, csv_path, since_dt):
        """Load completed trades from CSV that fall within this session."""
        import csv
        if not os.path.exists(csv_path):
            return
        try:
            with open(csv_path, "r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    exit_ts = row.get("exit_timestamp", "")
                    if not exit_ts:
                        continue
                    try:
                        exit_dt = datetime.fromisoformat(exit_ts)
                        if exit_dt.tzinfo is None:
                            exit_dt = exit_dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        continue
                    if exit_dt >= since_dt:
                        pair = row.get("pair", "?")
                        pnl_str = row.get("pnl_usd", "0").replace("+", "")
                        pnl_pct_str = row.get("pnl_pct", "0").replace("%", "").replace("+", "")
                        alloc_str = row.get("usd_allocated", "0")
                        try:
                            pnl_usd = float(pnl_str)
                            pnl_pct = float(pnl_pct_str)
                            alloc = float(alloc_str)
                        except ValueError:
                            continue
                        self.record_trade(pair, pnl_usd, pnl_pct, alloc)
        except Exception as e:
            print("  [monitor] Failed to load trade history: {}".format(e))

    def summary_lines(self):
        """Return formatted lines for the Discord status header."""
        lines = []
        total_r = self.total_realized()
        total_t = self.total_trades()
        if total_t > 0:
            emoji = ":moneybag:" if total_r >= 0 else ":small_red_triangle_down:"
            lines.append("{} Realized P/L: **{:+,.2f} USD** | {} trade{} | {:.0f}% win rate".format(
                emoji, total_r, total_t, "s" if total_t > 1 else "", self.win_rate()))
            for pair in sorted(self.realized.keys()):
                r = self.realized[pair]
                c = self.trade_count[pair]
                pemo = ":green_circle:" if r >= 0 else ":red_circle:"
                v = self.volume.get(pair, 0)
                pr = (r / v * 100) if v else 0
                lines.append("   {} {} {:+,.2f} USD ({:+.1f}%) | {} trade{} | ${:,.2f} vol".format(
                    pemo, pair, r, pr, c, "s" if c > 1 else "", v))
        else:
            lines.append("No completed trades yet")
        return lines


session = SessionTracker()


# ----------------------------------------------
# HEARTBEAT
# ----------------------------------------------
class Heartbeat:
    def __init__(self, filepath=HEARTBEAT_FILE):
        self.filepath = Path(filepath)
        self.cycle_count = 0

    def pulse(self, status="running", extra=None):
        self.cycle_count += 1
        data = {
            "last_pulse": datetime.now().isoformat(),
            "status": status,
            "uptime_cycles": self.cycle_count,
            "pid": os.getpid(),
        }
        if extra:
            data.update(extra)
        try:
            self.filepath.write_text(json.dumps(data, indent=2))
        except Exception:
            pass

    def is_stale(self, max_age_minutes=20):
        if not self.filepath.exists():
            return True
        try:
            info = json.loads(self.filepath.read_text())
            last = datetime.fromisoformat(info["last_pulse"])
            return datetime.now() - last > timedelta(minutes=max_age_minutes)
        except Exception:
            return True


# ----------------------------------------------
# CRASH LOGGER
# ----------------------------------------------
class CrashLogger:
    def __init__(self, filepath=CRASH_LOG_FILE, max_size_mb=MAX_CRASH_LOG_MB):
        self.filepath = Path(filepath)
        self.max_size_bytes = max_size_mb * 1024 * 1024

    def log(self, exception):
        self._rotate_if_needed()
        entry = [
            "",
            "=" * 60,
            "CRASH at {}".format(datetime.now().isoformat()),
            "PID: {}".format(os.getpid()),
            "Exception: {}: {}".format(type(exception).__name__, exception),
            "-" * 60,
            traceback.format_exc(),
            "=" * 60,
        ]
        with open(self.filepath, "a", encoding="utf-8") as f:
            f.write("\n".join(entry) + "\n")

    def _rotate_if_needed(self):
        if self.filepath.exists() and self.filepath.stat().st_size > self.max_size_bytes:
            rotated = self.filepath.with_suffix(".old.log")
            if rotated.exists():
                rotated.unlink()
            self.filepath.rename(rotated)


# ----------------------------------------------
# DISCORD -- low level
# ----------------------------------------------
def send_discord(message, webhook_url=None):
    """Send message to Discord, splitting into chunks if over 1950 chars."""
    url = webhook_url or DISCORD_WEBHOOK_URL
    if not url:
        return
    url = url.replace("discordapp.com", "discord.com")

    # Split on blank lines to avoid cutting mid-block
    def _chunks(text, limit=1950):
        chunks = []
        current = []
        for line in text.split("\n"):
            if sum(len(l) + 1 for l in current) + len(line) > limit and current:
                chunks.append("\n".join(current))
                current = [line]
            else:
                current.append(line)
        if current:
            chunks.append("\n".join(current))
        return chunks

    for chunk in _chunks(message):
        try:
            payload = json.dumps({
                "content": chunk,
                "username": "KrakenBot",
            }).encode("utf-8")
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json",
                         "User-Agent": "KrakenBot/1.0"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
            time.sleep(0.5)   # avoid Discord rate limit between chunks
        except Exception as e:
            print("  [monitor] Discord send failed: {}".format(e))


# ----------------------------------------------
# DISCORD -- lifecycle alerts
# ----------------------------------------------
def notify_startup():
    send_discord(
        ":green_circle: **SwingTrader STARTED**\n"
        "**Time:** {}\n"
        "**PID:** {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), os.getpid()))


def notify_shutdown(reason="user"):
    send_discord(
        ":octagonal_sign: **SwingTrader STOPPED** ({})\n"
        "**Time:** {}".format(
            reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def notify_crash(exception):
    tb = traceback.format_exc()
    if len(tb) > 800:
        tb = tb[:400] + "\n...\n" + tb[-400:]
    send_discord(
        ":rotating_light: **SwingTrader CRASHED**\n"
        "**Time:** {}\n"
        "**Error:** `{}: {}`\n"
        "```\n{}\n```\n"
        "Restarting in {}s...".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            type(exception).__name__, exception,
            tb, RESTART_DELAY_SEC))


def notify_recovery(crash_count):
    send_discord(
        ":white_check_mark: **SwingTrader RECOVERED**\n"
        "**Time:** {}\n"
        "**Total crashes this session:** {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), crash_count))


# ----------------------------------------------
# DISCORD -- trade event alerts
# ----------------------------------------------
def notify_buy(pair, price, usd_alloc, support_label=""):
    """Call when an order fills (status -> filled)."""
    send_discord(
        ":chart_with_upwards_trend: **BUY FILLED -- {}**\n"
        "**Price:** ${:,.4f}\n"
        "**Allocated:** ${:,.2f}\n"
        "**Support:** {}".format(
            pair, price, usd_alloc, support_label or "--"))


def notify_sell(pair, price, fill_price, pnl_pct, pnl_usd, exit_type, fraction=1.0, usd_allocated=0):
    """
    Call when a sell triggers. Sends Discord alert only.
    Trade counting is handled exclusively by load_history() reading the CSV —
    removing the record_trade() call here prevents every trade being counted twice
    (once live, once on the next restart when load_history re-reads the CSV).
    """
    emoji = ":moneybag:" if pnl_pct >= 0 else ":small_red_triangle_down:"
    partial = " ({:.0f}%)".format(fraction * 100) if fraction < 1.0 else ""
    send_discord(
        "{} **{} -- {}{}**\n"
        "**Exit price:** ${:,.2f}\n"
        "**Entry price:** ${:,.2f}\n"
        "**P/L:** {:+.2f}% ({:+,.2f} USD)".format(
            emoji, exit_type.replace("_", " "), pair, partial,
            price, fill_price, pnl_pct, pnl_usd))


# ----------------------------------------------
# DISCORD -- periodic status summary
# ----------------------------------------------
_last_status_time = None


def should_send_status():
    """Returns True if enough time has passed since last status update."""
    global _last_status_time
    now = datetime.now()
    if _last_status_time is None:
        _last_status_time = now
        return True
    elapsed = (now - _last_status_time).total_seconds() / 60
    if elapsed >= STATUS_INTERVAL_MIN:
        _last_status_time = now
        return True
    return False


def _rsi_bar(rsi, width=8):
    """RSI progress bar for inline display."""
    if rsi is None:
        return "░" * width
    filled = min(width, round(rsi / 100 * width))
    return "█" * filled + "░" * (width - filled)


def _pnl_bar(pnl_pct, width=10):
    """Centred P&L bar. 2% per block."""
    mid    = width // 2
    filled = min(mid, int(abs(pnl_pct) / 2))
    if pnl_pct >= 0:
        bar = "─" * mid + "█" * filled + "░" * (mid - filled)
    else:
        bar = "░" * (mid - filled) + "█" * filled + "─" * mid
    return "[{}]".format(bar)


def _parse_rsi(analysis_line):
    """Extract RSI float from cached analysis string e.g. 'RSI 44.2 [OVERSOLD]'."""
    if not analysis_line:
        return None
    m = re.search(r"RSI\s+([\d.]+)", analysis_line)
    return float(m.group(1)) if m else None


def _fmt_usd(value):
    """Format a float as $1,234.56 with commas."""
    return "${:,.2f}".format(value)


def _trim_price_decimals(text, decimals=2):
    """
    Round any $X.XXXX prices and parenthesised decimals (MACD etc) to N decimal places.
    Preserves comma-thousands separators in prices.
    Also cleans up internal underscores (e.g. swing_low → swing low).
    """
    if not text:
        return text
    # $1,234.5678 → $1,234.57  (with commas preserved)
    text = re.sub(
        r"\$(\d[\d,]*\.\d{3,})",
        lambda m: _fmt_usd(float(m.group(1).replace(",", ""))),
        text
    )
    # (+0.8697) or (-13.6158) → (+0.87) or (-13.62)
    text = re.sub(
        r"(\([+-]?\d+\.\d{3,}\))",
        lambda m: "({:.{}f})".format(float(m.group(1).strip("()")), decimals),
        text
    )
    # bare floats with 3+ decimal places (skip RSI values like 35.4 which are 1dp)
    text = re.sub(
        r"(?<!\$)(?<!\d)(\d+\.\d{3,})(?!\d)",
        lambda m: "{:.{}f}".format(float(m.group(1)), decimals),
        text
    )
    # clean underscores in labels like swing_low, 50_MA etc.
    text = text.replace("swing_low", "swing low").replace("_low", " low")
    return text


def _c(line):
    return line.strip() if line else ""


def send_status_summary(state, prices, usd_bal, active_pairs):
    """Build and send a formatted status summary to Discord."""
    if not DISCORD_WEBHOOK_URL:
        return
    if not should_send_status():
        return

    import sys
    now_utc = datetime.now(timezone.utc)
    _base   = getattr(sys.modules.get("__main__"), "_pair_budget_base", 0)

    position_blocks = []
    watching_blocks = []
    total_pnl_usd   = 0.0
    active_count     = 0
    locked           = 0.0

    for pair in sorted(active_pairs):
        ps     = state.get(pair, {})
        price  = prices.get(pair, 0)
        status = ps.get("status", "watching")

        # Cached analysis — strip label prefixes, trim prices to 2dp
        st  = _trim_price_decimals(_c(ps.get("_analysis_st", "")).replace("ST (4h) : ", ""))
        lt  = _trim_price_decimals(_c(ps.get("_analysis_lt", "")).replace("LT (Wk) : ", ""))
        out = _trim_price_decimals(_c(ps.get("_analysis_out", "")).replace("OUTLOOK  : ", ""))
        bt  = _trim_price_decimals(_c(ps.get("_bear_timer", "")))
        _raw_why = _c(ps.get("_last_reason") or "scanning for swing setup")
        why = _trim_price_decimals(_raw_why)
        # Capitalise first letter for consistent presentation
        why = why[:1].upper() + why[1:] if why else why

        if status == "watching":
            pair_bgt = _base + session.realized.get(pair, 0)
            cd = ""
            if ps.get("last_exit_time"):
                exit_dt = datetime.fromisoformat(ps["last_exit_time"])
                mins    = (now_utc - exit_dt).total_seconds() / 60
                if mins < 60:
                    cd = "  ⏳ {:.0f}m cooldown".format(60 - mins)

            lines = [
                "👁  **{pair}**  ${price:,.2f}  |  alloc ${bgt:,.2f}{cd}".format(
                    pair=pair, price=price, bgt=pair_bgt, cd=cd),
            ]
            if bt:
                lines.append("  📉 _{}_".format(bt))
            if st:
                lines.append("  📊 {}".format(st))
            if lt:
                lines.append("  📅 {}".format(lt))
            if out:
                lines.append("  {}".format(out))
            lines.append("  ✋ _{}_".format(why))
            stats = ps.get("_order_stats", {})
            if stats:
                lines.append("  📊 Orders: {} placed  /  {} cancelled  /  {} filled".format(
                    stats.get("placed", 0),
                    stats.get("cancelled", 0),
                    stats.get("filled", 0)))

            watching_blocks.append("\n".join(lines))

        elif status == "order_open":
            ep  = ps.get("entry_price") or 0
            sup = ps.get("entry_support_label", "?")
            watching_blocks.append(
                "⏳  **{}**  ${:,.2f}  BUY PENDING @ ${:,.2f}  ({})".format(
                    pair, price, ep, sup))

        elif status == "filled":
            active_count += 1
            fp      = ps.get("fill_price") or 0
            alloc   = ps.get("usd_allocated") or 0
            locked += alloc
            pnl_pct = (price - fp) / fp * 100 if fp and price else 0
            pnl_usd = alloc * ((price - fp) / fp) if fp and price and alloc else 0
            total_pnl_usd += pnl_usd
            fill_ts = ps.get("fill_timestamp", "")
            fill_dt = datetime.fromisoformat(fill_ts) if fill_ts else now_utc
            days    = (now_utc - fill_dt).total_seconds() / 86400
            armed   = ps.get("trailing_stop_armed", False)
            tp_tier = ps.get("tp_tier", 0)
            hwm     = ps.get("high_water_mark") or fp
            tp1     = ps.get("atr_tp1_pct", 0.10) * 100
            tp2     = ps.get("atr_tp2_pct", 0.15) * 100
            tp3     = ps.get("atr_tp3_pct", 0.20) * 100
            ts_pct  = ps.get("atr_ts_pct", 0.045) * 100
            layers  = ps.get("layers_added", 0)
            dot     = "🟢" if pnl_pct >= 0 else "🔴"
            ts_tag  = "  🔒 TS({:.0f}%)".format(ts_pct) if armed else ""
            tp_tag  = "  T{}/3".format(tp_tier) if tp_tier > 0 else ""
            lay_tag = "  +{}L".format(layers) if layers > 0 else ""

            sig = ps.get("signal_data") or {}
            lines = [
                "{dot}  **{pair}**  ${price:,.2f}  |  P&L **{pct:+.2f}%** ({usd:+,.2f} USD)  "
                "held {days:.1f}d  HWM ${hwm:,.2f}{ts}{tp}{lay}".format(
                    dot=dot, pair=pair, price=price,
                    pct=pnl_pct, usd=pnl_usd, days=days, hwm=hwm,
                    ts=ts_tag, tp=tp_tag, lay=lay_tag),
                "  📥 Entry ${fp:,.2f}  |  ${alloc:,.2f} deployed  |  "
                "TP {tp1:.0f}% / {tp2:.0f}% / {tp3:.0f}%  |  TS -{ts:.1f}%".format(
                    fp=fp, alloc=alloc, tp1=tp1, tp2=tp2, tp3=tp3, ts=ts_pct),
            ]
            if sig:
                lines.append(
                    "  Entry: {} support ${sup:,.2f}  |  "
                    "4h RSI {r4:.1f}  Daily RSI {rd:.1f}".format(
                        ps.get("entry_support_label", ""),
                        sup=ps.get("entry_support_level") or 0,
                        r4=sig.get("rsi_4h") or 0,
                        rd=sig.get("rsi_daily") or 0))
            code  = ps.get("entry_code", "")
            human = ps.get("entry_human", "")
            if code:
                lines.append("  📋 {} — {}".format(code, human))
            stats = ps.get("_order_stats", {})
            if stats:
                lines.append("  📊 Orders: {} placed  /  {} cancelled  /  {} filled".format(
                    stats.get("placed", 0),
                    stats.get("cancelled", 0),
                    stats.get("filled", 0)))
            if st:
                lines.append("  📊 {}".format(st))
            if out:
                lines.append("  {}".format(out))

            position_blocks.append("\n".join(lines))

        elif status == "sell_pending":
            fp     = ps.get("fill_price") or 0
            pnl_pct = (price - fp) / fp * 100 if fp and price else 0
            rs     = (ps.get("sell_reason") or "?").replace("_", " ").upper()
            partial = " (PARTIAL)" if ps.get("sell_is_partial") else ""
            position_blocks.append(
                "📤  **{}**  ${:,.2f}  SELL PENDING{}  P&L {pct:+.2f}%  reason: {}".format(
                    pair, price, partial, rs, pct=pnl_pct))

    # ── Send ─────────────────────────────────────────────────────────────────
    send_discord("📈  **SWING TRADER**  |  {}".format(
        now_utc.strftime("%Y-%m-%d %H:%M UTC")))

    if position_blocks:
        send_discord("**── OPEN POSITIONS ──**\n\n" +
                     "\n\n".join(position_blocks))

    if watching_blocks:
        send_discord("**── WATCHING ({}) ──**\n\n".format(len(watching_blocks)) +
                     "\n\n".join(watching_blocks))

    # ── Footer ────────────────────────────────────────────────────────────────
    footer = []
    if active_count > 0:
        pnl_e   = "💰" if total_pnl_usd >= 0 else "📉"
        total_deployed = locked if locked else 1
        unreal_pct = total_pnl_usd / total_deployed * 100 if total_deployed else 0
        footer.append("{} **Unrealized: {:+,.2f} USD  ({:+.2f}%)**  ({} position{})".format(
            pnl_e, total_pnl_usd, unreal_pct, active_count, "s" if active_count > 1 else ""))
    if session.total_trades() > 0:
        footer.extend(session.summary_lines())

    cash_str  = "${:,.2f}".format(usd_bal) if usd_bal is not None else "?"
    start_str = "Start ${:,.2f}  |  ".format(session.starting_usd) if session.starting_usd else ""
    footer.append("─────────────────────────")
    footer.append("{}Cash {}  |  Deployed ${:,.2f}  |  Vol ${:,.2f}".format(
        start_str, cash_str, locked, session.total_volume + locked))
    footer.append("Session {}  |  {}".format(
        session.start_time.strftime("%Y-%m-%d %H:%M"), session.duration_str()))
    send_discord("\n".join(footer))


# ----------------------------------------------
# SUPERVISED RUNNER
# ----------------------------------------------
def run_supervised(main_func, restart_on_crash=True):
    """
    Wraps main() with crash logging, heartbeat, and Discord alerts.
    Replaces the old watchdog loop at the bottom of SwingTrader.py.
    """
    crash_logger = CrashLogger()
    crash_count  = 0

    notify_startup()

    while True:
        try:
            main_func()
            print("[monitor] main() returned cleanly.")
            notify_shutdown("clean exit")
            break

        except KeyboardInterrupt:
            print("\n[monitor] Stopped by user (Ctrl+C).")
            notify_shutdown("user")
            break

        except Exception as e:
            crash_count += 1
            crash_logger.log(e)
            notify_crash(e)

            print("\n[monitor] CRASH #{}: {}: {}".format(
                crash_count, type(e).__name__, e))
            print("[monitor] Traceback written to {}".format(CRASH_LOG_FILE))

            if not restart_on_crash:
                break

            print("[monitor] Restarting in {}s...".format(RESTART_DELAY_SEC))
            time.sleep(RESTART_DELAY_SEC)
            notify_recovery(crash_count)


# ----------------------------------------------
# INTEGRATION GUIDE
# ----------------------------------------------
"""
In SwingTrader.py, make these changes:

==========================================
STEP 1 -- Add import (near the top, after existing imports)
==========================================

    from monitor import (run_supervised, Heartbeat, send_status_summary,
                         notify_buy, notify_sell)

    heartbeat = Heartbeat()

==========================================
STEP 2 -- Add heartbeat + status to main loop
==========================================

    In the main() function, find the line:

        print_header(now_local, prices, usd_bal, state, active_pairs, pairs_config,
                     first_cycle=first_cycle)

    Right AFTER that, add:

        heartbeat.pulse(extra={"positions": sum(
            1 for p in active_pairs if state[p].get("status") == "filled")})
        send_status_summary(state, prices, usd_bal, active_pairs)

==========================================
STEP 3 -- Replace the __main__ block
==========================================

    Delete everything from:
        if __name__ == "__main__":
    to the end of the file.

    Replace with:
        if __name__ == "__main__":
            run_supervised(main)

==========================================
OPTIONAL -- Trade alerts (buy fills & sells)
==========================================

    These go inside process_pair(). I can add them for you
    if you want -- just ask and I'll patch the file directly.
"""
