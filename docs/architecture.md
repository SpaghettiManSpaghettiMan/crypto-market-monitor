# Architecture

The repository separates strategy code, configuration, runtime state, and operational monitoring.

## Directory layout

```text
bots/       executable Python components
config/     public examples; local secrets/configs are ignored
data/       runtime state and generated analysis
logs/       runtime logs
```

## Shared monitoring

`monitor.py` provides:

- heartbeat updates
- crash logging
- restart supervision
- Discord startup/shutdown/crash/recovery notifications
- periodic status reporting
- trade-event notifications

The trading components import these capabilities rather than maintaining separate watchdog implementations.

## SwingTrader flow

1. Load local configuration.
2. Verify live trading is explicitly enabled.
3. Confirm configured pairs.
4. Load credentials only after the safety gate.
5. Resolve Kraken pair mappings.
6. Retrieve multi-timeframe market data.
7. Evaluate the configured entry/exit logic.
8. Maintain local state and trade history.
9. Place, monitor, cancel, or close spot orders as required.
10. Report operational events through the monitoring layer.

## FlashCatch flow

1. Load a pair-specific configuration.
2. Stop immediately when live trading is disabled.
3. Load credentials and state.
4. Monitor 1-hour candles for the configured crash conditions.
5. Stage entry and confirmation logic.
6. Manage target, stop, and timeout exits.
7. Persist state and report events.

## SentinelHODL flow

Sentinel combines BTC market data and macro-cycle inputs into a heuristic score. It stores state and emits signals for manual review. It does not submit exchange orders.

## Watchlist flow

WatchlistGenerator retrieves Kraken USD pairs, applies liquidity/spread filters, calculates technical features, produces a composite score, and writes the ranked result to `data/watchlist.json`.

## Public-release changes

The public build intentionally removes machine-specific paths and historical patch files, moves runtime output under repository-relative directories, separates credentials from examples, and adds explicit safe-mode gates to live-capable components.
