# Strategies

These are descriptions of the experimental logic implemented in the repository. They are not recommendations or claims of expected returns.

## SwingTrader

SwingTrader is a multi-day, spot-only strategy. It evaluates several technical signals across multiple timeframes and attempts to enter pullbacks around areas such as swing lows and moving averages.

The implementation includes indicators and features such as EMA, RSI, MACD, moving averages, Heikin Ashi candles, ATR, OBV, VWAP, divergences, and support/swing analysis. Exit management includes targets, trailing stops, order expiry, and re-entry cooldowns.

The strategy was designed around short swing horizons rather than long-term portfolio allocation.

## FlashCatch

FlashCatch looks for unusually large short-term declines combined with oversold and volume conditions. It uses staged entry logic and predefined target, stop, and maximum-hold conditions.

## SentinelHODL

Sentinel is a Bitcoin macro-cycle model. It combines market-cycle indicators and external sentiment/on-chain inputs into a 0–100 heuristic score and surfaces signals for manual review.

It does not place exchange orders.

## WatchlistGenerator

The scanner ranks Kraken USD pairs using liquidity, spread, volatility, trend, RSI, and swing/reversal characteristics. The resulting composite score is intended to narrow a large market universe into a smaller research list.

## Strategy limitations

The code encodes assumptions about historical market behavior. Those assumptions may fail. The repository should therefore be treated as an engineering and research project rather than evidence that any strategy is profitable.
