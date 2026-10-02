# Crypto Market Monitor

Experimental Python tools for cryptocurrency market monitoring, signal generation, market scanning, and spot-trading automation against Kraken.

> **Important:** This repository is a technical/educational project, not financial advice and not a claim of profitability. The trading strategies were designed for experimentation and manual evaluation. Live-order functionality is disabled in the example configurations and requires the user to explicitly configure and enable it.

## Components

- **SwingTrader** — multi-day, spot-only swing-trading system with real-order capability when explicitly enabled.
- **FlashCatch** — flash-crash/rebound strategy using staged entry and predefined exits.
- **SentinelHODL** — BTC macro-cycle heuristic that produces signals for manual review; it does not place exchange orders.
- **WatchlistGenerator** — Kraken USD-pair scanner that filters and ranks markets for swing-trading suitability.
- **monitor.py** — shared heartbeat, crash logging, Discord notifications, and supervised execution support.

## Architecture

```text
                    Kraken Public API
                           |
            +--------------+--------------+
            |              |              |
       Watchlist       SwingTrader    FlashCatch
            |              |              |
            +--------------+--------------+
                           |
                      monitor.py
                           |
                      Discord webhook

       External macro data --> SentinelHODL --> signal/report
```

## Project history

The strategy concepts and operating goals were designed by the author. The implementation went through many iterations, including substantial LLM-assisted coding and repeated troubleshooting. That process exposed edge cases and blind spots that were not obvious in the initial versions.

This repository is a **clean public release**, not a dump of the private development history. Historical patch scripts and private runtime files are intentionally excluded.

The point of the project is the engineering workflow: turning a trading idea into a configurable system that retrieves market data, evaluates conditions, maintains state, reports events, and can execute spot orders when deliberately enabled. Strategy profitability is a separate question and is not represented as a result of this repository.

## Kraken API integration

The project uses Kraken's HTTP APIs for public market data and, where required, authenticated private endpoints.

The private API integration demonstrates:

- HTTP GET/POST requests
- URL-encoded request bodies
- JSON response parsing
- API-key authentication
- HMAC-SHA512 request signing
- nonce generation
- balance retrieval
- open-order/trade queries
- order placement
- order cancellation
- order-status queries
- API error handling
- request rate limiting

Discord webhooks are used for operational notifications and status reporting.

The project demonstrates **API integration and automation**, rather than API development as a standalone service.

## Safety and live trading

The repository intentionally separates public configuration examples from local secrets.

The example configurations for live-capable components contain:

```json
"live_trading_enabled": false
```

Before live execution, a user must create their own local configuration and credentials. The repository does not contain API keys, API secrets, webhook URLs, trading databases, or runtime history.

SwingTrader also requires an interactive `SWING` confirmation before entering its live loop.

### API permissions

If you choose to test live functionality, use the minimum Kraken API permissions required by the component. Do not expose API secrets in source control.

## Installation

Clone the repository, create a virtual environment if desired, and install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Copy the examples and edit the copies locally:

```bash
cp config/keys.example.json config/keys.json
cp config/webhooks.example.json config/webhooks.json
cp config/swingtrader.example.json config/swingtrader_config.json
cp config/pairs.example.json config/pairs.json
cp config/flashcatch.example.json config/flashcatch_ZECUSD_config.json
cp config/sentinel.example.json config/sentinel_btc_config.json
```

The resulting local files are ignored by Git.

See [`docs/configuration.md`](docs/configuration.md) for details.

## Running the tools

From the repository root:

```bash
python3 bots/WatchlistGenerator.py
python3 bots/FlashCatch.py --config config/flashcatch_ZECUSD_config.json
python3 bots/SentinelHODL.py --config config/sentinel_btc_config.json
python3 bots/SWINGTRADER.py
```

The first three commands are useful for inspecting behavior without enabling live trading. SwingTrader remains in safe mode unless its local configuration explicitly sets `live_trading_enabled` to `true`.

## Runtime files

Runtime state is written under `data/` and operational logs under `logs/`. These files are intentionally ignored by Git because they are machine-specific and can contain private trading history or operational information.

## Limitations

- This is personal experimental software, not production trading infrastructure.
- Market conditions can change independently of the assumptions encoded in the strategies.
- Backtesting and profitability claims are outside the scope of this repository.
- Exchange APIs can change and may reject requests for reasons not represented by the local logic.
- Running automated trading software involves financial and operational risk.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — system structure and data flow
- [`docs/api-integration.md`](docs/api-integration.md) — API authentication and integration details
- [`docs/configuration.md`](docs/configuration.md) — local configuration and secrets
- [`docs/strategies.md`](docs/strategies.md) — strategy descriptions and assumptions

## License

This project is released under the [MIT License](LICENSE).
