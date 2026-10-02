# Configuration

The repository uses example configuration files so a clone can be configured without exposing the author's credentials or runtime state.

## Credentials

Copy:

```bash
cp config/keys.example.json config/keys.json
```

Then enter your own Kraken API key and secret in the local file. Never commit the resulting file.

## Discord webhooks

Copy:

```bash
cp config/webhooks.example.json config/webhooks.json
```

Replace the placeholder URLs with your own webhook URLs. `webhooks.json` is ignored by Git.

## SwingTrader

Copy both:

```bash
cp config/swingtrader.example.json config/swingtrader_config.json
cp config/pairs.example.json config/pairs.json
```

The public example has live trading disabled. To deliberately test live execution, set `live_trading_enabled` to `true` in your local configuration and ensure your API key has the permissions required by the bot.

## FlashCatch

Copy the example to the filename you want to use:

```bash
cp config/flashcatch.example.json config/flashcatch_ZECUSD_config.json
```

FlashCatch also defaults to safe mode. A local configuration must explicitly enable live trading.

## SentinelHODL

Sentinel is a signal/research tool and does not place exchange orders. The public example is configured for BTC because the model's cycle indicators are specifically designed around Bitcoin's macro-cycle data.

## Runtime state

Runtime databases, CSV files, JSON state files, logs, and generated watchlists are excluded from source control.
