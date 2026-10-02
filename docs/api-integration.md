# API Integration

The project is an example of integrating a Python application with exchange and notification APIs.

## Kraken public API

Public endpoints are used for market information such as OHLC/candle data and asset-pair metadata. Requests are made over HTTPS and responses are parsed as JSON.

## Kraken private API

Private endpoints require an API key and HMAC-SHA512 authentication.

The general flow is:

1. Build the request payload.
2. Add a nonce.
3. URL-encode the payload.
4. Combine the nonce/payload representation with the endpoint path as required by Kraken's signing scheme.
5. Hash the encoded request data with SHA-256.
6. Sign the endpoint path plus hash using the API secret and HMAC-SHA512.
7. Base64-encode the resulting signature.
8. Send the API key and signature in HTTP headers.
9. Parse the JSON response and inspect the API error field.

The project uses authenticated calls for operations including balances, open-order queries, order placement, cancellation, and order-status queries.

## Rate limiting

The code uses simple request-spacing/rate-limiting logic to avoid issuing requests continuously without delay. This is a local control, not a guarantee that exchange-side rate limits cannot be reached.

## Discord webhooks

Discord webhooks are used for operational messages rather than as part of the trading decision itself. Webhook URLs are kept in a local ignored configuration file.

## Security

Never commit:

- Kraken API keys
- Kraken API secrets
- Discord webhook URLs
- runtime databases
- trading history containing private information
- local state files

The public repository contains only placeholders and example configuration.
