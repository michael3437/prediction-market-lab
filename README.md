# Prediction Market Lab

Tools for collecting and analysing prediction market data from Kalshi.

## Description

A suite for collection and analysis of data from all markets on Kalshi. Measures prediction accuracy using Brier scores across a range of categories of markets, with options for slicing by volume, OI, or liquidity. Will take orderbook snapshots for active markets to be used for future analysis and investigating if orderbook features provide insight into the final outcome probability. Equipped with a full data pipeline storing market data and categorising it, as well as candlesticks for price history.

## Installation

```bash
git clone https://github.com/michael3437/prediction-market-lab.git
cd prediction-market-lab
uv sync

# Configure Kalshi API credentials
echo "KEYID=your_kalshi_key_id" > .env
# Place your RSA private key in apikey.txt in project root
```

## Usage

```bash
prediction-market <command>
```

| Command | Description |
|---------|-------------|
| `sync-markets` | Fetch and store settled markets from Kalshi |
| `sync-candles` | Download OHLC price history for synced markets |
| `brier-score` | Calculate forecast accuracy at various time intervals |
| `interactive` | Launch an interactive shell with API client |