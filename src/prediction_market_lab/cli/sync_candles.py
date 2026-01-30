"""Fetch 1m, 1h, 1d candles for each market in db that does not yet have candles stored."""

import pandas as pd
from datetime import datetime, timezone, timedelta

from prediction_market_lab.client.keys import get_keys
from prediction_market_lab.client.clients import KalshiHttpClient
from prediction_market_lab.db.database import init_db


# (period_interval_minutes, time_window)
INTERVALS = [
    (1, timedelta(days=1)),       # 1min candles, 1 day back
    (60, timedelta(weeks=1)),     # 1hour candles, 1 week back
    (1440, timedelta(days=30)),   # 1day candles, 1 month back
]


def get_unsynced_markets_batch(con, limit: int) -> list[dict]:
    """Get a batch of markets that haven't had candles synced yet."""
    rows = con.execute("""
        SELECT ticker, close_time
        FROM markets
        WHERE candles_synced_at IS NULL
          AND status = 'finalized'
        ORDER BY close_time DESC
        LIMIT ?
    """, [limit]).fetchall()
    return [{"ticker": r[0], "close_time": r[1]} for r in rows]


def count_unsynced_markets(con) -> int:
    """Count markets that haven't had candles synced yet."""
    result = con.execute("""
        SELECT COUNT(*)
        FROM markets
        WHERE candles_synced_at IS NULL
          AND status = 'finalized'
    """).fetchone()
    return result[0]


def fetch_candles_for_markets(
    client: KalshiHttpClient,
    tickers: list[str],
    close_times: list[datetime],
    period_interval: int,
    window: timedelta,
) -> list[dict]:
    """Fetch candles for markets at a given interval."""
    min_close = min(close_times)
    max_close = max(close_times)
    start_ts = int((min_close - window).timestamp())
    end_ts = int(max_close.timestamp())

    tickers_str = ",".join(tickers)
    resp = client.batch_get_candlesticks(
        market_tickers=tickers_str,
        start_ts=start_ts,
        end_ts=end_ts,
        period_interval=period_interval,
    )

    # Flatten response into rows
    rows = []
    for market in resp.get("markets", []):
        ticker = market["market_ticker"]
        for c in market.get("candlesticks", []):
            rows.append({
                "ticker": ticker,
                "period_interval": period_interval,
                "end_period_ts": datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc),
                "price_open": c["price"].get("open"),
                "price_high": c["price"].get("high"),
                "price_low": c["price"].get("low"),
                "price_close": c["price"].get("close"),
                "volume": c["volume"],
                "open_interest": c["open_interest"],
            })
    return rows


def insert_candles(con, candles: list[dict]) -> int:
    """Insert candles into the candlesticks table."""
    if not candles:
        return 0
    df = pd.DataFrame(candles)
    con.execute("""
        INSERT INTO candlesticks (
            ticker, period_interval, end_period_ts,
            price_open, price_high, price_low, price_close,
            volume, open_interest
        )
        SELECT
            ticker, period_interval, end_period_ts,
            price_open, price_high, price_low, price_close,
            volume, open_interest
        FROM df
        ON CONFLICT DO NOTHING
    """)
    return len(df)


def mark_synced(con, tickers: list[str]) -> None:
    """Mark markets as having candles synced."""
    placeholders = ", ".join(["?"] * len(tickers))
    con.execute(f"""
        UPDATE markets
        SET candles_synced_at = CURRENT_TIMESTAMP
        WHERE ticker IN ({placeholders})
    """, tickers)


def sync_batch(client: KalshiHttpClient, con, markets: list[dict]) -> int:
    """Sync all intervals for a batch of markets. Returns total candles inserted."""
    tickers = [m["ticker"] for m in markets]
    close_times = [m["close_time"] for m in markets]

    all_candles = []
    for period_interval, window in INTERVALS:
        candles = fetch_candles_for_markets(
            client, tickers, close_times, period_interval, window
        )
        all_candles.extend(candles)

    count = insert_candles(con, all_candles)
    mark_synced(con, tickers)
    return count


def main():
    key_id, private_key = get_keys()
    client = KalshiHttpClient(key_id, private_key)
    con = init_db()

    total_unsynced = count_unsynced_markets(con)
    print(f"Found {total_unsynced} markets without candles")

    if total_unsynced == 0:
        con.close()
        return

    total_candles = 0
    total_markets = 0
    batch_num = 0

    while True:
        markets = get_unsynced_markets_batch(con, 6) # 6 at a time to stay under 10k candle limit
        if not markets:
            break

        batch_num += 1
        try:
            count = sync_batch(client, con, markets)
            total_candles += count
            total_markets += len(markets)
            remaining = total_unsynced - total_markets
            print(f"Batch {batch_num}: {count} candles for {len(markets)} markets ({remaining} remaining)")
        except Exception as e:
            print(f"Batch {batch_num} failed: {e}")
            break # Very recently closed markets have no available candles yet

    print(f"\nDone. Synced {total_markets} markets, {total_candles} candles total.")
    con.close()


if __name__ == "__main__":
    main()
