"""Fetch all markets from Kalshi and upsert into DuckDB."""

import time
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

from prediction_market_lab.client.keys import get_keys
from prediction_market_lab.client.clients import KalshiHttpClient
from prediction_market_lab.db.database import init_db


def fetch_all_markets(client: KalshiHttpClient, min_settled_ts: Optional[int] = None) -> list[dict]:
    """Paginate through all markets, keeping those with nonzero volume. Excludes MVE."""
    all_markets = []
    cursor = None

    while True:
        params = {"limit": 1000}
        params["status"] = "settled"
        params["mve_filter"] = "exclude"
        if min_settled_ts is not None:
            params["min_settled_ts"] = min_settled_ts
        if cursor:
            params["cursor"] = cursor

        resp = client.get_markets(**params)
        markets = resp.get("markets", [])
        all_markets.extend([m for m in markets if m["volume"] != 0])

        cursor = resp.get("cursor")
        if not cursor or not markets:
            break

        print(f"  fetched {len(all_markets)} markets...")
        if len(all_markets) >= 200000:
            break

    return all_markets


def upsert_markets(con, markets: list[dict]) -> int:
    """Upsert markets into DuckDB. Returns count of rows affected."""
    df = pd.DataFrame([
        {
            "ticker": m["ticker"],
            "event_ticker": m["event_ticker"],
            "market_type": m["market_type"],
            "created_time": m["created_time"],
            "updated_time": m.get("updated_time"),
            "open_time": m["open_time"],
            "close_time": m.get("close_time"),
            "latest_expiration_time": m.get("latest_expiration_time"),
            "status": m["status"],
            "volume": m["volume"],
            "volume_fp": m["volume_fp"],
            "result": m.get("result"),
            "open_interest": m["open_interest"],
            "open_interest_fp": m["open_interest_fp"],
            "settlement_ts": m.get("settlement_ts"),
        }
        for m in markets
    ])

    con.execute("""
        INSERT INTO markets (
            ticker, event_ticker, market_type,
            created_time, updated_time, open_time, close_time, latest_expiration_time,
            status, volume, volume_fp, result, open_interest, open_interest_fp,
            settlement_ts
        )
        SELECT
            ticker, event_ticker, market_type,
            created_time, updated_time, open_time, close_time, latest_expiration_time,
            status, volume, volume_fp, result, open_interest, open_interest_fp,
            settlement_ts
        FROM df
        ON CONFLICT (ticker) DO UPDATE SET
            event_ticker = EXCLUDED.event_ticker,
            market_type = EXCLUDED.market_type,
            created_time = EXCLUDED.created_time,
            updated_time = EXCLUDED.updated_time,
            open_time = EXCLUDED.open_time,
            close_time = EXCLUDED.close_time,
            latest_expiration_time = EXCLUDED.latest_expiration_time,
            status = EXCLUDED.status,
            volume = EXCLUDED.volume,
            volume_fp = EXCLUDED.volume_fp,
            result = EXCLUDED.result,
            open_interest = EXCLUDED.open_interest,
            open_interest_fp = EXCLUDED.open_interest_fp,
            settlement_ts = EXCLUDED.settlement_ts
    """)

    con.execute("""
        INSERT INTO metadata (key, value, updated)
        VALUES ('markets_last_sync', ?, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated = EXCLUDED.updated
    """, [str(len(df))])

    return len(df)


def main():
    key_id, private_key = get_keys()
    client = KalshiHttpClient(key_id, private_key)
    min_settled_ts = int((time.time() - 48 * 60 * 60) * 1000) # Default to 48 hours ago
    con = init_db()

    markets_last_sync = con.execute("""
    SELECT updated FROM metadata WHERE key = 'markets_last_sync'
    """).fetchone()
    if markets_last_sync:
        min_settled_ts = int(markets_last_sync[0].timestamp() * 1000) - (14 * 24 * 60 * 60 * 1000) # 6 hour overlap of last sync

    print("Fetching markets from kalshi...")
    markets = fetch_all_markets(client, min_settled_ts=min_settled_ts)
    print(f"Fetched {len(markets)} markets total")

    count = upsert_markets(con, markets)
    print(f"Upserted {count} markets into db")

    # Quick summary
    result = con.execute("""
        SELECT status, count(*) as cnt
        FROM markets
        GROUP BY status
        ORDER BY cnt DESC
    """).fetchall()

    print("\nmarkets by status:")
    for status, cnt in result:
        print(f"  {status}: {cnt}")

    con.close()


if __name__ == "__main__":
    main()
