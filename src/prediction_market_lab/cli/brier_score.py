"""Calculate Brier scores for market forecasts at various intervals before close."""

import pandas as pd
from datetime import timedelta

from prediction_market_lab.db.database import init_db

# (name, minutes before close, candle period in minutes)
INTERVALS = [
    ("15m", 15, 1),
    ("30m", 30, 1),
    ("1h", 60, 1),
    ("2h", 120, 1),
    ("4h", 240, 60),
    ("12h", 720, 60),
    ("24h", 1440, 60),
    ("2d", 2880, 60),
    ("4d", 5760, 60),
    ("1w", 10080, 1440),
    ("2w", 20160, 1440),
]

PERCENTILE_BUCKETS = 1

def calculate_brier(df: pd.DataFrame) -> float:
    """Calculate mean Brier score from a DataFrame with forecast and outcome columns."""
    return ((df['forecast'] - df['outcome']) ** 2).mean()

def get_forecasts_at_interval(
    con,
    interval_name: str,
    minutes_before: int,
    candle_period: int,
    # categories: Optional[list[str]], - to be implemented
    # volume_threshold: Optional[int], - to be implemented
    ) -> pd.DataFrame:
    """Returns DataFrame with columns: ticker, forecast, outcome, volume, open_interest."""
    query = f"""
    SELECT
        m.ticker,
        c.price_close / 100.0 AS forecast,
        CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END AS outcome,
        m.volume,
        m.open_interest
    FROM markets m
    ASOF JOIN candlesticks c
        ON m.ticker = c.ticker
        AND c.end_period_ts <= m.close_time - INTERVAL '{minutes_before} MINUTES'
    WHERE c.period_interval = {candle_period}
    AND c.price_close IS NOT NULL
    AND m.result IS NOT NULL
    AND m.candles_synced_at IS NOT NULL
    """
    return con.execute(query).fetchdf()

def calculate_all_brier_scores(con) -> pd.DataFrame:
    """Calculate Brier scores for all intervals."""
    results = []

    for name, minutes_before, candle_period in INTERVALS:
        df = get_forecasts_at_interval(
            con, name, minutes_before, candle_period # category, volume_threshold,
        )
        if len(df) == 0:
            continue
        brier = calculate_brier(df)
        results.append({
            'interval': name,
            'brier_score': brier,
            'n_markets': len(df),
            # 'category': category,
            # 'volume_threshold': volume_threshold,
        })
    
    return pd.DataFrame(results)

def print_brier_scores_table(df: pd.DataFrame, title:str):
    """Prints a formatted table."""
    print(title)
    print("="*len(title))
    print(df.to_string(index=False))

def main():
    con = init_db()

    print("Calculating Brier scores by interval...")
    df = calculate_all_brier_scores(con)
    print_brier_scores_table(df, "Brier Scores by Interval")

    con.close()

if __name__ == "__main__":
    main()
