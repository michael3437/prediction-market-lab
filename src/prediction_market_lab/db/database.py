"""DuckDB schema and helpers."""

import duckdb
from pathlib import Path

db_path = Path(__file__).parent.parent.parent.parent / "db" / "markets.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a connection to the DuckDB database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create tables if they don't exist."""
    # Track when last sync occured
    con.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key        VARCHAR PRIMARY KEY,
            value      VARCHAR,
            updated    TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            ticker                    VARCHAR PRIMARY KEY,
            event_ticker              VARCHAR NOT NULL,
            market_type               VARCHAR NOT NULL,

            created_time              TIMESTAMPTZ NOT NULL,
            updated_time              TIMESTAMPTZ,
            open_time                 TIMESTAMPTZ NOT NULL,
            close_time                TIMESTAMPTZ,
            latest_expiration_time    TIMESTAMPTZ,
            status                    VARCHAR NOT NULL,
            volume                    INTEGER NOT NULL,
            volume_fp                 VARCHAR NOT NULL,
            result                    VARCHAR,
            open_interest             INTEGER NOT NULL,
            open_interest_fp          VARCHAR NOT NULL,
            settlement_ts             TIMESTAMPTZ,

            series_ticker             VARCHAR,
            category                  VARCHAR
        )
    """)



def init_db() -> duckdb.DuckDBPyConnection:
    """Initialise database with schema and return connection."""
    con = get_connection()
    init_schema(con)
    return con
