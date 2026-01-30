"""Main CLI entry point for prediction-market commands."""

import sys
import argparse


def sync_markets_command(args):
    """Run the sync markets command."""
    from prediction_market_lab.cli.sync_markets import main
    main()


def sync_candles_command(args):
    """Run the sync candles command."""
    from prediction_market_lab.cli.sync_candles import main
    main()


def interactive_command(args):
    """Start interactive Python session with client initialized."""
    from prediction_market_lab.cli.interactive import main
    main()


def brier_score_command(args):
    """Run Brier score analysis."""
    from prediction_market_lab.cli.brier_score import main
    main()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="prediction-market",
        description="Prediction Market Lab CLI tools"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # sync-markets command
    sync_parser = subparsers.add_parser(
        "sync-markets",
        help="Fetch and sync market data from Kalshi"
    )
    sync_parser.set_defaults(func=sync_markets_command)

    # sync-candles command
    candles_parser = subparsers.add_parser(
        "sync-candles",
        help="Fetch candlestick data for unsynced markets"
    )
    candles_parser.set_defaults(func=sync_candles_command)

    # interactive command
    interactive_parser = subparsers.add_parser(
        "interactive",
        help="Start interactive Python shell with client"
    )
    interactive_parser.set_defaults(func=interactive_command)

    # brier-score command
    brier_parser = subparsers.add_parser(
        "brier-score",
        help="Calculate Brier scores for market forecasts"
    )
    brier_parser.set_defaults(func=brier_score_command)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
