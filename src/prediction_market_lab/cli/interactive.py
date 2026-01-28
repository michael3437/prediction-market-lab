"""Interactive Python shell with Kalshi client initialized."""

from prediction_market_lab.client.keys import get_keys
from prediction_market_lab.client.clients import KalshiHttpClient


def main():
    """Start interactive shell with client initialized."""
    client = KalshiHttpClient(*get_keys())

    import code
    banner = "Prediction Market Lab Interactive Shell\n"
    banner += "Available: client (KalshiHttpClient instance)\n"
    code.interact(banner=banner, local={"client": client})


if __name__ == "__main__":
    main()
