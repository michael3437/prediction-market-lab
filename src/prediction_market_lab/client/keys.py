import os
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from prediction_market_lab.client.clients import KalshiHttpClient, KalshiWebSocketClient

keyfile_path = Path(__file__).parent.parent.parent.parent / "apikey.txt"

load_dotenv()
KEYID = os.environ.get("KEYID")

try:
    with open(keyfile_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
except FileNotFoundError:
    raise FileNotFoundError(f"Private key file not found at {keyfile_path}")
except Exception as e:
    raise Exception(f"Error loading private key: {str(e)}")

def get_keys():
    """Returns (KEYID, private_key)."""
    return (KEYID, private_key)