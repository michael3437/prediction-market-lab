import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from clients import KalshiHttpClient, KalshiWebSocketClient

load_dotenv()
KEYID = os.environ.get("KEYID")
try:
    with open("apikey.txt", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
except FileNotFoundError:
    raise FileNotFoundError(f"Private key file not found at apikey.txt")
except Exception as e:
    raise Exception(f"Error loading private key: {str(e)}")

def get_keys():
    """Returns (KEYID, private_key)."""
    return (KEYID, private_key)