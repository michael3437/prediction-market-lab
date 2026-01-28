# To be used with -i flag for quick testing.

from keys import get_keys
from clients import KalshiHttpClient

client = KalshiHttpClient(*get_keys())