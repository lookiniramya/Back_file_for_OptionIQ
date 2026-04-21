"""
config.py — Paytm Money credentials
Update ACCESS_TOKEN daily (expires every 24 hours)
Run quick_token.py to get all tokens at once
"""

API_KEY    = ""   # From developer.paytmmoney.com → My Apps → Trading API
API_SECRET = ""   # From developer.paytmmoney.com → My Apps → Trading API

# Generated daily via quick_token.py
ACCESS_TOKEN        = ""   # For option chain, order APIs
PUBLIC_ACCESS_TOKEN = ""   # For WebSocket live price feed
READ_ACCESS_TOKEN   = ""   # For read-only APIs

# App defaults
DEFAULT_SYMBOL   = "NIFTY"
AUTO_REFRESH     = False
REFRESH_INTERVAL = 60
