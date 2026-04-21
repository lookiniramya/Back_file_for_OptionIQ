"""
debug_api.py — Run this to see exact Paytm Money API response structure
Usage: python debug_api.py
Paste your access_token when prompted
"""
import requests, json

token = input("Paste your access_token: ").strip()
expiry = input("Paste an expiry date (DD-MM-YYYY, e.g. 13-04-2026): ").strip()
symbol = input("Symbol (default NIFTY): ").strip() or "NIFTY"

BASE = "https://developer.paytmmoney.com"
hdrs = {"x-jwt-token": token, "Content-Type": "application/json"}

print("\n" + "="*60)
print("TEST 1: Option Chain CALL")
print("="*60)
r = requests.get(f"{BASE}/fno/v1/option-chain",
                 headers=hdrs,
                 params={"type": "CALL", "symbol": symbol, "expiry": expiry},
                 timeout=15)
print(f"Status: {r.status_code}")
body = r.json()
print("Top-level keys:", list(body.keys()))

data = body.get("data", {})
print("data keys:", list(data.keys()) if isinstance(data, dict) else type(data))

results = data.get("results", []) if isinstance(data, dict) else []
if results:
    print(f"results type: {type(results)}, len: {len(results)}")
    if isinstance(results[0], list):
        print(f"results[0] type: list, len: {len(results[0])}")
        if results[0]:
            print("FIRST RECORD KEYS:", list(results[0][0].keys()))
            print("FIRST RECORD:", json.dumps(results[0][0], indent=2))
    elif isinstance(results[0], dict):
        print("FIRST RECORD KEYS:", list(results[0].keys()))
        print("FIRST RECORD:", json.dumps(results[0], indent=2))

print("\n" + "="*60)
print("TEST 2: Live Price (multiple pref formats)")
print("="*60)
prefs = [
    f"NSE|INDEX|NIFTY 50",
    f"NSE|INDEX|{symbol}",
    f"NSE|EQ|{symbol}",
    f"NSE:NIFTY 50",
    f"NSE:NIFTY",
]
for pref in prefs:
    r2 = requests.get(f"{BASE}/data/v1/price/live",
                      headers=hdrs,
                      params={"mode": "LTP", "pref": pref},
                      timeout=8)
    body2 = r2.json()
    found = body2.get("data", [{}])[0].get("found", "?") if isinstance(body2.get("data"), list) else "?"
    ltp = body2.get("data", [{}])[0].get("last_price", body2.get("data",[{}])[0].get("ltp","?")) if isinstance(body2.get("data"), list) else "?"
    print(f"  pref='{pref}' → status={r2.status_code} found={found} ltp={ltp}")
    if found == True or (isinstance(ltp, (int,float)) and ltp > 0):
        print(f"  ✅ WORKING PREF: {pref}")
        print("  Full response:", json.dumps(body2.get("data",[{}])[0], indent=4))
        break
