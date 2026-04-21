"""
quick_token.py — Get ALL Paytm Money tokens (access + public + read)
Run: python quick_token.py
"""
import requests, webbrowser, json

API_KEY    = ""   # paste your API Key
API_SECRET = ""   # paste your API Secret

if not API_KEY or not API_SECRET:
    print("Fill API_KEY and API_SECRET at the top of this file first!")
    exit(1)

login_url = f"https://login.paytmmoney.com/merchant-login?apiKey={API_KEY}&state=test"
print("="*60)
print("Opening Paytm Money login...")
print(f"URL: {login_url}")
print("="*60)

try:
    webbrowser.open(login_url)
except Exception:
    print("Open this URL manually in your browser")

print()
print("After login, copy the requestToken from browser URL bar")
print("URL looks like: http://127.0.0.1:8501/?requestToken=XXXXX&state=test")
print()

request_token = input("Paste requestToken here: ").strip()
if not request_token:
    print("No token entered. Exiting.")
    exit(1)

print("\nExchanging token with Paytm Money...")
try:
    resp = requests.post(
        "https://developer.paytmmoney.com/accounts/v2/gettoken",
        json={"api_key": API_KEY, "api_secret_key": API_SECRET,
              "request_token": request_token},
        timeout=15,
    )
    print(f"Status: {resp.status_code}")
    body = resp.json()

    # Print full raw response so you can see everything
    print("\nFULL RAW RESPONSE:")
    print(json.dumps(body, indent=2))

    if resp.status_code == 200:
        data = body.get("data", body)

        access_token        = data.get("access_token", "")
        public_access_token = data.get("public_access_token", "")
        read_access_token   = data.get("read_access_token", "")

        print("\n" + "="*60)
        print("YOUR TOKENS:")
        print("="*60)
        print(f"\naccess_token:\n{access_token}")
        print(f"\npublic_access_token:\n{public_access_token}")
        print(f"\nread_access_token:\n{read_access_token}")
        print("="*60)

        if public_access_token:
            print(f"\n✅ public_access_token found!")
            print(f"→ Paste into sidebar: ⚡ Live WebSocket Feed")
        else:
            print("\n⚠️  public_access_token NOT in response")
            print("This means your Paytm account may not have Live Broadcast API")
            print("The access_token above still works for option chain data")

        print(f"\nUpdate config.py with:")
        print(f'ACCESS_TOKEN = "{access_token}"')
        if public_access_token:
            print(f'PUBLIC_ACCESS_TOKEN = "{public_access_token}"')

except Exception as e:
    print(f"Error: {e}")
