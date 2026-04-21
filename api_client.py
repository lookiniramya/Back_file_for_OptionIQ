"""
api_client.py — Paytm Money API interactions
Handles fetching expiry dates and option chain data (CALL + PUT).
Includes DEMO mode with realistic synthetic data for testing.
"""

import requests
import random
from typing import Tuple, List, Dict, Any

PAYTM_BASE_URL = "https://developer.paytmmoney.com"
OPTION_CHAIN_URL = f"{PAYTM_BASE_URL}/fno/v1/option-chain"
CONFIG_URL = f"{PAYTM_BASE_URL}/fno/v1/option-chain/config"
# Alternative endpoints to try
ALT_CHAIN_URLS = [
    f"{PAYTM_BASE_URL}/fno/v1/option-chain",
    f"{PAYTM_BASE_URL}/fno/v2/option-chain",
    f"{PAYTM_BASE_URL}/data/v1/option-chain",
]


# ─── Expiry Dates ─────────────────────────────────────────────────────────────

def fetch_expiry_dates(
    access_token: str, symbol: str = "NIFTY"
) -> Tuple[List[str], str | None]:
    """
    Fetch available expiry dates via the Option Chain Config API.
    Official endpoint: GET /fno/v1/option-chain/config?symbol=NIFTY
    Returns: (list_of_expiry_strings, error_or_None)
    """
    if access_token == "DEMO":
        return _demo_expiry_dates(), None

    headers = {
        "x-jwt-token": access_token,
        "Content-Type": "application/json",
    }

    # Config endpoint only - never use option-chain for expiry
    attempts = [
        (CONFIG_URL, {"symbol": symbol}),
        (CONFIG_URL, {"symbol": symbol, "type": "CALL"}),
        (CONFIG_URL, {"scrip": symbol}),
    ]

    last_error = ""
    for url, params in attempts:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=12)

            # Capture raw response for debugging
            raw_text = resp.text[:500] if resp.text else ""

            if resp.status_code == 404:
                last_error = f"404 Not Found: {url}"
                continue
            if resp.status_code == 401:
                return [], "Token expired or invalid. Please generate a new access token."
            if resp.status_code == 403:
                return [], "Access forbidden. Make sure your app has F&O/Option Chain subscription."

            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code} at {url}: {raw_text}"
                continue

            body = resp.json()
            expiries = _extract_expiries(body)

            if expiries:
                return [str(e) for e in expiries], None
            else:
                # Show full raw response so we know exactly what Paytm returned
                last_error = (
                    f"Config API returned 200 but no expiry list found. "
                    f"RAW RESPONSE: {raw_text[:400]}"
                )

        except requests.exceptions.ConnectionError:
            return [], "Connection error. Check internet connectivity."
        except requests.exceptions.Timeout:
            last_error = f"Timeout at {url}"
        except Exception as e:
            last_error = f"Error at {url}: {str(e)}"

    return [], f"Could not load expiry dates. Last error: {last_error}"


def _unix_ms_to_date(ts) -> str:
    """Convert Unix millisecond timestamp to DD-MM-YYYY."""
    import datetime
    try:
        return datetime.datetime.fromtimestamp(int(ts) / 1000).strftime("%d-%m-%Y")
    except Exception:
        return str(ts)


def _extract_expiries(body: Any) -> List[str]:
    """
    Extract expiry dates from Paytm Money config response.
    Paytm Money actual format: {"data": {"expires": [1775550600000, ...]}}
    Timestamps are Unix milliseconds -> convert to DD-MM-YYYY
    """
    if isinstance(body, list):
        if not body:
            return []
        # Check if unix ms timestamps (> 1 trillion = ms)
        try:
            first = int(body[0])
            if first > 1_000_000_000_000:
                return [_unix_ms_to_date(ts) for ts in body]
        except (ValueError, TypeError):
            pass
        # Plain strings
        if isinstance(body[0], str):
            return [str(x) for x in body]
        # List of dicts
        if isinstance(body[0], dict):
            for dk in ["expiry", "date", "expiry_date", "value"]:
                out = [str(v[dk]) for v in body if dk in v]
                if out:
                    return out
        return []

    if not isinstance(body, dict):
        return []

    # Paytm uses "expires" key inside "data" - check priority keys first
    for key in ["expires", "expiry_list", "expiryList", "expiry_dates",
                "expiryDates", "expiries", "expiry", "data", "result"]:
        val = body.get(key)
        if val is None:
            continue
        result = _extract_expiries(val)
        if result:
            return result

    return []


# ─── Option Chain (Both Sides) ────────────────────────────────────────────────

def fetch_option_chain_both(
    access_token: str, symbol: str, expiry: str
) -> Tuple[Dict[str, Any], str | None]:
    """
    Fetch both CALL and PUT option chains for a symbol/expiry.
    Returns: ({"calls": [...], "puts": [...]}, error_or_None)
    """
    if access_token == "DEMO":
        return _demo_option_chain(symbol), None

    calls, err1 = _fetch_single_chain(access_token, symbol, expiry, "CALL")
    puts, err2 = _fetch_single_chain(access_token, symbol, expiry, "PUT")

    if err1 and err2:
        return {}, f"Failed to fetch both chains. CALL: {err1} | PUT: {err2}"

    return {"calls": calls or [], "puts": puts or []}, None


def _fetch_single_chain(
    access_token: str, symbol: str, expiry: str, option_type: str
) -> Tuple[List[Dict], str | None]:
    """
    Fetch a single option chain (CALL or PUT).
    Tries multiple endpoint URLs and param formats automatically.
    """
    headers = {
        "x-jwt-token": access_token,
        "Content-Type": "application/json",
    }

    # Confirmed working: v1 with symbol + DD-MM-YYYY expiry
    attempts = [
        (f"{PAYTM_BASE_URL}/fno/v1/option-chain", {"type": option_type, "symbol": symbol, "expiry": expiry}),
    ]

    last_error = ""
    for url, params in attempts:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=12)

            if resp.status_code == 404:
                last_error = f"404 at {url} params={params}"
                continue
            if resp.status_code == 401:
                return [], "Token expired. Please login again."
            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code} at {url}: {resp.text[:200]}"
                continue

            body = resp.json()
            records = _extract_records(body)

            if records:
                return _normalize_records(records, option_type), None
            else:
                last_error = f"200 OK but no records at {url}. Response: {resp.text[:300]}"

        except requests.exceptions.ConnectionError:
            return [], f"Connection error fetching {option_type} chain."
        except requests.exceptions.Timeout:
            last_error = f"Timeout at {url}"
        except Exception as e:
            last_error = f"Error at {url}: {str(e)}"

    return [], f"Could not fetch {option_type} chain. Last: {last_error}"



def _extract_records(body: Any) -> List[Dict]:
    """
    Extract option records from Paytm Money response.
    Actual format: {"data": {"results": [[...99 items...], [...29 items...]]}}
    results is a LIST OF LISTS — must be flattened.
    """
    if isinstance(body, list):
        # Could be a list of records OR a list of lists (Paytm actual format)
        if not body:
            return []
        if isinstance(body[0], dict):
            return body          # Already flat list of records
        if isinstance(body[0], list):
            # Flatten list of lists
            flat = []
            for sublist in body:
                if isinstance(sublist, list):
                    flat.extend(sublist)
            return flat
        return body

    if not isinstance(body, dict):
        return []

    # Paytm Money actual key path: body -> data -> results
    for key in ["results", "data", "records", "option_chain",
                "optionChain", "result", "response", "options"]:
        val = body.get(key)
        if val is None:
            continue
        if isinstance(val, list):
            return _extract_records(val)   # recurse handles flat vs nested
        if isinstance(val, dict):
            inner = _extract_records(val)
            if inner:
                return inner

    return []


def _safe_float(val, default=0.0) -> float:
    """Safely convert any value to float."""
    try:
        if val is None or val == "" or val == "NULL":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _normalize_records(records: List[Dict], option_type: str) -> List[Dict]:
    """
    Normalize Paytm Money option chain records.
    Confirmed field names from live API (13-Apr-2026):
      stk_price   = strike price
      spot_price  = current spot / LTP of index
      price       = option LTP
      oi          = open interest
      traded_vol  = volume
      oi_per_chg  = OI % change
      net_chg     = net price change
      delta, theta, gamma, vega, iv = greeks
      option_type = CE / PE
    """
    if not records:
        return []

    normalized = []
    suffix = "CE" if option_type == "CALL" else "PE"

    for r in records:
        try:
            # ── Strike: confirmed field is stk_price ──────────────────────
            strike = _safe_float(r.get("stk_price") or r.get("strike_price") or
                                  r.get("strike") or r.get("sp"))
            if strike < 100:
                continue  # Skip invalid records

            # ── Spot price embedded in each record ────────────────────────
            spot = _safe_float(r.get("spot_price") or r.get("underlaying_scrip_code"))

            # ── Option LTP ────────────────────────────────────────────────
            price = _safe_float(r.get("price") or r.get("ltp") or r.get("last_price"))

            # ── Open Interest ─────────────────────────────────────────────
            oi = _safe_float(r.get("oi") or r.get("OI") or r.get("open_interest"))

            # ── Volume: confirmed field is traded_vol ─────────────────────
            volume = _safe_float(r.get("traded_vol") or r.get("volume") or r.get("vol"))

            # ── OI Change %: confirmed field is oi_per_chg ───────────────
            oi_chg = _safe_float(r.get("oi_per_chg") or r.get("oi_perc_chg") or
                                  r.get("oi_change_perc"))

            # ── Net Change ────────────────────────────────────────────────
            net_chg = _safe_float(r.get("net_chg") or r.get("per_chg") or
                                   r.get("net_change"))

            # ── Option Type ───────────────────────────────────────────────
            otype_raw = str(r.get("option_type") or suffix).upper()
            otype = "CE" if ("CE" in otype_raw or "CALL" in otype_raw) else "PE"

            # ── Greeks (all confirmed present) ────────────────────────────
            delta = _safe_float(r.get("delta"))
            theta = _safe_float(r.get("theta"))
            gamma = _safe_float(r.get("gamma"))
            vega  = _safe_float(r.get("vega"))
            iv    = _safe_float(r.get("iv"))

            # ── Extra useful fields ───────────────────────────────────────
            lot_size    = int(_safe_float(r.get("lot_size", 0)))
            name        = str(r.get("name", ""))
            pml_symbol  = str(r.get("pml_symbol", ""))
            expiry_date = str(r.get("expiry_date", ""))

            normalized.append({
                "strike_price": strike,
                "spot_price":   spot,
                "price":        price,
                "oi":           oi,
                "oi_perc_chg":  oi_chg,
                "net_chg":      net_chg,
                "option_type":  otype,
                "volume":       volume,
                "delta":        delta,
                "theta":        theta,
                "gamma":        gamma,
                "vega":         vega,
                "iv":           iv,
                "lot_size":     lot_size,
                "name":         name,
                "pml_symbol":   pml_symbol,
                "expiry_date":  expiry_date,
            })
        except Exception:
            continue

    return normalized


# ─── DEMO Data Generator ──────────────────────────────────────────────────────

def _demo_expiry_dates() -> List[str]:
    """Realistic demo expiry dates."""
    return [
        "10-04-2025", "17-04-2025", "24-04-2025",
        "01-05-2025", "29-05-2025", "26-06-2025",
    ]


def _demo_option_chain(symbol: str = "NIFTY") -> Dict[str, Any]:
    """
    Generate realistic synthetic option chain data for demo/testing.
    Simulates a NIFTY-like chain around 22500 spot price.
    """
    spot = 22_480.0
    strikes = [round(spot / 50) * 50 + i * 50 for i in range(-15, 16)]

    random.seed(42)  # Deterministic demo data

    calls, puts = [], []

    for strike in strikes:
        diff = strike - spot

        # Realistic OI distribution: peaks near ATM
        dist = abs(diff)
        oi_base = max(500_000, 5_000_000 * (1 / (1 + dist / 300)))
        oi_noise = random.uniform(0.6, 1.4)

        # Add concentration at round numbers for realism
        if strike % 500 == 0:
            oi_noise *= 2.5
        elif strike % 100 == 0:
            oi_noise *= 1.5

        call_oi = int(oi_base * oi_noise)
        put_oi = int(oi_base * random.uniform(0.7, 1.3))

        # Prices using simplified Black-Scholes intuition
        call_iv = 0.15 + abs(diff) * 0.0002
        put_iv = 0.16 + abs(diff) * 0.0002
        call_price = max(0.05, (spot * call_iv * 0.4) - max(0, diff) * 0.8)
        put_price = max(0.05, (spot * put_iv * 0.4) + min(0, diff) * (-0.8))

        # OI change: adds "story" to the data
        call_oi_chg = random.uniform(-5, 15)
        put_oi_chg = random.uniform(-5, 12)

        # Delta: deep ITM ≈ 1, ATM ≈ 0.5, OTM → 0
        call_delta = max(0.01, min(0.99, 0.5 - diff / (spot * 0.1)))
        put_delta = max(-0.99, min(-0.01, -0.5 - diff / (spot * 0.1)))

        calls.append({
            "strike_price": strike,
            "price": round(call_price, 2),
            "oi": call_oi,
            "oi_perc_chg": round(call_oi_chg, 2),
            "net_chg": round(random.uniform(-50, 50), 2),
            "option_type": "CE",
            "volume": int(call_oi * random.uniform(0.3, 0.9)),
            "delta": round(call_delta, 4),
            "theta": round(-random.uniform(0.1, 0.8), 4),
            "gamma": round(random.uniform(0.001, 0.02), 4),
            "vega": round(random.uniform(0.05, 0.3), 4),
        })

        puts.append({
            "strike_price": strike,
            "price": round(put_price, 2),
            "oi": put_oi,
            "oi_perc_chg": round(put_oi_chg, 2),
            "net_chg": round(random.uniform(-50, 50), 2),
            "option_type": "PE",
            "volume": int(put_oi * random.uniform(0.3, 0.9)),
            "delta": round(put_delta, 4),
            "theta": round(-random.uniform(0.1, 0.8), 4),
            "gamma": round(random.uniform(0.001, 0.02), 4),
            "vega": round(random.uniform(0.05, 0.3), 4),
        })

    return {"calls": calls, "puts": puts, "_spot": spot}
