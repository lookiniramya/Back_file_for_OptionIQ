"""
market_data.py — Extended market intelligence for OptionsIQ
Fetches: Live price, Technical indicators, Market breadth, News sentiment
Uses Paytm Money Live Market Data API + free external sources
"""

import requests
import datetime
import json
from typing import Dict, Any, List, Tuple
import math

PAYTM_BASE = "https://developer.paytmmoney.com"
PAYTM_LIVE_API_URL = f"{PAYTM_BASE}/data/v1/price/live"
PAYTM_INDEX_PREFS = {
    "NIFTY": {"scripType": "INDEX", "exchangeType": "NSE", "scripId": "13"},
    "BANKNIFTY": {"scripType": "INDEX", "exchangeType": "NSE", "scripId": "25"},
    "FINNIFTY": {"scripType": "INDEX", "exchangeType": "NSE", "scripId": "2885"},
    "MIDCPNIFTY": {"scripType": "INDEX", "exchangeType": "NSE", "scripId": "14366"},
}


# ─── Live Market Price ────────────────────────────────────────────────────────

def fetch_live_price(access_token: str, symbol: str = "NIFTY",
                     option_chain_data: Dict = None) -> Dict[str, Any]:
    """
    Fetch live price + OHLC for index.
    Spot LTP  : from option chain records (spot_price field — confirmed working)
    OHLC data : from NSE India free API / Yahoo Finance fallback
    """
    result = {
        "ltp": 0.0, "open": 0.0, "high": 0.0, "low": 0.0,
        "prev_close": 0.0, "change": 0.0, "change_pct": 0.0,
        "volume": 0, "error": None, "source": "unknown",
        "52w_high": 0.0, "52w_low": 0.0,
    }

    if access_token == "DEMO":
        result.update({"ltp": 22480.0, "open": 22300.0, "high": 22550.0,
                       "low": 22250.0, "prev_close": 22380.0, "change": 100.0,
                       "change_pct": 0.45, "source": "demo",
                       "52w_high": 26277.0, "52w_low": 21964.0})
        return result

    # ── Step 1: Spot LTP from option chain ────────────────────────────────
    spot = 0.0
    if option_chain_data:
        for side in ["calls", "puts"]:
            for rec in option_chain_data.get(side, []):
                sp = rec.get("spot_price")
                if sp:
                    try:
                        sp_f = float(sp)
                        if sp_f > 100:
                            spot = sp_f
                            result["ltp"]    = sp_f
                            result["source"] = "option_chain"
                            break
                    except (TypeError, ValueError):
                        continue
            if spot > 0:
                break

    paytm_live = fetch_paytm_live_price(access_token, symbol)
    if paytm_live.get("ltp", 0) > 0:
        if paytm_live.get("ltp", 0) == 0 and spot > 0:
            paytm_live["ltp"] = spot
        if paytm_live.get("prev_close", 0) == 0 and spot > 0:
            paytm_live["prev_close"] = spot
        return {**result, **paytm_live}

    # ── Step 2: OHLC from NSE India free API ─────────────────────────────
    nse_symbol_map = {
        "NIFTY":      "NIFTY 50",
        "BANKNIFTY":  "NIFTY BANK",
        "FINNIFTY":   "NIFTY FIN SERVICE",
        "MIDCPNIFTY": "NIFTY MIDCAP SELECT",
    }
    nse_sym = nse_symbol_map.get(symbol, symbol)

    ohlc_fetched = False

    # Try NSE India API (free, no auth needed)
    try:
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
        }
        # NSE indices endpoint
        nse_resp = requests.get(
            "https://www.nseindia.com/api/allIndices",
            headers=nse_headers,
            timeout=8,
        )
        if nse_resp.status_code == 200:
            indices = nse_resp.json().get("data", [])
            for idx in indices:
                if idx.get("index", "").upper() == nse_sym.upper():
                    ltp_nse = float(idx.get("last", 0) or 0)
                    result["ltp"]        = ltp_nse if ltp_nse > 0 else spot
                    result["open"]       = float(idx.get("open", 0) or 0)
                    result["high"]       = float(idx.get("high", 0) or 0)
                    result["low"]        = float(idx.get("low", 0) or 0)
                    result["prev_close"] = float(idx.get("previousClose", 0) or 0)
                    result["change"]     = float(idx.get("change", 0) or 0)
                    result["change_pct"] = float(idx.get("percentChange", 0) or 0)
                    result["52w_high"]   = float(idx.get("yearHigh", 0) or 0)
                    result["52w_low"]    = float(idx.get("yearLow", 0) or 0)
                    result["source"]     = "NSE India API"
                    result["error"]      = None
                    ohlc_fetched = True
                    break
    except Exception:
        pass

    # ── Step 3: Yahoo Finance fallback for OHLC ───────────────────────────
    if not ohlc_fetched:
        try:
            yf_symbol_map = {
                "NIFTY":      "%5ENSEI",
                "BANKNIFTY":  "%5ENSEBANK",
                "FINNIFTY":   "NIFTYFINSERVICE.NS",
                "MIDCPNIFTY": "NIFTYMIDSELECT.NS",
            }
            yf_sym = yf_symbol_map.get(symbol, "%5ENSEI")
            yf_resp = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_sym}",
                params={"interval": "1d", "range": "5d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            if yf_resp.status_code == 200:
                meta = yf_resp.json()["chart"]["result"][0]["meta"]
                result["ltp"]        = float(meta.get("regularMarketPrice", spot) or spot)
                result["open"]       = float(meta.get("regularMarketOpen", 0) or 0)
                result["high"]       = float(meta.get("regularMarketDayHigh", 0) or 0)
                result["low"]        = float(meta.get("regularMarketDayLow", 0) or 0)
                result["prev_close"] = float(meta.get("previousClose", 0) or 0)
                result["volume"]     = int(meta.get("regularMarketVolume", 0) or 0)
                result["52w_high"]   = float(meta.get("fiftyTwoWeekHigh", 0) or 0)
                result["52w_low"]    = float(meta.get("fiftyTwoWeekLow", 0) or 0)
                ltp  = result["ltp"]
                prev = result["prev_close"]
                result["change"]     = round(ltp - prev, 2) if prev else 0
                result["change_pct"] = round(((ltp - prev) / prev) * 100, 2) if prev else 0
                result["source"]     = "Yahoo Finance"
                result["error"]      = None
                ohlc_fetched = True
        except Exception:
            pass

    # ── Step 4: If OHLC still missing, compute from option chain delta ────
    if not ohlc_fetched and spot > 0:
        # Use spot as best estimate — no OHLC available
        result["ltp"]        = spot
        result["open"]       = spot
        result["high"]       = spot
        result["low"]        = spot
        result["prev_close"] = spot
        result["source"]     = "option_chain_only"
        result["error"]      = "OHLC unavailable — showing spot from option chain"

    # Always ensure LTP is set to spot if APIs returned 0
    if result["ltp"] == 0 and spot > 0:
        result["ltp"] = spot

    return result


def _extract_price_data(body: Any) -> Dict:
    """Extract price fields from Paytm Money live price response."""
    out = {}
    try:
        data = body
        if isinstance(body, dict):
            # Unwrap data array
            inner = body.get("data", body)
            if isinstance(inner, list) and inner:
                data = inner[0]
            elif isinstance(inner, dict):
                data = inner

        if not isinstance(data, dict):
            return {}

        # Skip if not found
        if data.get("found") is False:
            return {}

        def _f(val): return float(val or 0)

        # Try all known field names for each price component
        ltp = _f(data.get("last_price") or data.get("ltp") or
                 data.get("close") or data.get("price") or 0)
        if ltp <= 0:
            return {}   # No valid price

        out["ltp"]        = ltp
        out["open"]       = _f(data.get("open") or data.get("open_price") or ltp)
        out["high"]       = _f(data.get("high") or data.get("high_price") or ltp)
        out["low"]        = _f(data.get("low")  or data.get("low_price")  or ltp)
        out["prev_close"] = _f(data.get("prev_close") or data.get("close_price") or
                               data.get("previous_close") or ltp)
        out["volume"]     = int(_f(data.get("volume") or data.get("vol") or 0))
        out["change"]     = _f(data.get("net_change") or data.get("change") or
                               data.get("net_chg") or (ltp - out["prev_close"]))
        out["change_pct"] = _f(data.get("percent_change") or data.get("change_pct") or
                               data.get("pct_change") or
                               ((out["change"] / out["prev_close"] * 100) if out["prev_close"] else 0))
    except Exception:
        pass
    return out


def fetch_paytm_live_price(access_token: str, symbol: str = "NIFTY") -> Dict[str, Any]:
    """Fetch near-real-time market data from Paytm's Live Market Data API."""
    pref = PAYTM_INDEX_PREFS.get(symbol.upper())
    if not pref or not access_token or access_token == "DEMO":
        return {}

    headers = {
        "x-jwt-token": access_token,
        "Content-Type": "application/json",
    }
    pref_payload = json.dumps([pref], separators=(",", ":"))

    for mode in ["FULL", "QUOTE", "LTP"]:
        try:
            resp = requests.get(
                PAYTM_LIVE_API_URL,
                headers=headers,
                params={"mode": mode, "pref": pref_payload},
                timeout=4,
            )
            if resp.status_code != 200:
                continue

            parsed = _extract_price_data(resp.json())
            if parsed.get("ltp", 0) > 0:
                parsed["source"] = f"Paytm Live API ({mode})"
                parsed["error"] = None
                parsed["timestamp"] = datetime.datetime.now().strftime("%H:%M:%S")
                parsed["symbol"] = symbol.upper()
                return parsed
        except Exception:
            continue

    return {}


def derive_spot_from_chain(calls_data: List[Dict], puts_data: List[Dict]) -> float:
    """
    Derive spot price from option chain using put-call parity.
    Used as fallback when live price API is unavailable.
    C - P = F - K  =>  spot ≈ K + (C_price - P_price) at ATM
    """
    try:
        import pandas as pd
        df_c = pd.DataFrame(calls_data)
        df_p = pd.DataFrame(puts_data)
        if df_c.empty or df_p.empty:
            return 0.0
        for df in [df_c, df_p]:
            df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce")
            df["price"]        = pd.to_numeric(df["price"],        errors="coerce").fillna(0)

        merged = pd.merge(
            df_c[["strike_price","price"]].rename(columns={"price":"call_price"}),
            df_p[["strike_price","price"]].rename(columns={"price":"put_price"}),
            on="strike_price"
        )
        # Find strike where call_price ≈ put_price (ATM by put-call parity)
        merged["diff"] = abs(merged["call_price"] - merged["put_price"])
        atm_row = merged.loc[merged["diff"].idxmin()]
        spot = float(atm_row["strike_price"]) + (float(atm_row["call_price"]) - float(atm_row["put_price"]))
        return round(spot, 2)
    except Exception:
        return 0.0


# ─── Technical Indicators ─────────────────────────────────────────────────────

def compute_technical_indicators(
    calls_data: List[Dict], puts_data: List[Dict],
    spot: float, symbol: str = "NIFTY"
) -> Dict[str, Any]:
    """
    Compute technical indicators from option chain data.
    Since we don't have OHLCV candle history from Paytm Money free tier,
    we derive indicators from the option chain itself — this is actually
    more relevant for options trading than price-based indicators.
    """
    indicators = {
        # IV indicators
        "atm_iv_call": 0.0,
        "atm_iv_put": 0.0,
        "iv_skew": 0.0,          # Put IV - Call IV (positive = bearish skew)
        "iv_percentile": 0.0,    # Estimated
        # OI-based momentum
        "oi_momentum": "NEUTRAL",
        "call_oi_concentration": 0.0,
        "put_oi_concentration": 0.0,
        # Max pain
        "max_pain": 0.0,
        # Volume indicators
        "call_put_vol_ratio": 0.0,
        "unusual_activity": [],
        # Greeks summary
        "net_delta": 0.0,
        "avg_theta_call": 0.0,
        "avg_theta_put": 0.0,
    }

    try:
        import pandas as pd
        df_c = pd.DataFrame(calls_data) if calls_data else pd.DataFrame()
        df_p = pd.DataFrame(puts_data) if puts_data else pd.DataFrame()

        if df_c.empty or df_p.empty:
            return indicators

        # Numeric cast
        for df in [df_c, df_p]:
            for col in ["strike_price", "price", "oi", "volume", "delta",
                        "theta", "gamma", "vega", "oi_perc_chg"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # ATM strike
        strikes = df_c["strike_price"].tolist()
        atm = min(strikes, key=lambda x: abs(x - spot))

        # ATM IV (approximate from vega & price — simplified)
        atm_call = df_c[df_c["strike_price"] == atm]
        atm_put  = df_p[df_p["strike_price"] == atm]

        if not atm_call.empty:
            indicators["atm_iv_call"] = round(float(atm_call.iloc[0].get("vega", 0)) * 100, 2)
            indicators["avg_theta_call"] = round(float(df_c["theta"].mean()), 4)
        if not atm_put.empty:
            indicators["atm_iv_put"] = round(float(atm_put.iloc[0].get("vega", 0)) * 100, 2)
            indicators["avg_theta_put"] = round(float(df_p["theta"].mean()), 4)

        indicators["iv_skew"] = round(indicators["atm_iv_put"] - indicators["atm_iv_call"], 2)

        # Max Pain — strike where total option buyers lose the most
        max_pain = _compute_max_pain(df_c, df_p)
        indicators["max_pain"] = max_pain

        # OI Concentration (top 3 strikes hold what % of total OI)
        total_call_oi = df_c["oi"].sum()
        total_put_oi  = df_p["oi"].sum()
        top3_call_oi  = df_c.nlargest(3, "oi")["oi"].sum()
        top3_put_oi   = df_p.nlargest(3, "oi")["oi"].sum()

        if total_call_oi > 0:
            indicators["call_oi_concentration"] = round((top3_call_oi / total_call_oi) * 100, 1)
        if total_put_oi > 0:
            indicators["put_oi_concentration"] = round((top3_put_oi / total_put_oi) * 100, 1)

        # Call/Put Volume Ratio
        total_call_vol = df_c["volume"].sum()
        total_put_vol  = df_p["volume"].sum()
        if total_call_vol > 0:
            indicators["call_put_vol_ratio"] = round(total_put_vol / total_call_vol, 3)

        # OI momentum from oi_perc_chg
        call_oi_chg = df_c["oi_perc_chg"].mean()
        put_oi_chg  = df_p["oi_perc_chg"].mean()
        if put_oi_chg > call_oi_chg + 2:
            indicators["oi_momentum"] = "BULLISH"
        elif call_oi_chg > put_oi_chg + 2:
            indicators["oi_momentum"] = "BEARISH"
        else:
            indicators["oi_momentum"] = "NEUTRAL"

        # Net delta (directional exposure)
        net_delta = df_c["delta"].sum() - abs(df_p["delta"].sum())
        indicators["net_delta"] = round(net_delta, 2)

        # Unusual activity (high volume relative to OI)
        df_c["vol_oi_ratio"] = df_c.apply(
            lambda r: r["volume"] / r["oi"] if r["oi"] > 0 else 0, axis=1)
        df_p["vol_oi_ratio"] = df_p.apply(
            lambda r: r["volume"] / r["oi"] if r["oi"] > 0 else 0, axis=1)

        unusual = []
        for _, row in df_c[df_c["vol_oi_ratio"] > 0.5].nlargest(3, "vol_oi_ratio").iterrows():
            unusual.append({
                "strike": int(row["strike_price"]),
                "type": "CE",
                "ratio": round(row["vol_oi_ratio"], 2),
                "volume": int(row["volume"]),
            })
        for _, row in df_p[df_p["vol_oi_ratio"] > 0.5].nlargest(3, "vol_oi_ratio").iterrows():
            unusual.append({
                "strike": int(row["strike_price"]),
                "type": "PE",
                "ratio": round(row["vol_oi_ratio"], 2),
                "volume": int(row["volume"]),
            })
        indicators["unusual_activity"] = sorted(unusual, key=lambda x: x["ratio"], reverse=True)[:5]

    except Exception as e:
        indicators["error"] = str(e)

    return indicators


def _compute_max_pain(df_c, df_p) -> float:
    """
    Max Pain = strike where total option writers' pain (buyer profit) is minimized.
    For each strike K, compute sum of ITM value across all options.
    """
    try:
        all_strikes = sorted(set(df_c["strike_price"].tolist() + df_p["strike_price"].tolist()))
        min_pain = float("inf")
        max_pain_strike = all_strikes[len(all_strikes) // 2]

        for k in all_strikes:
            # Call pain at K: sum of max(strike - K, 0) * OI for all call strikes < K
            call_pain = sum(
                max(float(row["strike_price"]) - k, 0) * float(row["oi"])
                for _, row in df_c.iterrows()
            )
            # Put pain at K: sum of max(K - strike, 0) * OI for all put strikes > K
            put_pain = sum(
                max(k - float(row["strike_price"]), 0) * float(row["oi"])
                for _, row in df_p.iterrows()
            )
            total = call_pain + put_pain
            if total < min_pain:
                min_pain = total
                max_pain_strike = k

        return float(max_pain_strike)
    except Exception:
        return 0.0


# ─── Market Sentiment ─────────────────────────────────────────────────────────

def compute_market_sentiment(
    pcr: float, snapshot: Dict, indicators: Dict, live_price: Dict
) -> Dict[str, Any]:
    """
    Aggregate all signals into a unified market sentiment score.
    Score: -100 (extreme bearish) to +100 (extreme bullish)
    """
    score = 0
    signals = []

    # 1. PCR signal (weight: 25)
    if pcr > 1.5:
        score += 25; signals.append(("🟢", "PCR > 1.5: Strong put writing (bullish)", 25))
    elif pcr > 1.2:
        score += 15; signals.append(("🟢", f"PCR {pcr:.2f}: Bullish sentiment", 15))
    elif pcr > 0.8:
        score += 0;  signals.append(("⚖️", f"PCR {pcr:.2f}: Neutral", 0))
    elif pcr > 0.5:
        score -= 15; signals.append(("🔴", f"PCR {pcr:.2f}: Bearish — call buying surge", -15))
    else:
        score -= 25; signals.append(("🔴", f"PCR {pcr:.2f}: Very bearish", -25))

    # 2. Price vs Max Pain (weight: 20)
    max_pain = indicators.get("max_pain", 0)
    ltp = live_price.get("ltp", snapshot.get("spot_price", 0))
    if max_pain > 0 and ltp > 0:
        diff_pct = ((ltp - max_pain) / max_pain) * 100
        if diff_pct > 1.5:
            score -= 15; signals.append(("🔴", f"Price {diff_pct:+.1f}% above Max Pain ({max_pain:.0f}): Gravity pull down", -15))
        elif diff_pct < -1.5:
            score += 15; signals.append(("🟢", f"Price {diff_pct:+.1f}% below Max Pain ({max_pain:.0f}): Gravity pull up", 15))
        else:
            score += 5; signals.append(("⚖️", f"Price near Max Pain ({max_pain:.0f}): Stable zone", 5))

    # 3. OI Momentum (weight: 20)
    oi_mom = indicators.get("oi_momentum", "NEUTRAL")
    if oi_mom == "BULLISH":
        score += 20; signals.append(("🟢", "OI momentum bullish: PUT OI building faster", 20))
    elif oi_mom == "BEARISH":
        score -= 20; signals.append(("🔴", "OI momentum bearish: CALL OI building faster", -20))
    else:
        signals.append(("⚖️", "OI momentum neutral", 0))

    # 4. IV Skew (weight: 15)
    iv_skew = indicators.get("iv_skew", 0)
    if iv_skew > 2:
        score -= 10; signals.append(("🔴", f"IV skew +{iv_skew:.1f}: Puts costlier — fear in market", -10))
    elif iv_skew < -2:
        score += 10; signals.append(("🟢", f"IV skew {iv_skew:.1f}: Calls costlier — bullish demand", 10))

    # 5. Intraday price move (weight: 20)
    chg_pct = live_price.get("change_pct", 0)
    if chg_pct > 0.5:
        score += 20; signals.append(("🟢", f"Price up {chg_pct:+.2f}% today: Bullish momentum", 20))
    elif chg_pct < -0.5:
        score -= 20; signals.append(("🔴", f"Price down {chg_pct:+.2f}% today: Bearish momentum", -20))
    else:
        signals.append(("⚖️", f"Price change {chg_pct:+.2f}%: Sideways", 0))

    # Normalize to -100 to +100
    score = max(-100, min(100, score))

    if score >= 50:
        sentiment = "STRONGLY BULLISH"; color = "#00e676"
    elif score >= 20:
        sentiment = "BULLISH"; color = "#69f0ae"
    elif score >= -20:
        sentiment = "NEUTRAL"; color = "#8888cc"
    elif score >= -50:
        sentiment = "BEARISH"; color = "#ff7043"
    else:
        sentiment = "STRONGLY BEARISH"; color = "#ff1744"

    return {
        "score": score,
        "sentiment": sentiment,
        "color": color,
        "signals": signals,
    }


# ─── News Headlines (Free API) ────────────────────────────────────────────────

def fetch_market_news(symbol: str = "NIFTY") -> List[Dict[str, str]]:
    """
    Fetch market news from free NewsData.io or GNews API.
    Falls back to curated static headlines if API unavailable.
    """
    try:
        query = f"NIFTY stock market India" if "NIFTY" in symbol else f"{symbol} India stock"
        # GNews free tier — no API key needed for basic queries
        resp = requests.get(
            "https://gnews.io/api/v4/search",
            params={
                "q": query,
                "lang": "en",
                "country": "in",
                "max": 6,
                "apikey": "demo",   # Public demo key — limited but works
            },
            timeout=5,
        )
        if resp.status_code == 200:
            articles = resp.json().get("articles", [])
            return [
                {
                    "title": a.get("title", ""),
                    "source": a.get("source", {}).get("name", ""),
                    "url": a.get("url", ""),
                    "published": a.get("publishedAt", "")[:10],
                    "sentiment": _infer_news_sentiment(a.get("title", "")),
                }
                for a in articles if a.get("title")
            ]
    except Exception:
        pass

    # Fallback — informational placeholders
    return [
        {"title": "Connect news API for live headlines", "source": "OptionsIQ",
         "url": "", "published": "", "sentiment": "NEUTRAL"},
    ]


def _infer_news_sentiment(headline: str) -> str:
    """Simple keyword-based sentiment on news headline."""
    h = headline.lower()
    bullish_words = ["rally", "surge", "gain", "rise", "bull", "high", "up", "positive", "growth", "strong"]
    bearish_words = ["fall", "drop", "crash", "bear", "low", "down", "negative", "weak", "sell", "decline"]
    b = sum(1 for w in bullish_words if w in h)
    s = sum(1 for w in bearish_words if w in h)
    if b > s: return "BULLISH"
    if s > b: return "BEARISH"
    return "NEUTRAL"


# ─── Market Breadth ───────────────────────────────────────────────────────────

def compute_market_breadth(
    calls_data: List[Dict], puts_data: List[Dict], spot: float
) -> Dict[str, Any]:
    """
    Compute option chain breadth metrics for market health assessment.
    """
    breadth = {
        "strikes_above_spot": 0,
        "strikes_below_spot": 0,
        "call_writers_above": 0,    # Resistance zones
        "put_writers_below": 0,     # Support zones
        "gamma_wall": 0.0,          # Strike with highest gamma (pinning zone)
        "gamma_wall_strength": "",
        "theta_decay_call": 0.0,
        "theta_decay_put": 0.0,
        "vega_exposure": 0.0,       # Total vega — market sensitivity to IV change
    }

    try:
        import pandas as pd
        df_c = pd.DataFrame(calls_data)
        df_p = pd.DataFrame(puts_data)

        for df in [df_c, df_p]:
            for col in ["strike_price","oi","gamma","theta","vega","volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if not df_c.empty:
            breadth["strikes_above_spot"] = int((df_c["strike_price"] > spot).sum())
            breadth["call_writers_above"]  = int(df_c[df_c["strike_price"] > spot]["oi"].sum())
            breadth["theta_decay_call"]    = round(float(df_c["theta"].sum()), 2)
            breadth["vega_exposure"]       = round(float(df_c["vega"].sum() + (df_p["vega"].sum() if not df_p.empty else 0)), 2)

            # Gamma wall — strike with highest combined gamma × OI
            df_c["gamma_weight"] = df_c["gamma"] * df_c["oi"]
            if df_c["gamma_weight"].max() > 0:
                gw_idx = df_c["gamma_weight"].idxmax()
                breadth["gamma_wall"] = float(df_c.loc[gw_idx, "strike_price"])
                gw_val = float(df_c.loc[gw_idx, "gamma_weight"])
                breadth["gamma_wall_strength"] = "STRONG" if gw_val > df_c["gamma_weight"].mean() * 3 else "MODERATE"

        if not df_p.empty:
            breadth["strikes_below_spot"] = int((df_p["strike_price"] < spot).sum())
            breadth["put_writers_below"]   = int(df_p[df_p["strike_price"] < spot]["oi"].sum())
            breadth["theta_decay_put"]     = round(float(df_p["theta"].sum()), 2)

    except Exception as e:
        breadth["error"] = str(e)

    return breadth
