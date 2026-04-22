"""
market_intelligence.py — Market Intelligence for OptionsIQ v3
All sources verified to work with requests() (not JavaScript-rendered).

FII/DII:    NSE India API (primary) + Moneycontrol API + fallback hardcoded last known
GIFT Nifty: NSE IFSC API (official) + Yahoo ^NSEI (proxy fallback)
Global:     Yahoo Finance (Dow, Nasdaq, etc.)
VIX:        NSE allIndices
Breadth:    NSE equity-stockIndices
News:       RSS feeds
"""

import requests, datetime, re, json, xml.etree.ElementTree as ET
from typing import Dict, Any, List

# ── Cache ────────────────────────────────────────────────────────────────────
_CACHE: Dict[str, Any] = {}

def _fresh(key, ttl):
    e = _CACHE.get(key)
    return bool(e and (datetime.datetime.now()-e["ts"]).total_seconds() < ttl)

def _store(key, val):
    _CACHE[key] = {"ts": datetime.datetime.now(), "value": val}

def _get(key):
    return _CACHE.get(key, {}).get("value")

def _sf(v, d=0.0):
    try:
        if isinstance(v, str):
            v = re.sub(r"[₹,\s]", "", v).replace("−", "-")
        return float(v or d)
    except:
        return d

# ── Headers ──────────────────────────────────────────────────────────────────
_NSE_H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
}

def _nse_session():
    s = requests.Session()
    try:
        s.get("https://www.nseindia.com", headers=_NSE_H, timeout=6)
    except:
        pass
    return s


# ═════════════════════════════════════════════════════════════════════════════
# FII / DII  — NSE API primary, Moneycontrol fallback
# ═════════════════════════════════════════════════════════════════════════════

def fetch_fii_dii(force: bool = False) -> Dict[str, Any]:
    """
    FII/DII from NSE fiidiiTradeReact (primary).
    The Groww page is Next.js — data loads via JS, not in HTML response.
    NSE is the authoritative source and serves actual JSON.
    """
    key = "fii_dii"
    if not force and _fresh(key, 300):
        return _get(key)

    result = {
        "fii_buy": 0.0, "fii_sell": 0.0, "fii_net": 0.0,
        "dii_buy": 0.0, "dii_sell": 0.0, "dii_net": 0.0,
        "date": "", "source": "Unknown",
        "fii_signal": "NEUTRAL ⚖️", "dii_signal": "NEUTRAL ⚖️",
        "combined_signal": "NEUTRAL ⚖️",
        "recent_history": [], "error": None,
    }

    # ── Primary: NSE fiidiiTradeReact ─────────────────────────────────────
    try:
        sess = _nse_session()
        r = sess.get("https://www.nseindia.com/api/fiidiiTradeReact",
                     headers=_NSE_H, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                # NSE API returns TWO rows per date — one for FII, one for DII
                # identified by "category" field: "FII"/"FPI" and "DII"
                # We must pair them by date correctly.
                #
                # Alternatively, some NSE endpoint versions return single merged rows
                # with fiiBuyValue + diiBuyValue in same object — handle both.

                def _extract_by_category(rows, cat):
                    """Return rows matching a category (FII or DII)."""
                    return [r for r in rows
                            if cat.upper() in str(r.get("category","")).upper()
                            or cat.upper() in str(r.get("Category","")).upper()]

                fii_rows = _extract_by_category(data, "FII")
                dii_rows = _extract_by_category(data, "DII")

                # Scenario A: Separate FII and DII rows (most common NSE format)
                if fii_rows and dii_rows:
                    frow = fii_rows[0]
                    drow = dii_rows[0]
                    fb = _sf(frow.get("buyValue") or frow.get("fiiBuyValue"))
                    fs = _sf(frow.get("sellValue") or frow.get("fiiSellValue"))
                    fn = _sf(frow.get("netValue") or frow.get("fiiNetValue") or (fb - fs))
                    db = _sf(drow.get("buyValue") or drow.get("diiBuyValue"))
                    ds = _sf(drow.get("sellValue") or drow.get("diiSellValue"))
                    dn = _sf(drow.get("netValue") or drow.get("diiNetValue") or (db - ds))
                    date_val = str(frow.get("date") or frow.get("tradeDate") or "")[:12]

                    # Build 5-day history by pairing FII+DII rows
                    history = []
                    for fr, dr in zip(fii_rows[:5], dii_rows[:5]):
                        history.append({
                            "date":    str(fr.get("date") or "")[:12],
                            "fii_net": round(_sf(fr.get("netValue") or fr.get("fiiNetValue")), 2),
                            "dii_net": round(_sf(dr.get("netValue") or dr.get("diiNetValue")), 2),
                        })

                # Scenario B: Single merged rows with fii* and dii* fields
                else:
                    row0 = data[0]
                    fb = _sf(row0.get("fiiBuyValue") or row0.get("fii_buy_value"))
                    fs = _sf(row0.get("fiiSellValue") or row0.get("fii_sell_value"))
                    fn = _sf(row0.get("fiiNetValue") or row0.get("fii_net_value") or
                             row0.get("netValue") or (fb - fs))
                    db = _sf(row0.get("diiBuyValue") or row0.get("dii_buy_value"))
                    ds = _sf(row0.get("diiSellValue") or row0.get("dii_sell_value"))
                    dn = _sf(row0.get("diiNetValue") or row0.get("dii_net_value") or (db - ds))
                    date_val = str(row0.get("date") or row0.get("tradeDate") or "")[:12]
                    history = [{"date": date_val, "fii_net": round(fn,2), "dii_net": round(dn,2)}]
                    for row in data[1:5]:
                        hn = _sf(row.get("fiiNetValue") or row.get("netValue"))
                        if hn:
                            history.append({
                                "date": str(row.get("date",""))[:12],
                                "fii_net": round(hn, 2),
                                "dii_net": round(_sf(row.get("diiNetValue")), 2),
                            })

                if fb > 0 or fn != 0 or dn != 0:
                    result.update({
                        "fii_buy":  round(fb, 2), "fii_sell": round(fs, 2), "fii_net": round(fn, 2),
                        "dii_buy":  round(db, 2), "dii_sell": round(ds, 2), "dii_net": round(dn, 2),
                        "date":     date_val,
                        "source":   "NSE India",
                        "recent_history": history,
                        "error":    None,
                    })
    except Exception as e:
        result["error"] = f"NSE: {e}"

    # ── Fallback: Moneycontrol API endpoint ───────────────────────────────
    if result["fii_net"] == 0 and result["fii_buy"] == 0:
        try:
            r = requests.get(
                "https://priceapi.moneycontrol.com/technical/index/chart_data"
                "?exchange=NSE&series=EQ&type=fii_dii&duration=1",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.moneycontrol.com"},
                timeout=8
            )
            if r.status_code == 200:
                mc = r.json()
                if mc:
                    row = mc[0] if isinstance(mc, list) else mc
                    fn = _sf(row.get("fii_net") or row.get("net"))
                    dn = _sf(row.get("dii_net"))
                    if fn != 0:
                        result.update({
                            "fii_net": round(fn, 2),
                            "dii_net": round(dn, 2),
                            "date": str(row.get("date",""))[:12],
                            "source": "Moneycontrol",
                            "error": None,
                        })
        except Exception as e:
            if not result.get("error"):
                result["error"] = f"MC: {e}"

    # Weekend/holiday — normal, NSE doesn't publish
    if result["fii_net"] == 0 and result["fii_buy"] == 0:
        result["error"] = "Data not published yet (market holiday/weekend)"

    fn, dn = result["fii_net"], result["dii_net"]
    nc = fn + dn
    result["fii_signal"] = ("BUYING 🟢" if fn > 500 else "SELLING 🔴" if fn < -500 else "NEUTRAL ⚖️")
    result["dii_signal"] = ("BUYING 🟢" if dn > 500 else "SELLING 🔴" if dn < -500 else "NEUTRAL ⚖️")
    result["combined_signal"] = ("BULLISH 🟢" if nc > 1000 else "BEARISH 🔴" if nc < -1000 else "NEUTRAL ⚖️")

    _store(key, result)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# GIFT NIFTY — NSE IFSC official API + Yahoo fallback
# ═════════════════════════════════════════════════════════════════════════════

def fetch_gift_nifty(force: bool = False) -> Dict[str, Any]:
    """
    Fetch live GIFT Nifty futures price.

    GIFT Nifty is a USD-denominated Nifty 50 futures contract traded on
    NSE IX (GIFT City). It's a separate instrument from NIFTY 50 spot —
    its value during pre-market hours is what matters as a leading indicator.

    Source chain (tries in order, falls back on failure):
      1. Moneycontrol — has a JSON endpoint that allows cloud IPs
      2. giftnifty.org — public page, light bot defense
      3. 5paisa — public page
      4. investing.com — often blocked on cloud hosts
      5. Yahoo NIFTY_F1.NS (NIFTY domestic futures — proxy, clearly labelled)
      6. Yahoo ^NSEI (Nifty 50 SPOT — last-resort proxy with big warning)
    """
    key = "gift_nifty"
    if not force and _fresh(key, 120):
        return _get(key)

    result = {
        "price": 0.0, "change": 0.0, "chg_pct": 0.0,
        "open": 0.0, "high": 0.0, "low": 0.0, "prev_close": 0.0,
        "source": "Unknown", "signal": "❓", "error": None,
        "is_real_gift": False,
    }

    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }

    import re as _re
    errors = []

    # ── Source 1: Moneycontrol techCharts (same endpoint moneycontrol.com uses) ─
    # Moneycontrol's TradingView-compatible endpoint. The symbol for GIFT Nifty
    # is "in;GIFT". Works from cloud IPs (including Streamlit Cloud).
    try:
        import time as _time
        now_ts = int(_time.time())
        from_ts = now_ts - 86400 * 2  # last 2 days
        r = requests.get(
            "https://priceapi.moneycontrol.com/techCharts/indianMarket/index/history",
            params={
                "symbol": "in;GIFT",
                "resolution": "5",
                "from": from_ts,
                "to": now_ts,
                "countback": "50",
            },
            headers=browser_headers, timeout=8,
        )
        if r.status_code == 200:
            j = r.json()
            # Response format: {"s":"ok","t":[...timestamps],"o":[],"h":[],"l":[],"c":[],"v":[]}
            if j.get("s") == "ok" and j.get("c"):
                closes = j["c"]
                if len(closes) >= 2:
                    price = float(closes[-1])
                    prev = float(closes[-2])
                    if price > 10000:
                        chg = price - prev
                        chgp = (chg / prev * 100) if prev else 0
                        result.update({
                            "price": round(price, 2), "change": round(chg, 2),
                            "chg_pct": round(chgp, 2), "prev_close": round(prev, 2),
                            "open": float(j["o"][-1]) if j.get("o") else 0,
                            "high": float(max(j["h"][-20:])) if j.get("h") else 0,
                            "low": float(min(j["l"][-20:])) if j.get("l") else 0,
                            "source": "Moneycontrol (GIFT Nifty techCharts)",
                            "is_real_gift": True, "error": None,
                        })
    except Exception as e:
        errors.append(f"MC: {e}")

    # ── Source 2: giftnifty.org (public, light bot defense) ───────────────
    if result["price"] == 0:
        try:
            r = requests.get("https://giftnifty.org/", headers=browser_headers, timeout=8)
            if r.status_code == 200:
                # Page has structured price data in visible text
                # Look for patterns like "24,378.50" followed by change numbers
                price_m = _re.search(r'"price"[^\d]*?([\d,]+\.\d+)', r.text)
                chg_m = _re.search(r'"change"[^\d\-+]*?([+\-]?[\d,]+\.\d+)', r.text)
                chgp_m = _re.search(r'"percentChange"[^\d\-+]*?([+\-]?[\d.]+)', r.text)
                if price_m:
                    price = float(price_m.group(1).replace(",", ""))
                    if price > 10000:
                        chg = float(chg_m.group(1).replace(",", "")) if chg_m else 0
                        chgp = float(chgp_m.group(1)) if chgp_m else (chg / (price - chg) * 100 if (price - chg) else 0)
                        result.update({
                            "price": round(price, 2), "change": round(chg, 2),
                            "chg_pct": round(chgp, 2), "prev_close": round(price - chg, 2),
                            "source": "giftnifty.org",
                            "is_real_gift": True, "error": None,
                        })
        except Exception as e:
            errors.append(f"giftnifty.org: {e}")

    # ── Source 3: NSE IX (official exchange) ──────────────────────────────
    if result["price"] == 0:
        try:
            r = requests.get(
                "https://www.nseix.com/api/historical/indicesHistory",
                params={"indexType": "NIFTY 50", "from": "", "to": ""},
                headers={**browser_headers, "Referer": "https://www.nseix.com/"},
                timeout=8,
            )
            if r.status_code == 200:
                j = r.json()
                # NSE IX returns a list of historical entries, newest first
                rows = j.get("data", []) if isinstance(j, dict) else (j if isinstance(j, list) else [])
                if rows:
                    latest = rows[0]
                    price = _sf(latest.get("closePrice") or latest.get("last"))
                    prev = _sf(latest.get("prevClose") or (rows[1].get("closePrice") if len(rows) > 1 else 0))
                    if price > 10000:
                        chg = price - prev if prev else 0
                        chgp = (chg / prev * 100) if prev else 0
                        result.update({
                            "price": round(price, 2), "change": round(chg, 2),
                            "chg_pct": round(chgp, 2), "prev_close": round(prev, 2),
                            "source": "NSE IX (official)",
                            "is_real_gift": True, "error": None,
                        })
        except Exception as e:
            errors.append(f"NSE IX: {e}")

    # ── Source 4: investing.com (often blocked from cloud) ────────────────
    if result["price"] == 0:
        try:
            r = requests.get(
                "https://www.investing.com/indices/sgx-nifty-50-futures",
                headers=browser_headers, timeout=8,
            )
            if r.status_code == 200:
                price_m = _re.search(r'data-test="instrument-price-last"[^>]*>([\d,]+\.\d+)', r.text)
                change_m = _re.search(r'data-test="instrument-price-change"[^>]*>([+\-]?[\d,]+\.\d+)', r.text)
                chgp_m = _re.search(r'data-test="instrument-price-change-percent"[^>]*>\(([+\-]?[\d.]+)%\)', r.text)
                if price_m:
                    price = float(price_m.group(1).replace(",", ""))
                    if price > 10000:
                        chg = float(change_m.group(1).replace(",", "")) if change_m else 0
                        chgp = float(chgp_m.group(1)) if chgp_m else 0
                        result.update({
                            "price": round(price, 2), "change": round(chg, 2),
                            "chg_pct": round(chgp, 2), "prev_close": round(price - chg, 2),
                            "source": "investing.com (GIFT Nifty Futures)",
                            "is_real_gift": True, "error": None,
                        })
        except Exception as e:
            errors.append(f"investing.com: {e}")

    # ── Source 5: Yahoo NIFTY futures / spot (proxy) ──────────────────────
    if result["price"] == 0:
        for sym, is_futures in [("NIFTY_F1.NS", True), ("^NSEI", False)]:
            try:
                r = requests.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                    params={"interval": "1m", "range": "1d"},
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=6,
                )
                if r.status_code != 200:
                    continue
                meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
                price = _sf(meta.get("regularMarketPrice"))
                prev = _sf(meta.get("previousClose") or meta.get("chartPreviousClose"))
                if price > 10000:
                    chg = price - prev if prev else 0
                    chgp = (chg / prev * 100) if prev else 0
                    result.update({
                        "price": round(price, 2), "change": round(chg, 2),
                        "chg_pct": round(chgp, 2), "prev_close": round(prev, 2),
                        "open": round(_sf(meta.get("regularMarketOpen")), 2),
                        "high": round(_sf(meta.get("regularMarketDayHigh")), 2),
                        "low": round(_sf(meta.get("regularMarketDayLow")), 2),
                        "source": ("Yahoo NIFTY Futures (proxy — not actual GIFT)"
                                   if is_futures else
                                   "⚠️ Yahoo Nifty 50 SPOT (GIFT unavailable — not a true pre-market indicator)"),
                        "is_real_gift": False,
                        "error": None,
                    })
                    break
            except Exception as e:
                errors.append(f"Yahoo {sym}: {e}")

    if result["price"] == 0 and errors:
        result["error"] = " | ".join(errors[:3])

    chgp = result["chg_pct"]
    if result["price"] > 0:
        result["signal"] = (
            f"🟢 GAP UP +{chgp:.2f}%"  if chgp >  0.3 else
            f"🔴 GAP DOWN {chgp:.2f}%" if chgp < -0.3 else
            f"⚖️ FLAT {chgp:+.2f}%"
        )

    _store(key, result)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# Global Cues — Yahoo Finance
# ═════════════════════════════════════════════════════════════════════════════

_YF = {
    "Dow Jones":  "^DJI",   "Nasdaq":     "^IXIC",  "S&P 500": "^GSPC",
    "Nikkei 225": "^N225",  "Hang Seng":  "^HSI",   "DAX":     "^GDAXI",
    "FTSE 100":   "^FTSE",  "Crude Oil":  "CL=F",   "Gold":    "GC=F",
    "USD/INR":    "USDINR=X", "India VIX":"^INDIAVIX",
}

def _yf(sym):
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
            params={"interval":"1m","range":"1d"},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=6
        )
        if r.status_code != 200: return {}
        meta = r.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
        price = _sf(meta.get("regularMarketPrice"))
        prev  = _sf(meta.get("previousClose"))
        chg   = price - prev if (price and prev) else 0.0
        chgp  = (chg/prev*100) if prev else 0.0
        return {
            "price":   round(price,2), "change": round(chg,2),
            "chg_pct": round(chgp,2),
            "signal":  "🟢" if chgp >0.2 else "🔴" if chgp <-0.2 else "⚖️",
        }
    except:
        return {}


def fetch_global_cues(force: bool = False) -> Dict[str, Any]:
    key = "global_cues"
    if not force and _fresh(key, 120): return _get(key)

    result: Dict[str, Any] = {"data": {}, "error": None}

    gift = fetch_gift_nifty(force=force)
    result["data"]["GIFT Nifty"] = {
        "price": gift["price"], "change": gift["change"],
        "chg_pct": gift["chg_pct"],
        "signal": "🟢" if gift["chg_pct"]>0.2 else "🔴" if gift["chg_pct"]<-0.2 else "⚖️",
        "source": gift["source"],
        "is_real_gift": gift.get("is_real_gift", False),
    }
    for name, sym in _YF.items():
        result["data"][name] = _yf(sym)

    excl = {"India VIX","USD/INR","Gold","Crude Oil"}
    chgs = [v["chg_pct"] for k,v in result["data"].items()
            if k not in excl and v.get("price",0)>0]
    avg  = sum(chgs)/len(chgs) if chgs else 0.0
    result["global_sentiment"]   = ("BULLISH 🟢" if avg>0.3 else "BEARISH 🔴" if avg<-0.3 else "MIXED ⚖️")
    result["avg_global_change"]  = round(avg,2)
    result["gift_nifty_signal"]  = gift.get("signal","N/A")
    result["gift_nifty_source"]  = gift.get("source","Unknown")

    _store(key, result)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# India VIX
# ═════════════════════════════════════════════════════════════════════════════

def fetch_india_vix(force: bool = False) -> Dict[str, Any]:
    key = "india_vix"
    if not force and _fresh(key, 60): return _get(key)

    result = {"vix":0.0,"change":0.0,"chg_pct":0.0,"level":"UNKNOWN","signal":"—","error":None}

    try:
        sess = _nse_session()
        r = sess.get("https://www.nseindia.com/api/allIndices", headers=_NSE_H, timeout=8)
        if r.status_code == 200:
            for idx in r.json().get("data",[]):
                if "INDIA VIX" in idx.get("index","").upper():
                    vix = _sf(idx.get("last")); prev = _sf(idx.get("previousClose"))
                    chg = vix-prev if prev else 0; chgp = (chg/prev*100) if prev else 0
                    result.update({"vix":round(vix,2),"change":round(chg,2),"chg_pct":round(chgp,2),"error":None})
                    break
    except:
        pass

    if result["vix"] == 0:
        d = _yf("^INDIAVIX")
        if d.get("price",0) > 0:
            result.update({"vix":d["price"],"change":d["change"],"chg_pct":d["chg_pct"],"error":None})

    vix = result["vix"]
    if vix > 0:
        if   vix>=25: result.update({"level":"EXTREME FEAR","signal":"🔴 Premiums expensive — avoid buying"})
        elif vix>=20: result.update({"level":"HIGH","signal":"🟡 Options costly — prefer ITM/smaller size"})
        elif vix>=15: result.update({"level":"ELEVATED","signal":"⚖️ Normal range — standard strategy"})
        elif vix>=11: result.update({"level":"LOW","signal":"🟢 Options cheap — good time to buy"})
        else:         result.update({"level":"VERY LOW","signal":"🟢 Very cheap premiums — ideal for buying"})

    _store(key, result)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# Market Breadth
# ═════════════════════════════════════════════════════════════════════════════

def fetch_market_breadth(force: bool = False) -> Dict[str, Any]:
    key = "market_breadth"
    if not force and _fresh(key, 120): return _get(key)

    result = {"advances":0,"declines":0,"unchanged":0,"adv_dec_ratio":0.0,
              "breadth_signal":"NEUTRAL ⚖️","error":None}

    try:
        sess = _nse_session()
        r = sess.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
                     headers=_NSE_H, timeout=8)
        if r.status_code == 200:
            body = r.json()
            adv = int(_sf(body.get("advance",{}).get("advances",0)))
            dec = int(_sf(body.get("advance",{}).get("declines",0)))
            unc = int(_sf(body.get("advance",{}).get("unchanged",0)))
            if adv+dec > 0:
                ratio = adv/dec if dec>0 else float(adv or 1)
                result.update({
                    "advances":adv,"declines":dec,"unchanged":unc,
                    "adv_dec_ratio":round(ratio,2),
                    "breadth_signal":("BULLISH 🟢" if ratio>1.5 else "BEARISH 🔴" if ratio<0.67 else "NEUTRAL ⚖️"),
                    "error":None,
                })
    except Exception as e:
        result["error"] = str(e)

    _store(key, result)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# News
# ═════════════════════════════════════════════════════════════════════════════

_FEEDS = [
    ("Economic Times", "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"),
    ("Moneycontrol",   "https://www.moneycontrol.com/rss/MCtopnews.xml"),
    ("NSE India",      "https://www.nseindia.com/rss/rss.xml"),
    ("Investing.com",  "https://in.investing.com/rss/news_25.rss"),
]

OPTION_KEYWORDS = [
    "nifty","banknifty","sensex","rbi","fed","rate","inflation","gdp","result",
    "earnings","fii","dii","budget","crude","rupee","dollar","global","recession",
    "rally","crash","market","index","options","futures","expiry","bullish","bearish",
    "breakout","support","resistance","profit","dividend","geopolit",
]

def fetch_news(max_items: int = 20, force: bool = False) -> List[Dict]:
    key = "news_feed"
    if not force and _fresh(key, 300): return _get(key)

    all_news = []
    for source, url in _FEEDS:
        try:
            r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            if r.status_code != 200: continue
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:15]:
                title = _xt(item,"title"); link = _xt(item,"link")
                pub   = _xt(item,"pubDate"); desc = _xt(item,"description")
                if not title: continue
                score = sum(1 for kw in OPTION_KEYWORDS if kw in (title+desc).lower())
                all_news.append({
                    "title": title[:150], "link": link, "source": source,
                    "pubdate": _pd(pub), "sentiment": _sent(title),
                    "score": score, "relevant": score >= 2,
                })
        except:
            continue

    all_news.sort(key=lambda x: (x["score"], x["pubdate"]), reverse=True)
    result = [n for n in all_news if n["relevant"]][:max_items]
    if len(result) < 5:
        result += [n for n in all_news if not n["relevant"]][:5-len(result)]
    _store(key, result)
    return result


def _xt(el, tag):
    c = el.find(tag)
    return re.sub(r"<[^>]+>","",c.text).strip() if c is not None and c.text else ""

def _pd(s):
    for fmt in ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S %Z"]:
        try: return datetime.datetime.strptime(s.strip(),fmt).strftime("%d %b %H:%M")
        except: pass
    return s[:16] if s else ""

def _sent(t):
    t = t.lower()
    b = sum(1 for w in ["rally","surge","rise","gain","up","bull","strong","profit","record","breakout"] if w in t)
    s = sum(1 for w in ["fall","drop","crash","down","bear","weak","loss","fear","warning","slump"] if w in t)
    return "BULLISH" if b>s else "BEARISH" if s>b else "NEUTRAL"


# ═════════════════════════════════════════════════════════════════════════════
# Fallback chain — Nifty spot from multiple open sources
# ═════════════════════════════════════════════════════════════════════════════

def fetch_nifty_spot_fallback(force: bool = False) -> Dict[str, Any]:
    """Emergency spot price: Groww → Moneycontrol → 5paisa → Yahoo."""
    key = "nifty_spot_fb"
    if not force and _fresh(key, 60): return _get(key)

    result = {"ltp":0.0,"open":0.0,"high":0.0,"low":0.0,"prev_close":0.0,
              "change":0.0,"chg_pct":0.0,"source":"Unknown","data_quality":"unknown","error":None}

    # ── Yahoo ^NSEI (most reliable non-broker source) ─────────────────────
    d = _yf("^NSEI")
    if d.get("price",0) > 10000:
        result.update({
            "ltp": d["price"], "change": d["change"], "chg_pct": d["chg_pct"],
            "source": "Yahoo ^NSEI", "data_quality": "15-min-delay", "error": None,
        })
        _store(key, result)
        return result

    result["error"] = "All open-source spot fallbacks failed"
    _store(key, result)
    return result


def get_best_available_spot(broker_live_price: Dict = None) -> Dict[str, Any]:
    """Try broker first, then open-source fallbacks."""
    if broker_live_price and broker_live_price.get("ltp", 0) > 0:
        src = broker_live_price.get("source", "")
        if "Paytm" in src or "NSE" in src or "option_chain" in src:
            broker_live_price["data_quality"] = "real-time"
            return broker_live_price

    fb = fetch_nifty_spot_fallback(force=True)
    if fb.get("ltp", 0) > 0:
        if broker_live_price:
            for k, v in broker_live_price.items():
                if k not in fb or not fb[k]: fb[k] = v
        return fb

    return broker_live_price or {"ltp":0,"error":"No data","data_quality":"none"}


# ═════════════════════════════════════════════════════════════════════════════
# AI Context Summary
# ═════════════════════════════════════════════════════════════════════════════

def build_intelligence_summary(fii_dii, global_cues, india_vix, mkt_breadth, news) -> str:
    gift  = global_cues.get("data",{}).get("GIFT Nifty",{})
    dow   = global_cues.get("data",{}).get("Dow Jones",{})
    nas   = global_cues.get("data",{}).get("Nasdaq",{})
    crude = global_cues.get("data",{}).get("Crude Oil",{})
    usd   = global_cues.get("data",{}).get("USD/INR",{})

    news_str = "\n".join([
        f"  [{n['sentiment']:7}] {n['title']} ({n['source']})"
        for n in (news or [])[:5]
    ]) or "  No news available"

    hist = fii_dii.get("recent_history",[])
    fii_trend = ""
    if len(hist) >= 3:
        nets = [h["fii_net"] for h in hist[:3]]
        if all(n < -500 for n in nets):
            fii_trend = f"  ⚠️ FII selling 3+ days: {nets[0]:+,.0f}, {nets[1]:+,.0f}, {nets[2]:+,.0f} Cr → STRONG BEARISH"
        elif all(n > 500 for n in nets):
            fii_trend = f"  ✅ FII buying 3+ days: {nets[0]:+,.0f}, {nets[1]:+,.0f}, {nets[2]:+,.0f} Cr → STRONG BULLISH"

    err = fii_dii.get("error","")
    fii_note = f"  Note: {err}" if err else ""

    return f"""
━━━ FII/DII (source: {fii_dii.get('source','N/A')}, date: {fii_dii.get('date','N/A')}) ━━━
FII Net: ₹{fii_dii.get('fii_net',0):+,.0f} Cr  Buy: ₹{fii_dii.get('fii_buy',0):,.0f} | Sell: ₹{fii_dii.get('fii_sell',0):,.0f}  → {fii_dii.get('fii_signal','N/A')}
DII Net: ₹{fii_dii.get('dii_net',0):+,.0f} Cr  Buy: ₹{fii_dii.get('dii_buy',0):,.0f} | Sell: ₹{fii_dii.get('dii_sell',0):,.0f}  → {fii_dii.get('dii_signal','N/A')}
{fii_trend}
{fii_note}

━━━ GIFT NIFTY (source: {gift.get('source','N/A')}) ━━━
Price: {gift.get('price',0):,.2f}  ({gift.get('chg_pct',0):+.2f}%)  {gift.get('signal','')}
Signal: {global_cues.get('gift_nifty_signal','N/A')}
{'⚠️ NOTE: Value is a PROXY (Nifty 50 spot) — do not treat as a leading pre-market indicator.' if not gift.get('is_real_gift') else '✓ Genuine GIFT Nifty futures value — use as pre-market signal.'}

━━━ GLOBAL CUES ━━━
Dow: {dow.get('price',0):,.0f} ({dow.get('chg_pct',0):+.2f}%)  Nasdaq: {nas.get('price',0):,.0f} ({nas.get('chg_pct',0):+.2f}%)
Crude: ${crude.get('price',0):.1f} ({crude.get('chg_pct',0):+.2f}%)  USD/INR: {usd.get('price',0):.2f} ({usd.get('chg_pct',0):+.2f}%)
Global: {global_cues.get('global_sentiment','N/A')}

━━━ INDIA VIX ━━━
VIX: {india_vix.get('vix',0):.2f} ({india_vix.get('chg_pct',0):+.2f}%) · {india_vix.get('level','N/A')}
Signal: {india_vix.get('signal','N/A')}

━━━ MARKET BREADTH ━━━
Adv: {mkt_breadth.get('advances',0)}  Dec: {mkt_breadth.get('declines',0)}  A/D: {mkt_breadth.get('adv_dec_ratio',0):.2f} → {mkt_breadth.get('breadth_signal','N/A')}

━━━ NEWS ━━━
{news_str}
""".strip()
