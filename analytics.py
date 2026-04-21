"""
analytics.py — Core analytics engine for Options Intelligence Platform
All computation logic is isolated here for testability and reuse.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, List


# ─── 1. MARKET SNAPSHOT ──────────────────────────────────────────────────────

def compute_market_snapshot(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame
) -> Dict[str, Any]:
    """
    Derive market-level metrics from option chain data.
    Spot price: read directly from spot_price field in records (confirmed Paytm field).
    """
    snapshot = {
        "spot_price": 0.0,
        "trend": "NEUTRAL",
        "trend_strength": "WEAK",
        "total_call_volume": 0,
        "total_put_volume": 0,
        "volume_bias": "NEUTRAL",
        "total_call_oi": 0,
        "total_put_oi": 0,
        "avg_call_iv": 0.0,
        "avg_put_iv": 0.0,
    }

    try:
        # Method 1: spot_price field directly from Paytm records
        if not df_calls.empty and "spot_price" in df_calls.columns:
            sp = pd.to_numeric(df_calls["spot_price"], errors="coerce").dropna()
            sp = sp[sp > 100]
            if not sp.empty:
                snapshot["spot_price"] = round(float(sp.iloc[0]), 2)

        # Method 2: put-call parity fallback
        if snapshot["spot_price"] == 0 and not df_calls.empty and not df_puts.empty:
            merged = pd.merge(
                df_calls[["strike_price", "price"]].rename(columns={"price": "call_price"}),
                df_puts[["strike_price", "price"]].rename(columns={"price": "put_price"}),
                on="strike_price",
            )
            if not merged.empty:
                merged["price_diff"] = abs(merged["call_price"] - merged["put_price"])
                atm_row = merged.loc[merged["price_diff"].idxmin()]
                spot = float(atm_row["strike_price"]) + (
                    float(atm_row["call_price"]) - float(atm_row["put_price"])
                )
                snapshot["spot_price"] = round(spot, 2)

        # Volume metrics
        if not df_calls.empty:
            vol_col = "volume" if "volume" in df_calls.columns else "traded_vol"
            snapshot["total_call_volume"] = int(pd.to_numeric(df_calls.get(vol_col, 0), errors="coerce").fillna(0).sum())
            snapshot["total_call_oi"]     = int(pd.to_numeric(df_calls["oi"], errors="coerce").fillna(0).sum())
        if not df_puts.empty:
            vol_col = "volume" if "volume" in df_puts.columns else "traded_vol"
            snapshot["total_put_volume"]  = int(pd.to_numeric(df_puts.get(vol_col, 0), errors="coerce").fillna(0).sum())
            snapshot["total_put_oi"]      = int(pd.to_numeric(df_puts["oi"], errors="coerce").fillna(0).sum())

        # Volume bias
        total_vol = snapshot["total_call_volume"] + snapshot["total_put_volume"]
        if total_vol > 0:
            call_vol_pct = snapshot["total_call_volume"] / total_vol
            if call_vol_pct > 0.55:
                snapshot["volume_bias"] = "CALL-HEAVY (Bearish)"
            elif call_vol_pct < 0.45:
                snapshot["volume_bias"] = "PUT-HEAVY (Bullish)"
            else:
                snapshot["volume_bias"] = "BALANCED"

        # Trend from OI imbalance + net change
        if not df_calls.empty and not df_puts.empty:
            net_call_chg = df_calls["oi_perc_chg"].mean() if "oi_perc_chg" in df_calls else 0
            net_put_chg = df_puts["oi_perc_chg"].mean() if "oi_perc_chg" in df_puts else 0
            diff = net_put_chg - net_call_chg

            if diff > 3:
                snapshot["trend"] = "BULLISH"
                snapshot["trend_strength"] = "STRONG" if diff > 8 else "MODERATE"
            elif diff < -3:
                snapshot["trend"] = "BEARISH"
                snapshot["trend_strength"] = "STRONG" if diff < -8 else "MODERATE"
            else:
                snapshot["trend"] = "NEUTRAL"
                snapshot["trend_strength"] = "WEAK"

    except Exception:
        pass  # Return safe defaults on error

    return snapshot


# ─── 2. ATM STRIKE IDENTIFICATION ───────────────────────────────────────────

def identify_atm_strike(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame, spot_price: float
) -> float:
    """
    Find the ATM (At-The-Money) strike closest to spot price.
    Uses available strikes from CALL side preferentially.
    """
    if spot_price <= 0:
        spot_price = 22500.0  # Fallback for demo

    strikes = []
    if not df_calls.empty and "strike_price" in df_calls.columns:
        strikes = df_calls["strike_price"].dropna().tolist()
    elif not df_puts.empty and "strike_price" in df_puts.columns:
        strikes = df_puts["strike_price"].dropna().tolist()

    if not strikes:
        return round(spot_price / 50) * 50  # Nearest 50

    strikes_arr = np.array(strikes)
    atm = strikes_arr[np.argmin(np.abs(strikes_arr - spot_price))]
    return float(atm)


# ─── 3. ITM / OTM CLASSIFICATION ────────────────────────────────────────────

def classify_strikes(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame, atm_strike: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Classify each strike as ITM, ATM, or OTM for both CALL and PUT.
    CALL: ITM = strike < spot, OTM = strike > spot
    PUT:  ITM = strike > spot, OTM = strike < spot
    """

    def _classify_calls(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["moneyness"] = df["strike_price"].apply(
            lambda s: "ATM" if s == atm_strike
            else ("ITM" if s < atm_strike else "OTM")
        )
        return df

    def _classify_puts(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["moneyness"] = df["strike_price"].apply(
            lambda s: "ATM" if s == atm_strike
            else ("ITM" if s > atm_strike else "OTM")
        )
        return df

    return _classify_calls(df_calls), _classify_puts(df_puts)


# ─── 4. PCR ANALYSIS ────────────────────────────────────────────────────────

def compute_pcr(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame
) -> Dict[str, Any]:
    """
    Compute Put-Call Ratio (PCR) by OI and volume.
    Interpret:
        PCR > 1.2 → Bullish (heavy put writing = market expects upside)
        PCR < 0.8 → Bearish (heavy call writing = market expects downside)
        0.8–1.2   → Neutral
    """
    result = {
        "pcr_oi": 0.0,
        "pcr_volume": 0.0,
        "interpretation": "NEUTRAL",
        "signal": "⚖️",
        "total_put_oi": 0,
        "total_call_oi": 0,
        "total_put_vol": 0,
        "total_call_vol": 0,
    }

    try:
        call_oi = float(df_calls["oi"].sum()) if not df_calls.empty else 0
        put_oi = float(df_puts["oi"].sum()) if not df_puts.empty else 0
        call_vol = float(df_calls["volume"].sum()) if not df_calls.empty else 0
        put_vol = float(df_puts["volume"].sum()) if not df_puts.empty else 0

        result["total_call_oi"] = int(call_oi)
        result["total_put_oi"] = int(put_oi)
        result["total_call_vol"] = int(call_vol)
        result["total_put_vol"] = int(put_vol)

        result["pcr_oi"] = round(put_oi / call_oi, 3) if call_oi > 0 else 0.0
        result["pcr_volume"] = round(put_vol / call_vol, 3) if call_vol > 0 else 0.0

        pcr = result["pcr_oi"]
        if pcr > 1.5:
            result["interpretation"] = "STRONGLY BULLISH"
            result["signal"] = "🔥"
        elif pcr > 1.2:
            result["interpretation"] = "BULLISH"
            result["signal"] = "🟢"
        elif pcr > 0.8:
            result["interpretation"] = "NEUTRAL"
            result["signal"] = "⚖️"
        elif pcr > 0.5:
            result["interpretation"] = "BEARISH"
            result["signal"] = "🔴"
        else:
            result["interpretation"] = "STRONGLY BEARISH"
            result["signal"] = "💀"

    except Exception:
        pass

    return result


# ─── 5. SUPPORT & RESISTANCE ─────────────────────────────────────────────────

def find_support_resistance(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame,
    spot: float = 0, nearby_range: int = 500
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Find INTRADAY-RELEVANT support and resistance levels.

    Strategy:
    1. Filter strikes within ±500 points of spot (intraday relevant range)
    2. Among those, find highest PUT OI (support) and CALL OI (resistance)
    3. This gives realistic levels achievable in 15-60 minute trades

    Falls back to global max OI if no levels found in nearby range.
    """
    support    = {"strike": 0, "oi": 0, "price": 0, "volume": 0, "type": "nearby"}
    resistance = {"strike": 0, "oi": 0, "price": 0, "volume": 0, "type": "nearby"}

    def _best_level(df: pd.DataFrame, spot: float, side: str) -> Dict:
        if df.empty: return {"strike": 0, "oi": 0, "price": 0, "volume": 0}
        df = df.copy()
        df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce")
        df["oi"]           = pd.to_numeric(df["oi"],           errors="coerce").fillna(0)

        # Step 1: nearby strikes within ±500 pts of spot
        if spot > 0:
            nearby = df[abs(df["strike_price"] - spot) <= nearby_range].copy()
        else:
            nearby = df.copy()

        # Step 2: For support (PUT), take highest OI strike BELOW spot
        #         For resistance (CALL), take highest OI strike ABOVE spot
        if side == "support" and spot > 0:
            candidates = nearby[nearby["strike_price"] <= spot]
        elif side == "resistance" and spot > 0:
            candidates = nearby[nearby["strike_price"] >= spot]
        else:
            candidates = nearby

        if candidates.empty:
            candidates = nearby   # relax direction filter
        if candidates.empty:
            candidates = df       # relax range filter

        if candidates.empty:
            return {"strike": 0, "oi": 0, "price": 0, "volume": 0}

        idx = candidates["oi"].idxmax()
        row = candidates.loc[idx]
        return {
            "strike": float(row["strike_price"]),
            "oi":     int(row["oi"]),
            "price":  float(row.get("price",  0) or 0),
            "volume": int(float(row.get("volume", 0) or 0)),
        }

    try:
        support    = _best_level(df_puts,  spot, "support")
        resistance = _best_level(df_calls, spot, "resistance")
    except Exception:
        pass

    return support, resistance


def find_intraday_levels(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame,
    spot: float, timeframe_pts: int = 150
) -> Dict[str, Any]:
    """
    Find immediate intraday levels within ±150 points of spot.
    These are the levels achievable in a 15-30 min trade.
    Returns dict with immediate_support, immediate_resistance,
    strong_support, strong_resistance for UI display.
    """
    result = {
        "immediate_support":    0,  # Nearest PUT OI within 150 pts below
        "immediate_resistance": 0,  # Nearest CALL OI within 150 pts above
        "strong_support":       0,  # Highest PUT OI within 300 pts below
        "strong_resistance":    0,  # Highest CALL OI within 300 pts above
        "trading_range_low":    0,
        "trading_range_high":   0,
        "range_width":          0,
    }
    try:
        for df in [df_calls, df_puts]:
            if not df.empty:
                for col in ["strike_price", "oi"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Immediate levels (±150 pts — 15-30 min achievable)
        if not df_puts.empty and spot > 0:
            near_puts = df_puts[
                (df_puts["strike_price"] < spot) &
                (df_puts["strike_price"] >= spot - timeframe_pts)
            ]
            if not near_puts.empty:
                result["immediate_support"] = float(
                    near_puts.loc[near_puts["oi"].idxmax(), "strike_price"])

        if not df_calls.empty and spot > 0:
            near_calls = df_calls[
                (df_calls["strike_price"] > spot) &
                (df_calls["strike_price"] <= spot + timeframe_pts)
            ]
            if not near_calls.empty:
                result["immediate_resistance"] = float(
                    near_calls.loc[near_calls["oi"].idxmax(), "strike_price"])

        # Strong levels (±300 pts — 60 min achievable)
        if not df_puts.empty and spot > 0:
            strong_puts = df_puts[
                (df_puts["strike_price"] < spot) &
                (df_puts["strike_price"] >= spot - 300)
            ]
            if not strong_puts.empty:
                result["strong_support"] = float(
                    strong_puts.loc[strong_puts["oi"].idxmax(), "strike_price"])

        if not df_calls.empty and spot > 0:
            strong_calls = df_calls[
                (df_calls["strike_price"] > spot) &
                (df_calls["strike_price"] <= spot + 300)
            ]
            if not strong_calls.empty:
                result["strong_resistance"] = float(
                    strong_calls.loc[strong_calls["oi"].idxmax(), "strike_price"])

        # Trading range
        lo = result["strong_support"]    or result["immediate_support"]
        hi = result["strong_resistance"] or result["immediate_resistance"]
        result["trading_range_low"]  = lo
        result["trading_range_high"] = hi
        result["range_width"] = hi - lo if hi > lo else 0

    except Exception:
        pass
    return result


def find_top_oi_levels(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame, top_n: int = 5
) -> Dict[str, List[Dict]]:
    """Find top N strikes by OI for both calls and puts."""
    result = {"calls": [], "puts": []}

    try:
        if not df_calls.empty:
            top_calls = (
                df_calls.nlargest(top_n, "oi")[
                    ["strike_price", "oi", "price", "volume", "oi_perc_chg"]
                ]
                .to_dict("records")
            )
            result["calls"] = top_calls

        if not df_puts.empty:
            top_puts = (
                df_puts.nlargest(top_n, "oi")[
                    ["strike_price", "oi", "price", "volume", "oi_perc_chg"]
                ]
                .to_dict("records")
            )
            result["puts"] = top_puts

    except Exception:
        pass

    return result


# ─── 6. OI BUILD-UP ANALYSIS ─────────────────────────────────────────────────

def analyze_oi_buildup(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame
) -> Dict[str, List[Dict]]:
    """
    Classify OI change into four buildup patterns:
    ┌─────────────────┬─────────────────┬────────────────┐
    │   Pattern       │ OI Change       │ Price Change   │
    ├─────────────────┼─────────────────┼────────────────┤
    │ Long Buildup    │ Increasing ↑    │ Increasing ↑   │
    │ Short Buildup   │ Increasing ↑    │ Decreasing ↓   │
    │ Short Covering  │ Decreasing ↓    │ Increasing ↑   │
    │ Long Unwinding  │ Decreasing ↓    │ Decreasing ↓   │
    └─────────────────┴─────────────────┴────────────────┘
    """
    buildup = {
        "long_buildup": [],
        "short_buildup": [],
        "short_covering": [],
        "long_unwinding": [],
    }

    def _classify(df: pd.DataFrame, side: str) -> None:
        if df.empty:
            return
        for _, row in df.iterrows():
            try:
                oi_chg    = float(row.get("oi_perc_chg", 0) or 0)
                price_chg = float(row.get("net_chg", 0) or 0)
                strike    = float(row.get("strike_price", 0))
                oi        = float(row.get("oi", 0) or 0)
                price     = float(row.get("price", 0) or 0)
            except (TypeError, ValueError):
                continue

            entry = {
                "strike": strike,
                "side": side,
                "oi": oi,
                "price": price,
                "oi_chg": round(oi_chg, 2),
                "price_chg": round(price_chg, 2),
            }

            if oi_chg > 2 and price_chg > 0:
                buildup["long_buildup"].append(entry)
            elif oi_chg > 2 and price_chg <= 0:
                buildup["short_buildup"].append(entry)
            elif oi_chg <= 0 and price_chg > 0:
                buildup["short_covering"].append(entry)
            elif oi_chg <= 0 and price_chg <= 0:
                buildup["long_unwinding"].append(entry)

    _classify(df_calls, "CALL")
    _classify(df_puts, "PUT")

    # Sort each by OI descending, take top 10
    for key in buildup:
        buildup[key] = sorted(buildup[key], key=lambda x: x["oi"], reverse=True)[:10]

    return buildup


# ─── 7. SMART MONEY TRACKING ─────────────────────────────────────────────────

def track_smart_money(
    df_calls: pd.DataFrame, df_puts: pd.DataFrame
) -> Dict[str, Any]:
    """
    Identify strikes with unusual combination of high OI + high volume.
    Smart money footprint: large OI + large volume at a specific strike
    signals institutional activity.
    """
    result = {
        "call_accumulation": [],
        "put_accumulation": [],
        "call_signal": "None",
        "put_signal": "None",
    }

    def _find_accumulation(df: pd.DataFrame) -> Tuple[List[Dict], str]:
        if df.empty:
            return [], "No data"

        # Z-score based anomaly detection for OI × Volume
        df = df.copy()
        df["oi_vol_score"] = df["oi"] * df["volume"]
        mean_score = df["oi_vol_score"].mean()
        std_score = df["oi_vol_score"].std()

        if std_score == 0:
            return [], "Insufficient variance"

        df["z_score"] = (df["oi_vol_score"] - mean_score) / std_score
        high_activity = df[df["z_score"] > 1.2].nlargest(5, "z_score")

        records = []
        for _, row in high_activity.iterrows():
            records.append({
                "strike": float(row["strike_price"]),
                "oi": int(row["oi"]),
                "volume": int(row["volume"]),
                "z_score": round(float(row["z_score"]), 2),
                "price": float(row.get("price", 0)),
            })

        signal = "STRONG ACCUMULATION" if len(records) >= 3 else "MODERATE ACTIVITY"
        return records, signal

    result["call_accumulation"], result["call_signal"] = _find_accumulation(df_calls)
    result["put_accumulation"], result["put_signal"] = _find_accumulation(df_puts)

    return result


# ─── 8. TRADE DECISION ENGINE ────────────────────────────────────────────────

def generate_trade_decision(
    pcr_data: Dict[str, Any],
    support: Dict[str, Any],
    resistance: Dict[str, Any],
    buildup: Dict[str, List[Dict]],
    snapshot: Dict[str, Any],
    atm_strike: float,
) -> Dict[str, Any]:
    """
    Rule-based trade decision engine.
    Generates: action, confidence, reasoning, entry/target/SL suggestions.

    Scoring system (total 100 points):
    - PCR signal:          30 pts
    - Price vs S/R:        25 pts
    - OI buildup:          25 pts
    - Volume confirmation: 20 pts
    """
    score = 0
    reasons = []
    bullish_pts = 0
    bearish_pts = 0

    pcr = pcr_data.get("pcr_oi", 1.0)
    spot = snapshot.get("spot_price", atm_strike)
    support_strike = support.get("strike", 0)
    resistance_strike = resistance.get("strike", 0)
    volume_bias = snapshot.get("volume_bias", "BALANCED")

    # ── Rule 1: PCR (30 pts) ──────────────────────────────────────────────
    if pcr > 1.5:
        bullish_pts += 30
        reasons.append(f"🟢 PCR={pcr:.2f} (>1.5): Strong put writing, bullish signal.")
    elif pcr > 1.2:
        bullish_pts += 20
        reasons.append(f"🟢 PCR={pcr:.2f} (>1.2): Bullish market sentiment.")
    elif pcr > 0.9:
        reasons.append(f"⚖️ PCR={pcr:.2f} (0.9–1.2): Neutral sentiment.")
    elif pcr > 0.6:
        bearish_pts += 20
        reasons.append(f"🔴 PCR={pcr:.2f} (<0.9): Bearish, heavy call buying.")
    else:
        bearish_pts += 30
        reasons.append(f"🔴 PCR={pcr:.2f} (<0.6): Strongly bearish.")

    # ── Rule 2: Price vs Support/Resistance (25 pts) ──────────────────────
    if spot > 0 and resistance_strike > 0 and support_strike > 0:
        range_width = resistance_strike - support_strike
        if range_width > 0:
            pos_in_range = (spot - support_strike) / range_width

            if pos_in_range < 0.25:
                bullish_pts += 25
                reasons.append(
                    f"🟢 Spot ({spot:.0f}) near support ({support_strike:.0f}): Bounce opportunity."
                )
            elif pos_in_range > 0.75:
                bearish_pts += 25
                reasons.append(
                    f"🔴 Spot ({spot:.0f}) near resistance ({resistance_strike:.0f}): Selling pressure."
                )
            elif 0.4 < pos_in_range < 0.6:
                reasons.append(
                    f"⚖️ Spot ({spot:.0f}) mid-range. Wait for breakout."
                )
            else:
                bullish_pts += 10 if pos_in_range < 0.5 else 0
                bearish_pts += 10 if pos_in_range > 0.5 else 0

    # ── Rule 3: OI Buildup (25 pts) ──────────────────────────────────────
    n_long = len(buildup.get("long_buildup", []))
    n_short = len(buildup.get("short_buildup", []))
    n_covering = len(buildup.get("short_covering", []))
    n_unwinding = len(buildup.get("long_unwinding", []))

    if n_long > n_short and n_long > 3:
        bullish_pts += 25
        reasons.append(f"🟢 Long buildup dominant ({n_long} strikes): Buyers active.")
    elif n_short > n_long and n_short > 3:
        bearish_pts += 25
        reasons.append(f"🔴 Short buildup dominant ({n_short} strikes): Sellers in control.")
    elif n_covering > n_unwinding and n_covering > 3:
        bullish_pts += 15
        reasons.append(f"🟡 Short covering active ({n_covering} strikes): Upside likely.")
    elif n_unwinding > n_covering and n_unwinding > 3:
        bearish_pts += 15
        reasons.append(f"🟡 Long unwinding active ({n_unwinding} strikes): Weakness visible.")
    else:
        reasons.append("⚖️ OI buildup mixed — no clear directional bias.")

    # ── Rule 4: Volume Confirmation (20 pts) ─────────────────────────────
    if "PUT-HEAVY" in volume_bias:
        bullish_pts += 20
        reasons.append("🟢 Volume skewed to PUTs: Institutions buying puts as hedge (bullish for market).")
    elif "CALL-HEAVY" in volume_bias:
        bearish_pts += 20
        reasons.append("🔴 Volume skewed to CALLs: Call buying surge, potential cap on upside.")
    else:
        reasons.append("⚖️ Balanced call/put volume — no volume confirmation.")

    # ── Final Decision ────────────────────────────────────────────────────
    total = bullish_pts + bearish_pts
    confidence = min(100, int(max(bullish_pts, bearish_pts)))

    if bullish_pts > bearish_pts + 20:
        action = "BUY CALL"
        action_emoji = "📈"
        color = "green"
    elif bearish_pts > bullish_pts + 20:
        action = "BUY PUT"
        action_emoji = "📉"
        color = "red"
    else:
        action = "NO TRADE"
        action_emoji = "🚫"
        color = "gray"
        confidence = max(0, 100 - abs(bullish_pts - bearish_pts) * 2)

    # Entry suggestion
    entry_strike = atm_strike
    if action == "BUY CALL":
        entry_strike = atm_strike  # ATM or 1 OTM call
        target = resistance_strike if resistance_strike > 0 else spot * 1.01
        sl = support_strike if support_strike > 0 else spot * 0.99
    elif action == "BUY PUT":
        entry_strike = atm_strike
        target = support_strike if support_strike > 0 else spot * 0.99
        sl = resistance_strike if resistance_strike > 0 else spot * 1.01
    else:
        target = 0
        sl = 0

    return {
        "action": action,
        "action_emoji": action_emoji,
        "color": color,
        "confidence": confidence,
        "bullish_score": bullish_pts,
        "bearish_score": bearish_pts,
        "reasons": reasons,
        "entry_strike": entry_strike,
        "target": target,
        "stop_loss": sl,
    }
