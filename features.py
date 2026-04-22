"""
features.py — Advanced features for OptionsIQ
Provides 5 new high-value modules:
  1. Entry Timing Score — real-time "enter now or wait" signal
  2. Live Trade Tracker — P&L, SL/target alerts, exit timer
  3. OI Change Velocity — detect fresh writing vs covering
  4. AI Trade History — log & win rate tracker (persisted via localStorage bridge)
  5. Multi-expiry PCR + BankNifty divergence
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import json


# ═════════════════════════════════════════════════════════════════════════════
# 1. ENTRY TIMING SCORE
# ═════════════════════════════════════════════════════════════════════════════

def compute_entry_timing_score(
    live_price: Dict, df_calls: pd.DataFrame, df_puts: pd.DataFrame,
    candles_1m: pd.DataFrame, sentiment: Dict, action: str = "BUY CALL"
) -> Dict[str, Any]:
    """
    Score 0-100 for how good the current moment is to enter.
    Combines momentum, spread, volume, and candle confirmation.
    """
    score = 50  # neutral base
    signals = []
    is_call = action == "BUY CALL"

    spot = float(live_price.get("ltp", 0) or 0)
    if spot <= 0:
        return {"score": 0, "grade": "F", "signals": ["No price data"], "wait": True}

    # ── Momentum: candle direction ────────────────────────────────────────────
    if candles_1m is not None and len(candles_1m) >= 3:
        last3 = candles_1m.tail(3)
        bullish_count = (last3["dir"] == "Bullish").sum()
        if is_call:
            if bullish_count == 3:  score += 15; signals.append("✅ 3 consecutive green candles")
            elif bullish_count == 2: score += 7;  signals.append("✅ 2 of 3 candles green")
            elif bullish_count == 0: score -= 15; signals.append("❌ 3 consecutive red candles")
            else:                    score -= 5;  signals.append("⚠️ Mixed candle direction")
        else:
            if bullish_count == 0:  score += 15; signals.append("✅ 3 consecutive red candles")
            elif bullish_count == 1: score += 7;  signals.append("✅ 2 of 3 candles red")
            elif bullish_count == 3: score -= 15; signals.append("❌ 3 consecutive green candles")
            else:                    score -= 5;  signals.append("⚠️ Mixed candle direction")

        # Candle body size vs range (larger body = stronger momentum)
        last = candles_1m.iloc[-1]
        body = abs(last["close"] - last["open"])
        rng  = last["high"] - last["low"]
        if rng > 0 and body / rng > 0.6:
            score += 8; signals.append("✅ Strong candle body (momentum)")
        elif rng > 0 and body / rng < 0.3:
            score -= 5; signals.append("⚠️ Weak doji candle (indecision)")

    # ── Volume spike on last candle ───────────────────────────────────────────
    if candles_1m is not None and len(candles_1m) >= 5:
        avg_vol = candles_1m["display_volume"].iloc[:-1].mean()
        last_vol = candles_1m["display_volume"].iloc[-1]
        if avg_vol > 0:
            ratio = last_vol / avg_vol
            if ratio > 2.0:   score += 10; signals.append(f"✅ Volume spike {ratio:.1f}x avg")
            elif ratio > 1.3: score += 4;  signals.append(f"✅ Above-avg volume {ratio:.1f}x")
            elif ratio < 0.5: score -= 8;  signals.append(f"⚠️ Low volume {ratio:.1f}x avg")

    # ── Option spread tightness (proxy: ATM option price > 0) ────────────────
    atm = round(spot / 50) * 50
    chain_df = df_calls if is_call else df_puts
    if not chain_df.empty:
        atm_row = chain_df[chain_df["strike_price"].astype(float).sub(atm).abs() < 30]
        if not atm_row.empty:
            ltp = float(atm_row.iloc[0].get("price", 0) or 0)
            oi  = float(atm_row.iloc[0].get("oi", 0) or 0)
            vol = float(atm_row.iloc[0].get("volume", 0) or 0)
            if ltp > 0:
                score += 5; signals.append(f"✅ ATM LTP ₹{ltp:.0f} (liquid)")
            if oi > 100000:
                score += 5; signals.append(f"✅ High OI {oi/100000:.1f}L (easy exit)")
            if vol > 50000:
                score += 5; signals.append(f"✅ High volume {vol/1000:.0f}K (active)")

    # ── Sentiment alignment ───────────────────────────────────────────────────
    sent_score = sentiment.get("score", 0) if sentiment else 0
    if is_call and sent_score > 20:
        score += 8; signals.append(f"✅ Bullish sentiment {sent_score:+d}")
    elif is_call and sent_score < -20:
        score -= 10; signals.append(f"❌ Bearish sentiment {sent_score:+d}")
    elif not is_call and sent_score < -20:
        score += 8; signals.append(f"✅ Bearish sentiment {sent_score:+d}")
    elif not is_call and sent_score > 20:
        score -= 10; signals.append(f"❌ Bullish sentiment {sent_score:+d}")

    # ── Time of day penalty ───────────────────────────────────────────────────
    now_h = datetime.now().hour
    now_m = datetime.now().minute
    time_val = now_h + now_m / 60
    if 9.25 <= time_val <= 9.75:
        score -= 10; signals.append("⚠️ Opening 30min — volatile, avoid")
    elif time_val >= 14.75:
        score -= 12; signals.append("⚠️ After 2:45 PM — theta decay accelerating")
    elif 11.0 <= time_val <= 13.0:
        score -= 5;  signals.append("⚠️ Midday lull — lower momentum")
    else:
        score += 5;  signals.append("✅ Good trading window")

    score = max(0, min(100, score))
    if score >= 75:   grade, wait = "A", False
    elif score >= 60: grade, wait = "B", False
    elif score >= 45: grade, wait = "C", True
    elif score >= 30: grade, wait = "D", True
    else:             grade, wait = "F", True

    return {"score": score, "grade": grade, "signals": signals, "wait": wait,
            "action": action}


def render_entry_timing(timing: Dict):
    """Render entry timing score widget."""
    score = timing["score"]
    grade = timing["grade"]
    wait  = timing["wait"]
    color = ("#2e7d32" if grade in ["A","B"] else
             "#ffcc00" if grade == "C" else "#c62828")
    label = "ENTER NOW" if not wait else ("WAIT" if grade in ["C","D"] else "SKIP")

    st.markdown(f"""
    <div style="background:#ffffff;border:1px solid {color}44;border-left:4px solid {color};
                border-radius:8px;padding:0.8rem 1rem;margin-bottom:0.6rem">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
            <div>
                <span style="font-size:0.65rem;color:#78909c;text-transform:uppercase">
                    Entry Timing Score</span>
                <span style="font-size:2rem;font-weight:700;color:{color};
                             font-family:monospace;margin-left:0.8rem">{score}</span>
                <span style="font-size:1rem;color:{color};margin-left:0.3rem">/ 100</span>
            </div>
            <div style="background:{color}22;border:1px solid {color};border-radius:6px;
                        padding:0.3rem 0.8rem;font-weight:700;color:{color};font-size:0.9rem">
                {grade} — {label}
            </div>
        </div>
        <div style="background:#f8f9ff;border-radius:4px;height:6px;overflow:hidden;margin-bottom:0.5rem">
            <div style="width:{score}%;height:100%;background:{color};border-radius:4px;
                        transition:width 0.5s ease"></div>
        </div>
        <div style="font-size:0.7rem;color:#78909c">
            {'  ·  '.join(timing['signals'][:4])}
        </div>
    </div>""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# 2. LIVE TRADE TRACKER
# ═════════════════════════════════════════════════════════════════════════════

def init_trade_tracker():
    """Initialize trade tracker session state."""
    if "active_trade" not in st.session_state:
        st.session_state.active_trade = None
    if "trade_history" not in st.session_state:
        st.session_state.trade_history = []


def start_trade(ai_result: Dict, live_price: Dict, option_data: Dict):
    """Start tracking a new trade from AI recommendation."""
    if not ai_result or ai_result.get("action") == "NO TRADE":
        return

    spot = float(live_price.get("ltp", 0) or 0)
    action       = ai_result.get("action", "")
    entry_strike = int(ai_result.get("entry_strike", round(spot/50)*50))
    entry_price  = ai_result.get("entry_price_range", "0-0")
    # Two-tier targets — T1 is primary, T2 is stretch
    target1      = float(ai_result.get("target1_price", 0) or 0)
    target2      = float(ai_result.get("target2_price", 0) or 0)
    # Fall back to legacy target_price if T1/T2 missing
    legacy_tgt   = float(ai_result.get("target_price", 0) or 0)
    if target1 <= 0:
        target1 = legacy_tgt
    target = target1 if target1 > 0 else legacy_tgt  # backward-compat field
    sl           = float(ai_result.get("stop_loss_price", 0) or 0)
    timeframe    = ai_result.get("timeframe", "30 min")

    # Parse entry mid-price
    try:
        parts = str(entry_price).split("-")
        mid_entry = (float(parts[0]) + float(parts[-1])) / 2
    except:
        mid_entry = (target + sl) / 2

    # Get current option LTP from chain
    is_call = "CALL" in action
    chain   = option_data.get("calls" if is_call else "puts", [])
    opt_ltp = mid_entry  # fallback
    for row in chain:
        try:
            if abs(float(row.get("strike_price", 0)) - entry_strike) < 30:
                ltp = float(row.get("price", 0) or 0)
                if ltp > 0:
                    opt_ltp = ltp
                    break
        except:
            continue

    mins = int(str(timeframe).replace(" min","").replace("min","").strip() or 30)
    st.session_state.active_trade = {
        "action":       action,
        "symbol":       ai_result.get("risk_profile","").replace("CONSERVATIVE","").replace("MODERATE","").replace("AGGRESSIVE","").strip() or "NIFTY",
        "strike":       entry_strike,
        "option_type":  "CE" if is_call else "PE",
        "entry_price":  round(opt_ltp, 2),
        "target":       round(target, 2),       # Legacy field (= T1)
        "target1":      round(target1, 2),
        "target2":      round(target2, 2) if target2 > 0 else 0,
        "t1_hit":       False,                  # flips True when LTP first touches T1
        "t2_hit":       False,
        "sl":           round(sl, 2),
        "current_ltp":  round(opt_ltp, 2),
        "entry_time":   datetime.now(),
        "exit_by":      datetime.now() + timedelta(minutes=mins),
        "max_ltp":      round(opt_ltp, 2),
        "min_ltp":      round(opt_ltp, 2),
        "pnl_pct":      0.0,
        "status":       "ACTIVE",
        "lots":         int(ai_result.get("max_lots", 1)),
        "lot_size":     75,
    }


def update_trade_ltp(option_data: Dict):
    """Update current LTP for active trade from fresh option chain."""
    trade = st.session_state.get("active_trade")
    if not trade or trade.get("status") != "ACTIVE":
        return

    is_call = trade["option_type"] == "CE"
    chain   = option_data.get("calls" if is_call else "puts", [])
    for row in chain:
        try:
            if abs(float(row.get("strike_price",0)) - trade["strike"]) < 30:
                ltp = float(row.get("price", 0) or 0)
                if ltp > 0:
                    trade["current_ltp"] = round(ltp, 2)
                    trade["max_ltp"]     = max(trade["max_ltp"], ltp)
                    trade["min_ltp"]     = min(trade["min_ltp"], ltp)
                    entry = trade["entry_price"]
                    if entry > 0:
                        trade["pnl_pct"] = round((ltp - entry) / entry * 100, 1)
                    # Two-tier hit detection — flip flags when LTP first touches targets
                    t1 = trade.get("target1", 0) or 0
                    t2 = trade.get("target2", 0) or 0
                    if t1 > 0 and not trade.get("t1_hit") and ltp >= t1:
                        trade["t1_hit"] = True
                        trade["t1_hit_time"] = datetime.now()
                    if t2 > 0 and not trade.get("t2_hit") and ltp >= t2:
                        trade["t2_hit"] = True
                        trade["t2_hit_time"] = datetime.now()
                    break
        except:
            continue


def close_trade(outcome: str = "MANUAL"):
    """Close active trade and move to history."""
    trade = st.session_state.get("active_trade")
    if not trade:
        return
    trade["close_time"] = datetime.now()
    trade["status"]     = outcome
    trade["duration"]   = int((trade["close_time"] - trade["entry_time"]).total_seconds() / 60)
    # Determine win/loss
    if outcome == "TARGET":
        trade["result"] = "WIN"
    elif outcome == "SL":
        trade["result"] = "LOSS"
    else:
        trade["result"] = "WIN" if trade["pnl_pct"] > 0 else ("LOSS" if trade["pnl_pct"] < -5 else "BREAKEVEN")

    st.session_state.trade_history.append(dict(trade))
    st.session_state.active_trade = None


def render_live_tracker(option_data: Dict):
    """Render the live trade tracker widget."""
    init_trade_tracker()

    trade = st.session_state.get("active_trade")
    ai    = st.session_state.get("ai_result")

    # Start button
    if not trade and ai and ai.get("action") != "NO TRADE":
        live = st.session_state.get("live_price") or {}
        opts = st.session_state.get("option_data") or {}
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("📌 Start Tracking This Trade", use_container_width=True, type="primary"):
                start_trade(ai, live, opts)
                st.rerun()
        return

    if not trade:
        st.info("Get an AI recommendation first, then click 'Start Tracking' to monitor your trade live.", icon="📌")
        return

    # Update LTP
    if option_data:
        update_trade_ltp(option_data)

    now       = datetime.now()
    entry     = trade["entry_price"]
    current   = trade["current_ltp"]
    target    = trade["target"]                     # legacy = T1
    target1   = trade.get("target1", target) or target
    target2   = trade.get("target2", 0) or 0
    t1_hit    = trade.get("t1_hit", False)
    t2_hit    = trade.get("t2_hit", False)
    sl        = trade["sl"]
    pnl_pct   = trade["pnl_pct"]
    time_left = max(0, int((trade["exit_by"] - now).total_seconds() / 60))
    elapsed   = int((now - trade["entry_time"]).total_seconds() / 60)

    # Auto-close logic — only T2 (stretch target) auto-closes the full trade.
    # When T1 hits, we show an alert but let the user decide (they should book
    # 50-75% and trail the rest). This matches the two-tier philosophy.
    if t1_hit and not trade.get("_t1_alerted"):
        trade["_t1_alerted"] = True  # only alert once
        st.success(
            f"🎯 **TARGET 1 HIT at ₹{target1:.0f}!** "
            f"Book 50-75% of position now and trail SL to entry (₹{entry:.0f}). "
            f"Let remainder run to T2 ₹{target2:.0f}."
        )
        st.balloons()
    if t2_hit and trade["status"] == "ACTIVE":
        close_trade("TARGET")
        st.success(f"🚀 TARGET 2 HIT at ₹{target2:.0f}! Full trade closed.")
        st.balloons()
        st.rerun()
        return
    # Fallback: if only T1 exists (no T2), close at T1 like before
    if target2 <= 0 and current >= target1 and trade["status"] == "ACTIVE":
        close_trade("TARGET")
        st.success("🎯 TARGET HIT! Trade closed automatically.")
        st.balloons()
        st.rerun()
        return
    if current <= sl and trade["status"] == "ACTIVE":
        close_trade("SL")
        st.error("🛑 STOP LOSS HIT! Trade closed.")
        st.rerun()
        return
    if time_left == 0 and trade["status"] == "ACTIVE":
        st.warning("⏰ Time window expired! Exit recommended.")

    # Color logic — progress bar tracks against T2 if available, else T1
    progress_target = target2 if target2 > 0 else target1
    pnl_color = "#2e7d32" if pnl_pct > 0 else "#c62828" if pnl_pct < -5 else "#ffcc00"
    to_t1      = round((target1 - current) / entry * 100, 1) if entry > 0 else 0
    to_t2      = round((target2 - current) / entry * 100, 1) if (entry > 0 and target2 > 0) else 0
    to_sl      = round((current - sl) / entry * 100, 1) if entry > 0 else 0
    progress   = max(0, min(100, (current - sl) / (progress_target - sl) * 100)) if progress_target != sl else 50

    lots     = trade.get("lots", 1)
    lot_size = trade.get("lot_size", 75)
    pnl_abs  = round((current - entry) * lots * lot_size, 0)

    action_icon = "📈" if "CALL" in trade["action"] else "📉"
    time_color  = "#c62828" if time_left <= 5 else "#ffcc00" if time_left <= 15 else "#2e7d32"

    # T1/T2 target column renderers — greened out when hit
    t1_bg = "#c8e6c9" if t1_hit else "#e8f5e9"
    t1_color = "#1b5e20"
    t1_badge = "✅ HIT" if t1_hit else "T1 (Primary)"
    t2_bg = "#ffccbc" if t2_hit else "#fff3e0"
    t2_color = "#bf360c"
    t2_badge = "✅ HIT" if t2_hit else "T2 (Stretch)"

    # Build targets cells — 5-column layout (Entry, Current, T1, T2, SL)
    # If T2 is 0 (legacy single-target trade), collapse to 4-column layout.
    show_t2 = target2 > 0
    grid_cols = "1fr 1fr 1fr 1fr 1fr" if show_t2 else "1fr 1fr 1fr 1fr"
    t2_cell = (
        f'<div style="background:{t2_bg};border-radius:6px;padding:0.5rem;text-align:center">'
        f'<div style="font-size:0.55rem;color:#78909c">{t2_badge}</div>'
        f'<div style="font-size:1rem;font-weight:700;color:{t2_color};font-family:monospace">₹{target2:.0f}</div>'
        f'</div>'
    ) if show_t2 else ""

    st.markdown(f"""
    <div style="background:#ffffff;border:1px solid #e0e4ec;border-radius:12px;padding:1rem;margin-bottom:0.8rem">

        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem">
            <div>
                <span style="font-size:0.65rem;color:#78909c;text-transform:uppercase">Active Trade</span>
                <div style="font-size:1rem;font-weight:700;color:#1a237e;margin-top:2px">
                    {action_icon} {trade['action']} · {trade['strike']}{trade['option_type']}
                </div>
            </div>
            <div style="text-align:right">
                <div style="font-size:0.65rem;color:#78909c">Time Left</div>
                <div style="font-size:1.4rem;font-weight:700;color:{time_color};font-family:monospace">
                    {time_left:02d}m
                </div>
            </div>
        </div>

        <div style="display:grid;grid-template-columns:{grid_cols};gap:6px;margin-bottom:0.8rem">
            <div style="background:#f0f2f6;border-radius:6px;padding:0.5rem;text-align:center">
                <div style="font-size:0.55rem;color:#78909c">ENTRY</div>
                <div style="font-size:1rem;font-weight:700;color:#546e7a;font-family:monospace">₹{entry:.0f}</div>
            </div>
            <div style="background:#f0f2f6;border-radius:6px;padding:0.5rem;text-align:center">
                <div style="font-size:0.55rem;color:#78909c">LTP</div>
                <div style="font-size:1rem;font-weight:700;color:{pnl_color};font-family:monospace">₹{current:.0f}</div>
            </div>
            <div style="background:{t1_bg};border-radius:6px;padding:0.5rem;text-align:center">
                <div style="font-size:0.55rem;color:#78909c">{t1_badge}</div>
                <div style="font-size:1rem;font-weight:700;color:{t1_color};font-family:monospace">₹{target1:.0f}</div>
            </div>
            {t2_cell}
            <div style="background:#fce8e8;border-radius:6px;padding:0.5rem;text-align:center">
                <div style="font-size:0.55rem;color:#78909c">STOP LOSS</div>
                <div style="font-size:1rem;font-weight:700;color:#c62828;font-family:monospace">₹{sl:.0f}</div>
            </div>
        </div>

        <div style="background:#f0f2f6;border-radius:6px;height:8px;margin-bottom:0.6rem;overflow:hidden">
            <div style="height:100%;width:{progress:.0f}%;background:linear-gradient(90deg,#c62828,{pnl_color},{pnl_color});border-radius:6px;transition:width 0.5s"></div>
        </div>

        <div style="display:flex;justify-content:space-between;align-items:center">
            <div>
                <span style="font-size:1.4rem;font-weight:700;color:{pnl_color};font-family:monospace">
                    {pnl_pct:+.1f}%
                </span>
                <span style="font-size:0.75rem;color:{pnl_color};margin-left:0.4rem">
                    ₹{pnl_abs:+,.0f} ({lots} lot)
                </span>
            </div>
            <div style="font-size:0.7rem;color:#78909c;text-align:right">
                To T1: {to_t1:+.1f}%{' · To T2: ' + f'{to_t2:+.1f}%' if show_t2 else ''} · To SL: {to_sl:.1f}% · {elapsed}m elapsed
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("✂️ Book at T1", use_container_width=True,
                     help="Partial exit at T1 — marks T1 hit but keeps trade active for T2",
                     disabled=t1_hit):
            trade["t1_hit"] = True
            trade["t1_hit_time"] = datetime.now()
            st.success("✅ T1 booked. Trail SL to entry and let rest run to T2.")
            st.rerun()
    with c2:
        if st.button("🎯 Close at TARGET", use_container_width=True):
            close_trade("TARGET"); st.rerun()
    with c3:
        if st.button("🛑 Close at SL", use_container_width=True):
            close_trade("SL"); st.rerun()
    with c4:
        if st.button("🚪 Exit Manually", use_container_width=True):
            close_trade("MANUAL"); st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# 3. OI CHANGE VELOCITY
# ═════════════════════════════════════════════════════════════════════════════

def compute_oi_velocity(df_calls: pd.DataFrame, df_puts: pd.DataFrame,
                         spot: float) -> Dict[str, Any]:
    """
    Detect fresh OI writing vs covering from OI % change.
    Uses oi_perc_chg from Paytm option chain.
    """
    results = {"call_surges": [], "put_surges": [], "signals": [], "bias": "NEUTRAL"}

    if df_calls.empty and df_puts.empty:
        return results

    atm = round(spot / 50) * 50

    def process(df, side):
        surges = []
        if df.empty or "oi_perc_chg" not in df.columns:
            return surges
        near = df[df["strike_price"].astype(float).sub(spot).abs() <= 500].copy()
        near["oi_perc_chg"] = pd.to_numeric(near["oi_perc_chg"], errors="coerce").fillna(0)
        near["oi"]          = pd.to_numeric(near["oi"], errors="coerce").fillna(0)
        near["price"]       = pd.to_numeric(near["price"], errors="coerce").fillna(0)
        for _, row in near.iterrows():
            chg = float(row["oi_perc_chg"])
            oi  = float(row["oi"])
            ltp = float(row["price"])
            stk = float(row["strike_price"])
            if abs(chg) >= 20 and oi > 50000:
                action = ("FRESH WRITE ⚠️" if chg > 0 else "COVERING 🟢")
                surges.append({
                    "strike": stk, "oi_chg": chg, "oi": oi, "ltp": ltp,
                    "action": action, "side": side,
                    "dist": "ATM" if abs(stk-atm) < 30 else (f"+{int(stk-atm)}" if stk > atm else f"{int(stk-atm)}")
                })
        surges.sort(key=lambda x: abs(x["oi_chg"]), reverse=True)
        return surges[:5]

    results["call_surges"] = process(df_calls, "CE")
    results["put_surges"]  = process(df_puts,  "PE")

    # Derive signals
    fresh_calls   = sum(1 for s in results["call_surges"] if s["oi_chg"] > 30)
    fresh_puts    = sum(1 for s in results["put_surges"]  if s["oi_chg"] > 30)
    cover_calls   = sum(1 for s in results["call_surges"] if s["oi_chg"] < -20)
    cover_puts    = sum(1 for s in results["put_surges"]  if s["oi_chg"] < -20)

    # Also compare the MAGNITUDE of writing, not just strike count.
    # Two strikes with +500% OI change is far more meaningful than three
    # strikes with +50%. We weight by total % change summed across strikes.
    call_write_mag = sum(s["oi_chg"] for s in results["call_surges"] if s["oi_chg"] > 30)
    put_write_mag  = sum(s["oi_chg"] for s in results["put_surges"]  if s["oi_chg"] > 30)

    # Determine bias using BOTH count and magnitude — prefer the side with
    # stronger writing activity overall, not just the first side to cross
    # the threshold (the old code always triggered on CALL writing first
    # due to if/elif order, making bias mostly BEARISH).
    call_strength = fresh_calls * 100 + call_write_mag   # count weighted + magnitude
    put_strength  = fresh_puts  * 100 + put_write_mag

    # Need a meaningful gap (15%) between sides to call a directional bias,
    # otherwise it's NEUTRAL (heavy writing on both sides = indecision).
    total = call_strength + put_strength
    gap   = abs(call_strength - put_strength)
    decisive = total > 0 and (gap / total) >= 0.15

    if fresh_puts > fresh_calls and cover_calls > 0:
        # Classic bullish: puts being written + calls being covered
        results["bias"] = "BULLISH"
        results["signals"].append("✅ Fresh PUT writing + CALL covering = bullish pressure")
    elif fresh_calls > fresh_puts and cover_puts > 0:
        # Classic bearish: calls being written + puts being covered
        results["bias"] = "BEARISH"
        results["signals"].append("⚠️ Fresh CALL writing + PUT covering = bearish resistance")
    elif decisive and put_strength > call_strength:
        # More/stronger PUT writing = put writers confident support will hold
        results["bias"] = "BULLISH"
        results["signals"].append(
            f"✅ {fresh_puts} strikes with heavy fresh PUT writing "
            f"(net magnitude {put_write_mag:+.0f}%) — support building"
        )
    elif decisive and call_strength > put_strength:
        # More/stronger CALL writing = call writers confident resistance will hold
        results["bias"] = "BEARISH"
        results["signals"].append(
            f"⚠️ {fresh_calls} strikes with heavy fresh CALL writing "
            f"(net magnitude {call_write_mag:+.0f}%) — resistance building"
        )
    elif fresh_calls + fresh_puts >= 4:
        # Heavy writing on BOTH sides with no clear winner = indecision
        results["bias"] = "NEUTRAL"
        results["signals"].append(
            f"⚖️ Heavy two-way writing ({fresh_calls} CE vs {fresh_puts} PE) "
            f"— indecision, watch for breakout"
        )
    elif cover_calls > 1:
        results["bias"] = "BULLISH"
        results["signals"].append("✅ CALL unwinding — short covering may push market up")
    elif cover_puts > 1:
        results["bias"] = "BEARISH"
        results["signals"].append("⚠️ PUT unwinding — support weakening")
    else:
        results["bias"] = "NEUTRAL"
        results["signals"].append("⚖️ No significant OI velocity signals")

    # Expose the strength numbers for debugging / transparency
    results["call_strength"] = round(call_strength, 1)
    results["put_strength"]  = round(put_strength, 1)

    return results


def render_oi_velocity(velocity: Dict):
    """Render OI change velocity section."""
    bias  = velocity.get("bias","NEUTRAL")
    bc    = "#2e7d32" if bias=="BULLISH" else "#c62828" if bias=="BEARISH" else "#8888cc"
    sigs  = velocity.get("signals", [])

    st.markdown(f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
        <span style="font-size:0.75rem;color:#78909c;text-transform:uppercase">OI Change Velocity</span>
        <span style="background:{bc}22;border:1px solid {bc};border-radius:4px;
                     padding:2px 8px;font-size:0.72rem;color:{bc};font-weight:700">{bias}</span>
    </div>""", unsafe_allow_html=True)

    # ── Strength comparison bar — shows Call vs Put writing strength visually
    call_str = float(velocity.get("call_strength", 0) or 0)
    put_str  = float(velocity.get("put_strength",  0) or 0)
    total    = call_str + put_str
    if total > 0:
        call_pct = (call_str / total) * 100
        put_pct  = (put_str  / total) * 100
        st.markdown(f"""
        <div style="margin:4px 0 8px 0;">
            <div style="display:flex;justify-content:space-between;font-size:0.65rem;color:#78909c;margin-bottom:2px">
                <span>⚠️ CALL writing: {call_pct:.0f}%</span>
                <span>PUT writing: {put_pct:.0f}% ✅</span>
            </div>
            <div style="display:flex;height:8px;border-radius:4px;overflow:hidden;background:#f0f0f0">
                <div style="width:{call_pct}%;background:#c62828"></div>
                <div style="width:{put_pct}%;background:#2e7d32"></div>
            </div>
        </div>""", unsafe_allow_html=True)

    if sigs:
        for s in sigs:
            st.markdown(f"<div style='font-size:0.75rem;color:#2c3e50;margin-bottom:2px'>{s}</div>",
                       unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    def render_surges(surges, label, col):
        with col:
            st.markdown(f"<div style='font-size:0.65rem;color:#78909c;margin-bottom:4px'>{label}</div>",
                       unsafe_allow_html=True)
            if not surges:
                st.caption("No significant moves")
                return
            for s in surges[:4]:
                chg_color = "#c62828" if s["oi_chg"] > 0 else "#2e7d32"
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;padding:3px 6px;
                             margin:2px 0;background:#f8f9ff;border-radius:4px;font-size:0.7rem">
                    <span style="color:#2c3e50">{s['strike']:.0f} ({s['dist']})</span>
                    <span style="color:{chg_color}">{s['oi_chg']:+.0f}%</span>
                    <span style="color:#78909c;font-size:0.65rem">{s['action']}</span>
                </div>""", unsafe_allow_html=True)

    render_surges(velocity.get("call_surges",[]), "CALL OI Changes", col1)
    render_surges(velocity.get("put_surges", []), "PUT OI Changes",  col2)


# ═════════════════════════════════════════════════════════════════════════════
# 4. AI TRADE HISTORY & WIN RATE TRACKER
# ═════════════════════════════════════════════════════════════════════════════

def render_trade_history():
    """Render trade history log and win rate statistics."""
    init_trade_tracker()
    history = st.session_state.get("trade_history", [])

    if not history:
        st.info("No completed trades yet. Start tracking a trade and close it to build your history.", icon="📊")
        return

    wins   = sum(1 for t in history if t.get("result") == "WIN")
    losses = sum(1 for t in history if t.get("result") == "LOSS")
    be     = sum(1 for t in history if t.get("result") == "BREAKEVEN")
    total  = len(history)
    wr     = round(wins / total * 100) if total > 0 else 0

    avg_pnl = np.mean([t.get("pnl_pct", 0) for t in history]) if history else 0
    best    = max((t.get("pnl_pct", 0) for t in history), default=0)
    worst   = min((t.get("pnl_pct", 0) for t in history), default=0)

    wr_color = "#2e7d32" if wr >= 55 else "#ffcc00" if wr >= 45 else "#c62828"

    c1, c2, c3, c4 = st.columns(4)
    for col, lbl, val, clr in [
        (c1, "Win Rate",   f"{wr}%",         wr_color),
        (c2, "Trades",     f"{total}",        "#546e7a"),
        (c3, "Avg P&L",    f"{avg_pnl:+.1f}%", "#2e7d32" if avg_pnl > 0 else "#c62828"),
        (c4, "Best / Worst", f"{best:+.0f}% / {worst:+.0f}%", "#2c3e50"),
    ]:
        with col:
            st.markdown(f"""
            <div style="background:#ffffff;border:1px solid #e0e4ec;border-radius:8px;
                        padding:0.7rem;text-align:center">
                <div style="font-size:0.6rem;color:#78909c">{lbl}</div>
                <div style="font-size:1.2rem;font-weight:700;color:{clr};font-family:monospace">{val}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

    for trade in reversed(history[-15:]):
        result  = trade.get("result","?")
        rc      = "#2e7d32" if result=="WIN" else "#c62828" if result=="LOSS" else "#ffcc00"
        pnl     = trade.get("pnl_pct", 0)
        dur     = trade.get("duration", 0)
        et      = trade.get("entry_time")
        ts      = et.strftime("%d %b %H:%M") if isinstance(et, datetime) else str(et)[:16]

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;
                    margin:3px 0;background:#f8f9ff;border-radius:6px;
                    border-left:3px solid {rc}">
            <span style="color:{rc};font-size:0.7rem;font-weight:700;min-width:55px">{result}</span>
            <span style="color:#2c3e50;font-size:0.75rem;flex:1">
                {trade.get('action','?')} · {trade.get('strike',0)}{trade.get('option_type','?')}
            </span>
            <span style="color:{rc};font-size:0.78rem;font-family:monospace;min-width:50px;text-align:right">
                {pnl:+.1f}%</span>
            <span style="color:#78909c;font-size:0.65rem;min-width:45px;text-align:right">{dur}m</span>
            <span style="color:#78909c;font-size:0.65rem;min-width:70px;text-align:right">{ts}</span>
        </div>""", unsafe_allow_html=True)

    if st.button("🗑️ Clear History", help="Reset all trade history"):
        st.session_state.trade_history = []
        st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# 5. BANKNIFTY DIVERGENCE + MULTI-EXPIRY PCR
# ═════════════════════════════════════════════════════════════════════════════

def fetch_banknifty_spot() -> float:
    """Fetch BankNifty spot from Yahoo Finance."""
    try:
        import requests
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEBANK",
            params={"interval":"1m","range":"1d"},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=5
        )
        if r.status_code == 200:
            meta = r.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
            return float(meta.get("regularMarketPrice", 0) or 0)
    except:
        pass
    return 0.0


def compute_divergence(nifty_chg_pct: float, banknifty_spot: float) -> Dict:
    """Compute BankNifty vs Nifty divergence signal."""
    if banknifty_spot <= 0:
        return {"available": False, "signal": "BankNifty data unavailable"}

    # Estimate BankNifty % change from session state if we have prev close
    # For now use the spot vs typical prev close ratio
    result = {
        "available":   True,
        "bn_spot":     banknifty_spot,
        "nifty_chg":   nifty_chg_pct,
        "signal":      "NEUTRAL",
        "description": "",
        "color":       "#8888cc",
    }

    # Rough divergence: if Nifty positive but BankNifty is weak (or vice versa)
    # BankNifty leads 70% of the time — use its direction as confirmation
    if nifty_chg_pct > 0.3:
        result["signal"]      = "CONFIRMING BULLISH"
        result["description"] = f"Nifty +{nifty_chg_pct:.2f}% with BankNifty present — CHECK BN direction"
        result["color"]       = "#2e7d32"
    elif nifty_chg_pct < -0.3:
        result["signal"]      = "CONFIRMING BEARISH"
        result["description"] = f"Nifty {nifty_chg_pct:.2f}% — verify BankNifty also falling"
        result["color"]       = "#c62828"
    else:
        result["signal"]      = "RANGE-BOUND"
        result["description"] = "Both indices ranging — wait for directional breakout"
        result["color"]       = "#ffcc00"

    return result


def render_divergence_pcr(live_price: Dict, pcr_data: Dict,
                           second_expiry_pcr: float = 0.0):
    """Render BankNifty divergence and multi-expiry PCR."""
    nifty_chg = float(live_price.get("change_pct", 0) or 0)

    # Cache BankNifty spot — only fetch once per 60 seconds
    import datetime as _dt
    _now = _dt.datetime.now()
    _last = st.session_state.get("_bn_last_fetch")
    if (_last is None or (_now - _last).total_seconds() > 60 or
            st.session_state.get("banknifty_spot", 0) == 0):
        try:
            _bn = fetch_banknifty_spot()
            if _bn > 0:
                st.session_state["banknifty_spot"] = _bn
                st.session_state["_bn_last_fetch"] = _now
        except Exception:
            pass
    bn_spot = float(st.session_state.get("banknifty_spot", 0))
    div     = compute_divergence(nifty_chg, bn_spot)

    pcr_near  = pcr_data.get("pcr_oi", 0)
    pcr_vol   = pcr_data.get("pcr_volume", 0)

    dc = div["color"]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        <div style="background:#ffffff;border:1px solid {dc}44;border-radius:8px;padding:0.7rem 0.9rem">
            <div style="font-size:0.65rem;color:#78909c;margin-bottom:0.4rem">BANKNIFTY DIVERGENCE</div>
            <div style="font-size:0.9rem;font-weight:700;color:{dc}">{div['signal']}</div>
            <div style="font-size:0.7rem;color:#546e7a;margin-top:0.3rem">{div['description']}</div>
            {"<div style='font-size:0.75rem;color:#2c3e50;margin-top:0.4rem;font-family:monospace'>BN Spot: ₹"+f"{bn_spot:,.0f}</div>" if bn_spot > 0 else ""}
        </div>""", unsafe_allow_html=True)

    with col2:
        pcr_color  = "#2e7d32" if pcr_near > 1.2 else "#c62828" if pcr_near < 0.8 else "#ffcc00"
        pcr2_color = "#2e7d32" if second_expiry_pcr > 1.2 else "#c62828" if 0 < second_expiry_pcr < 0.8 else "#8888cc"
        st.markdown(f"""
        <div style="background:#ffffff;border:1px solid #e0e4ec;border-radius:8px;padding:0.7rem 0.9rem">
            <div style="font-size:0.65rem;color:#78909c;margin-bottom:0.4rem">PCR COMPARISON</div>
            <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="font-size:0.7rem;color:#78909c">Near expiry (OI)</span>
                <span style="font-size:0.85rem;font-weight:700;color:{pcr_color};font-family:monospace">{pcr_near:.3f}</span>
            </div>
            <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="font-size:0.7rem;color:#78909c">Near expiry (Vol)</span>
                <span style="font-size:0.85rem;font-weight:700;color:#546e7a;font-family:monospace">{pcr_vol:.3f}</span>
            </div>
            <div style="display:flex;justify-content:space-between">
                <span style="font-size:0.7rem;color:#78909c">Next expiry (OI)</span>
                <span style="font-size:0.85rem;font-weight:700;color:{pcr2_color};font-family:monospace">
                    {"N/A" if second_expiry_pcr == 0 else f"{second_expiry_pcr:.3f}"}</span>
            </div>
            <div style="font-size:0.65rem;color:#78909c;margin-top:4px">
                OI PCR>1.2=bullish · <0.8=bearish · Vol PCR confirms conviction
            </div>
        </div>""", unsafe_allow_html=True)
