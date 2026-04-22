"""
OptionsIQ v2.0 — Complete Options Intelligence Platform
Sections: Live Price | Market Internals | Option Chain | OI Analysis |
          Indicators | Smart Money | AI Decision Engine
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import os as _os
try:
    if not _os.path.exists(".streamlit/config.toml"):
        _os.makedirs(".streamlit", exist_ok=True)
        open(".streamlit/config.toml","w").write(
            '[theme]\nbase="light"\nprimaryColor="#1565c0"\n'
            'backgroundColor="#f0f2f6"\nsecondaryBackgroundColor="#ffffff"\n'
            'textColor="#1a1a2e"\n')
except Exception:
    pass
import pandas as pd
import altair as alt
import requests
import time
from datetime import datetime

from api_client import fetch_expiry_dates, fetch_option_chain_both
from analytics import (
    compute_market_snapshot, identify_atm_strike, classify_strikes,
    compute_pcr, find_support_resistance, find_intraday_levels,
    analyze_oi_buildup, track_smart_money, generate_trade_decision,
)
from market_hours import get_market_status, market_status_banner
from features import (
    compute_entry_timing_score, render_entry_timing,
    init_trade_tracker, render_live_tracker,
    compute_oi_velocity, render_oi_velocity,
    render_trade_history, render_divergence_pcr,
)
from websocket_feed import get_feed
from market_data import (
    fetch_live_price, compute_technical_indicators,
    compute_market_sentiment, compute_market_breadth, fetch_market_news,
    derive_spot_from_chain,
)
from ai_engine import (
    build_market_context_with_candles, get_ai_analysis, get_profile,
    normalize_trade_recommendation, RISK_PROFILES,
)
from ui_components import apply_custom_css

PAYTM_BASE = "https://developer.paytmmoney.com"

st.set_page_config(page_title="OptionsIQ v2", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")
apply_custom_css()

# ── Additional CSS (Light Theme) ─────────────────────────────────────────────
st.markdown("""<style>
.section-card { background:#fff; border:1px solid #e0e4ec; border-radius:10px; padding:1.2rem 1.4rem; margin-bottom:.8rem; box-shadow:0 1px 4px rgba(0,0,0,.05); }
.section-title { font-size:.75rem; text-transform:uppercase; letter-spacing:2px; color:#5c6bc0; margin-bottom:.8rem; font-weight:600; }
.kpi-row { display:flex; gap:.8rem; flex-wrap:wrap; }
.kpi-box { flex:1; min-width:120px; background:#f8f9ff; border:1px solid #e0e4ec; border-radius:8px; padding:.7rem 1rem; text-align:center; }
.kpi-label { font-size:.65rem; color:#78909c; text-transform:uppercase; letter-spacing:1px; }
.kpi-val { font-family:"JetBrains Mono",monospace; font-size:1.3rem; font-weight:700; color:#1a237e; }
.kpi-sub { font-size:.7rem; color:#5c6bc0; }
.up { color:#1b5e20 !important; } .dn { color:#b71c1c !important; } .nt { color:#3949ab !important; }
.ai-card { background:#fff; border:2px solid #2e7d32; border-radius:14px; padding:1.8rem; box-shadow:0 2px 8px rgba(0,0,0,.06); }
.ai-buy-call { border-color:#2e7d32 !important; background:linear-gradient(135deg,#f1f8f2,#fff) !important; }
.ai-buy-put  { border-color:#c62828 !important; background:linear-gradient(135deg,#fdf3f3,#fff) !important; }
.ai-no-trade { border-color:#5c6bc0 !important; background:linear-gradient(135deg,#f3f4fb,#fff) !important; }
.ai-action { font-family:"JetBrains Mono",monospace; font-size:2.2rem; font-weight:700; letter-spacing:.05em; }
.factor-pill { display:inline-block; background:#e8eaf6; border:1px solid #c5cae9; border-radius:20px; padding:.3rem .8rem; margin:.2rem; font-size:.72rem; color:#3949ab; font-weight:500; }
.news-item { border-left:3px solid #c5cae9; padding:.5rem .8rem; margin:.4rem 0; background:#f8f9ff; border-radius:0 6px 6px 0; }
.sentiment-bar { height:14px; border-radius:7px; background:#e8eaf6; margin:.5rem 0; position:relative; overflow:hidden; }

/* ── Auto-refresh smoothness overrides ─────────────────────────────────
   Streamlit's default rerun behavior greys out the whole page every time
   the script re-runs. At a 5s cadence this feels terrible. These overrides
   keep the UI responsive during auto-refresh reruns. */
[data-testid="stStatusWidget"] { display:none !important; }     /* top-right "Running" */
[data-testid="stHeader"] button[kind="header"] { opacity:0.3; }  /* dim the stop button */
.stSpinner > div { animation-duration: 0.4s !important; }        /* snappier spinners */
/* Prevent the whole app from dimming during reruns */
.stApp { transition: none !important; }
[data-testid="stAppViewContainer"] { opacity: 1 !important; }
/* Soft fade for data widgets so numbers don't "jump" */
[data-testid="stMetricValue"], [data-testid="stMetricLabel"] {
    transition: color 0.3s ease, opacity 0.3s ease;
}
</style>
""", unsafe_allow_html=True)


# ── Session State ─────────────────────────────────────────────────────────────
def init_state():
    defaults = {
        "step": "credentials",
        "api_key": "", "api_secret": "",
        "request_token": "", "access_token": "", "authenticated": False,
        "read_access_token": "",
        "expiry_dates": [], "selected_expiry": None,
        "selected_symbol": "NIFTY",
        "option_data": None, "live_price": None,
        "indicators": None, "breadth": None,
        "sentiment": None, "news": None,
        "last_news_refresh": None,
        "last_refresh": None,
        "last_live_poll": None,
        "last_chain_refresh": None,
        "auto_refresh": False, "refresh_interval": 5,
        "ai_result": None, "ai_loading": False,
        "ai_result_timestamp": None,
        "_last_auto_fetch_ts": None,
        "_refresh_paused": False,
        "_ai_click_pending": False,
        "_last_ai_error": None,
        "market_status": None,
        "anthropic_api_key": "",
        "ws_enabled": False,
        "public_access_token": "",
        "token_response": None,
        "ws_symbol": "",
        "risk_profile": "MODERATE",
        "user_note": "",
        "tick_history": [],
        "chart_symbol": "",
        "active_trade": None,
        "trade_history": [],
        "intelligence_loaded": False,
        "last_intelligence_refresh": None,
        "fii_dii": None,
        "global_cues": None,
        "india_vix": None,
        "mkt_breadth": None,
        "news_items": [],
        "last_chain_refresh": None,
        "banknifty_spot": 0.0,
        "show_chart": True,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

# Auto-capture requestToken from redirect
url_params = st.query_params
captured = url_params.get("requestToken") or url_params.get("request_token") or ""
if captured and not st.session_state.access_token:
    st.session_state.request_token = captured
    st.session_state.step = "exchange"


# ── Helpers ───────────────────────────────────────────────────────────────────
def num(df, col):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def color_class(val, reverse=False):
    if val > 0: return "dn" if reverse else "up"
    if val < 0: return "up" if reverse else "dn"
    return "nt"

def fmt_lakh(n):
    n = float(n)
    if n >= 1e7: return f"{n/1e7:.2f}Cr"
    if n >= 1e5: return f"{n/1e5:.2f}L"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(int(n))


def _news_is_stale(max_age_minutes=10):
    ts = st.session_state.get("last_news_refresh")
    if ts is None:
        return True
    return (datetime.now() - ts).total_seconds() >= max_age_minutes * 60


def _compute_ai_time_context(mkt: dict, selected_expiry: str | None = None) -> dict:
    """Build the time-awareness dict that the AI prompt uses to scale targets
    and holding period to what's actually achievable before market close."""
    from datetime import datetime as _dt
    now = _dt.now()
    is_open = bool(mkt.get("is_open"))

    # Minutes until NSE close (15:30 IST) — ZERO when market is closed.
    # Do NOT calculate "minutes to next open" here — that gives 300-400 min
    # and causes the AI to act as if it has a full session ahead.
    if not is_open:
        mtc = 0
    else:
        close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        mtc = max(0, int((close_time - now).total_seconds() // 60))

    # Session phase — finer granularity than before
    if not is_open:
        phase = "CLOSED"
    elif now.hour < 10:
        phase = "EARLY_SESSION"          # 9:15 – 10:00: high volatility
    elif now.hour < 11:
        phase = "OPENING_HOUR"           # 10:00 – 11:00: settling down
    elif now.hour < 13:
        phase = "MIDDAY"                 # 11:00 – 13:00: lower volatility
    elif now.hour < 14:
        phase = "AFTERNOON"              # 13:00 – 14:00: pick-up begins
    elif now.hour == 14 or (now.hour == 15 and now.minute < 0):
        phase = "LATE_SESSION"           # 14:00 – 15:00: trend extension
    elif mtc <= 30:
        phase = "FINAL_30_MIN"           # 15:00 – 15:30: theta crunch
    else:
        phase = "LAST_HOUR"

    # Is today an expiry day? Match selected_expiry date to today
    today_str = mkt.get("current_date", now.strftime("%d-%b-%Y"))
    is_expiry_day = False
    if selected_expiry:
        try:
            import re as _re
            # Extract day number from both dates
            exp_day = _re.findall(r"\d+", str(selected_expiry))[0]
            today_day = _re.findall(r"\d+", today_str)[0]
            # Also check month/year match roughly
            is_expiry_day = exp_day == today_day and str(now.year) in str(selected_expiry)
        except Exception:
            pass

    # Typical NIFTY point range per timeframe at this time of day
    # Used to calibrate realistic target expectations
    if phase in ("EARLY_SESSION", "OPENING_HOUR"):
        typical_30min_range = 80    # high morning volatility
        typical_15min_range = 50
    elif phase == "MIDDAY":
        typical_30min_range = 40    # quiet midday
        typical_15min_range = 25
    elif phase in ("AFTERNOON", "LATE_SESSION"):
        typical_30min_range = 55    # afternoon trend
        typical_15min_range = 35
    else:  # FINAL_30_MIN
        typical_30min_range = min(mtc * 1.5, 45)  # scale to remaining time
        typical_15min_range = min(mtc * 0.8, 30)

    return {
        "minutes_to_close":       mtc,
        "session_phase":          phase,
        "current_time_ist":       now.strftime("%H:%M IST"),
        "current_date":           today_str,
        "is_expiry_day":          is_expiry_day,
        "is_open":                is_open,
        "typical_30min_pts":      int(typical_30min_range),
        "typical_15min_pts":      int(typical_15min_range),
        "max_hold_minutes":       min(mtc - 2, 60) if is_open and mtc > 5 else 0,
    }


def _compute_vwap_and_stats() -> dict:
    """Compute VWAP and candle range stats from tick history.
    VWAP = sum(price * volume) / sum(volume), approximated from ticks.
    Also returns ATR-style avg range of last 5 candles."""
    tick_history = st.session_state.get("tick_history", [])
    result = {"vwap": 0.0, "avg_range_5m": 0.0, "avg_range_1m": 0.0,
              "session_high": 0.0, "session_low": 0.0,
              "opening_range_high": 0.0, "opening_range_low": 0.0,
              "price_vs_vwap": "N/A"}
    if not tick_history or len(tick_history) < 5:
        return result
    try:
        df = pd.DataFrame(tick_history)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["ltp"] = pd.to_numeric(df["ltp"], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
        df = df.dropna(subset=["timestamp", "ltp"]).sort_values("timestamp")
        if df.empty:
            return result

        # Session high/low
        result["session_high"] = float(df["ltp"].max())
        result["session_low"] = float(df["ltp"].min())

        # VWAP (volume-weighted, fallback to simple mean if volume=0)
        total_vol = df["volume"].sum()
        if total_vol > 0:
            vwap = (df["ltp"] * df["volume"]).sum() / total_vol
        else:
            vwap = df["ltp"].mean()
        result["vwap"] = round(float(vwap), 2)

        # Current price vs VWAP
        cur = float(df["ltp"].iloc[-1])
        if vwap > 0:
            diff = cur - vwap
            pct = (diff / vwap) * 100
            result["price_vs_vwap"] = f"{'above' if diff > 0 else 'below'} VWAP by {abs(diff):.1f} pts ({abs(pct):.2f}%)"

        # Opening range (first 15 min)
        start = df["timestamp"].iloc[0]
        or_end = start + pd.Timedelta(minutes=15)
        or_df = df[df["timestamp"] <= or_end]
        if not or_df.empty:
            result["opening_range_high"] = float(or_df["ltp"].max())
            result["opening_range_low"] = float(or_df["ltp"].min())

        # Avg candle range from 5-min and 1-min candles
        for tf, key in [(5, "avg_range_5m"), (1, "avg_range_1m")]:
            candles_tf = df.set_index("timestamp")["ltp"].resample(f"{tf}min").ohlc().dropna()
            if not candles_tf.empty:
                recent = candles_tf.tail(8)
                ranges = (recent["high"] - recent["low"])
                result[key] = round(float(ranges.mean()), 1)
    except Exception:
        pass
    return result


def _get_live_price_with_ws_fallback(token, symbol, option_data):
    """Get live price preferring WebSocket when available, falling back to REST."""
    feed = get_feed()
    if (
        st.session_state.get("ws_enabled")
        and st.session_state.get("ws_symbol") == symbol
        and feed.is_connected
    ):
        ws_price = feed.get_price()
        if ws_price and ws_price.get("ltp", 0) > 0:
            return ws_price
    return fetch_live_price(token, symbol, option_chain_data=option_data)


def _poll_runtime_market_data(token, symbol, expiry):
    """Keep live price fast while refreshing heavy option-chain data less often.

    Skipped when auto-refresh is enabled (our proper auto-refresh handles it)
    to avoid duplicate fetches and spinner flashes.
    """
    if not token or token == "DEMO" or not expiry:
        return
    # If the user-controlled auto-refresh is on, let that be the single source
    # of truth — don't double-fetch from here.
    if st.session_state.get("auto_refresh"):
        return

    now = datetime.now()
    last_live = st.session_state.get("last_live_poll")
    if last_live is None or (now - last_live).total_seconds() >= 2:
        live = _get_live_price_with_ws_fallback(
            token,
            symbol,
            st.session_state.get("option_data") or {},
        )
        if live and live.get("ltp", 0) > 0:
            st.session_state.live_price = live
            st.session_state.last_live_poll = now

    last_chain = st.session_state.get("last_chain_refresh")
    if last_chain is None or (now - last_chain).total_seconds() >= 20:
        fetch_all_data(token, symbol, expiry, include_news=False,
                       reset_ai=False, silent=True)
        st.session_state.last_chain_refresh = datetime.now()


def _derive_hybrid_levels(support, resistance, intraday_levels, candles_5m):
    """Blend option-chain walls with recent price structure for more actionable levels."""
    hybrid_support = dict(support)
    hybrid_resistance = dict(resistance)

    if candles_5m is None or candles_5m.empty:
        return hybrid_support, hybrid_resistance

    recent = candles_5m.tail(12)
    swing_low = float(recent["low"].min()) if not recent.empty else 0.0
    swing_high = float(recent["high"].max()) if not recent.empty else 0.0
    imm_support = float(intraday_levels.get("immediate_support", 0) or 0)
    imm_resistance = float(intraday_levels.get("immediate_resistance", 0) or 0)

    support_candidates = [x for x in [imm_support, support.get("strike", 0), swing_low] if float(x or 0) > 0]
    resistance_candidates = [x for x in [imm_resistance, resistance.get("strike", 0), swing_high] if float(x or 0) > 0]

    if support_candidates:
        hybrid_support["strike"] = round(sum(support_candidates) / len(support_candidates), 2)
        hybrid_support["source"] = "HYBRID_OI_PRICE"
    if resistance_candidates:
        hybrid_resistance["strike"] = round(sum(resistance_candidates) / len(resistance_candidates), 2)
        hybrid_resistance["source"] = "HYBRID_OI_PRICE"

    return hybrid_support, hybrid_resistance


def _reset_tick_history_if_symbol_changed(symbol):
    if st.session_state.get("chart_symbol") != symbol:
        st.session_state.tick_history = []
        st.session_state.chart_symbol = symbol


def _record_live_tick(symbol, live_price):
    ltp = float(live_price.get("ltp", 0) or 0)
    if ltp <= 0:
        return
    _reset_tick_history_if_symbol_changed(symbol)

    tick_time = datetime.now()
    tick_history = st.session_state.get("tick_history", [])
    if tick_history:
        last = tick_history[-1]
        last_time = pd.to_datetime(last["timestamp"])
        if (tick_time - last_time).total_seconds() < 1 and abs(float(last["ltp"]) - ltp) < 0.01:
            return

    tick_history.append({
        "timestamp": tick_time.isoformat(),
        "ltp": round(ltp, 2),
        "volume": float(live_price.get("volume", 0) or 0),
    })
    st.session_state.tick_history = tick_history[-3000:]


def _build_candles(timeframe_minutes: int) -> pd.DataFrame:
    tick_history = st.session_state.get("tick_history", [])
    if not tick_history:
        return pd.DataFrame()

    df = pd.DataFrame(tick_history)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["ltp"] = pd.to_numeric(df["ltp"], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
    df = df.dropna(subset=["timestamp", "ltp"]).sort_values("timestamp")
    if df.empty:
        return pd.DataFrame()

    rule = f"{timeframe_minutes}min"
    price_resampled = (
        df.set_index("timestamp")["ltp"]
        .resample(rule)
        .ohlc()
        .dropna()
    )
    volume_last = df.set_index("timestamp")["volume"].resample(rule).last().fillna(0)
    volume_first = df.set_index("timestamp")["volume"].resample(rule).first().fillna(0)
    tick_count = df.set_index("timestamp")["ltp"].resample(rule).count().fillna(0)

    ohlc = price_resampled.copy()
    ohlc["volume"] = (volume_last.reindex(ohlc.index).fillna(0) - volume_first.reindex(ohlc.index).fillna(0)).clip(lower=0)
    ohlc["tick_volume"] = tick_count.reindex(ohlc.index).fillna(0)
    candles = ohlc.reset_index()
    if candles.empty:
        return candles
    candles["display_volume"] = candles.apply(
        lambda row: row["volume"] if row["volume"] > 0 else row["tick_volume"],
        axis=1,
    )
    candles["dir"] = candles.apply(
        lambda row: "Bullish" if row["close"] >= row["open"] else "Bearish",
        axis=1,
    )
    candles["label"] = candles["timestamp"].dt.strftime("%H:%M")
    candles["pattern"] = "None"
    for idx in range(len(candles)):
        candles.loc[idx, "pattern"] = _detect_candle_pattern(candles.iloc[: idx + 1])
    return candles.tail(40)


def _detect_candle_pattern(candles: pd.DataFrame) -> str:
    if candles.empty or len(candles) < 2:
        return "Insufficient data"

    last = candles.iloc[-1]
    prev = candles.iloc[-2]
    body = abs(last["close"] - last["open"])
    full_range = max(last["high"] - last["low"], 0.01)
    upper_wick = last["high"] - max(last["open"], last["close"])
    lower_wick = min(last["open"], last["close"]) - last["low"]

    if body / full_range < 0.2:
        return "Doji / indecision"
    if lower_wick > body * 1.8 and last["close"] >= last["open"]:
        return "Hammer / bullish rejection"
    if upper_wick > body * 1.8 and last["close"] <= last["open"]:
        return "Shooting star / bearish rejection"
    if (
        last["close"] > last["open"]
        and prev["close"] < prev["open"]
        and last["close"] >= prev["open"]
        and last["open"] <= prev["close"]
    ):
        return "Bullish engulfing"
    if (
        last["close"] < last["open"]
        and prev["close"] > prev["open"]
        and last["open"] >= prev["close"]
        and last["close"] <= prev["open"]
    ):
        return "Bearish engulfing"
    if last["close"] > last["open"]:
        return "Bullish momentum candle"
    return "Bearish momentum candle"


def _summarize_candle_context() -> dict:
    summary = {}
    for tf in [1, 5, 15]:
        candles = _build_candles(tf)
        if candles.empty:
            summary[f"{tf}m"] = "No candle data yet."
            continue

        recent = candles.tail(3)
        closes = recent["close"].tolist()
        trend = "uptrend" if closes[-1] >= closes[0] else "downtrend"
        high = candles["high"].tail(10).max()
        low = candles["low"].tail(10).min()
        pattern = _detect_candle_pattern(candles)
        summary[f"{tf}m"] = (
            f"Last close {recent.iloc[-1]['close']:.2f}, {trend}, pattern {pattern}, "
            f"recent range {low:.2f}-{high:.2f}."
        )
    return summary


def _compute_breakout_alerts(candles_by_tf, support, resistance):
    alerts = []
    support_level = float(support.get("strike", 0) or 0)
    resistance_level = float(resistance.get("strike", 0) or 0)

    for tf_label, candles in candles_by_tf.items():
        if candles.empty or len(candles) < 3:
            continue

        last = candles.iloc[-1]
        prev = candles.iloc[-2]
        recent_high = float(candles["high"].tail(6).iloc[:-1].max()) if len(candles.tail(6)) > 1 else float(prev["high"])
        recent_low = float(candles["low"].tail(6).iloc[:-1].min()) if len(candles.tail(6)) > 1 else float(prev["low"])

        if resistance_level > 0 and last["close"] > resistance_level and prev["close"] <= resistance_level:
            alerts.append({
                "timeframe": tf_label,
                "direction": "BULLISH",
                "message": f"{tf_label} breakout above resistance {resistance_level:.0f}",
            })
        elif support_level > 0 and last["close"] < support_level and prev["close"] >= support_level:
            alerts.append({
                "timeframe": tf_label,
                "direction": "BEARISH",
                "message": f"{tf_label} breakdown below support {support_level:.0f}",
            })
        elif last["close"] > recent_high and last["close"] > last["open"]:
            alerts.append({
                "timeframe": tf_label,
                "direction": "BULLISH",
                "message": f"{tf_label} momentum breakout above recent swing high {recent_high:.2f}",
            })
        elif last["close"] < recent_low and last["close"] < last["open"]:
            alerts.append({
                "timeframe": tf_label,
                "direction": "BEARISH",
                "message": f"{tf_label} momentum breakdown below recent swing low {recent_low:.2f}",
            })

    return alerts


def _render_candlestick_chart(candles: pd.DataFrame, title: str):
    if candles.empty:
        st.info(f"{title}: waiting for enough live ticks to build candles.")
        return

    base = alt.Chart(candles).encode(
        x=alt.X("timestamp:T", axis=alt.Axis(title=None, format="%H:%M")),
        color=alt.Color(
            "dir:N",
            scale=alt.Scale(domain=["Bullish", "Bearish"], range=["#2e7d32", "#ff5252"]),
            legend=None,
        ),
    )
    wick = base.mark_rule().encode(
        y=alt.Y("low:Q", title="Price"),
        y2="high:Q",
    )
    body = base.mark_bar(size=9).encode(
        y="open:Q",
        y2="close:Q",
        tooltip=[
            alt.Tooltip("label:N", title="Time"),
            alt.Tooltip("open:Q", format=".2f"),
            alt.Tooltip("high:Q", format=".2f"),
            alt.Tooltip("low:Q", format=".2f"),
            alt.Tooltip("close:Q", format=".2f"),
            alt.Tooltip("pattern:N", title="Pattern"),
            alt.Tooltip("display_volume:Q", title="Volume", format=".0f"),
        ],
    )
    label_data = candles.tail(1).copy()
    label_data["label_y"] = label_data["high"] * 1.0008
    labels = alt.Chart(label_data).mark_text(
        align="left",
        dx=8,
        dy=-8,
        fontSize=11,
        fontWeight="bold",
        color="#ffd54f",
    ).encode(
        x="timestamp:T",
        y="label_y:Q",
        text="pattern:N",
    )

    price_chart = (wick + body + labels).properties(height=280, title=title)

    volume_base = alt.Chart(candles).encode(
        x=alt.X("timestamp:T", axis=alt.Axis(title=None, format="%H:%M")),
        color=alt.Color(
            "dir:N",
            scale=alt.Scale(domain=["Bullish", "Bearish"], range=["#2e7d32", "#ff5252"]),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("label:N", title="Time"),
            alt.Tooltip("display_volume:Q", title="Volume", format=".0f"),
        ],
    )
    volume_chart = volume_base.mark_bar(size=9, opacity=0.55).encode(
        y=alt.Y("display_volume:Q", title="Volume"),
    ).properties(height=90)

    st.altair_chart(alt.vconcat(price_chart, volume_chart).resolve_scale(x="shared"), use_container_width=True)

    last = candles.iloc[-1]
    st.caption(
        f"Last candle {last['label']} | O {last['open']:.2f} H {last['high']:.2f} "
        f"L {last['low']:.2f} C {last['close']:.2f} | Pattern: {_detect_candle_pattern(candles)} | "
        f"Volume: {last['display_volume']:.0f}"
    )


# ── Full Data Fetch ───────────────────────────────────────────────────────────
def fetch_all_data(token, symbol, expiry, include_news=False, reset_ai=True, silent=False):
    """Fetch all data sources in one call.

    silent=True suppresses the st.spinner() UI — used during auto-refresh
    so the page doesn't grey out every few seconds.
    """
    # Context manager helper: real spinner if interactive, no-op if silent
    import contextlib
    def _spin(label):
        return contextlib.nullcontext() if silent else st.spinner(label)

    with _spin("Fetching option chain..."):
        data, err = fetch_option_chain_both(token, symbol, expiry)
    if err:
        if not silent:
            st.error(f"❌ Option chain: {err}")
        return False

    st.session_state.option_data = data
    df_calls = pd.DataFrame(data.get("calls", []))
    df_puts  = pd.DataFrame(data.get("puts",  []))
    for df in [df_calls, df_puts]:
        for col in ["strike_price","price","oi","oi_perc_chg","net_chg",
                    "volume","delta","theta","gamma","vega"]:
            df = num(df, col)

    with _spin("Fetching live price..."):
        live = _get_live_price_with_ws_fallback(token, symbol, data)
    st.session_state.live_price = live

    spot = live.get("ltp") or 0
    if spot == 0:
        # Fallback 1: derive from option chain put-call parity
        spot = derive_spot_from_chain(data.get("calls",[]), data.get("puts",[]))
    if spot == 0:
        # Fallback 2: analytics snapshot
        snapshot = compute_market_snapshot(df_calls, df_puts)
        spot = snapshot.get("spot_price", 0)
    if spot == 0 and not df_calls.empty:
        # Fallback 3: use middle strike from chain
        strikes = sorted(df_calls["strike_price"].dropna().tolist())
        spot = float(strikes[len(strikes)//2]) if strikes else 0
    # Store derived spot in live dict for display
    if live.get("ltp", 0) == 0 and spot > 0:
        live["ltp"] = spot
        live["_derived"] = True

    with _spin("Computing indicators..."):
        ind = compute_technical_indicators(
            data.get("calls",[]), data.get("puts",[]), spot, symbol)
        br  = compute_market_breadth(
            data.get("calls",[]), data.get("puts",[]), spot)

    st.session_state.indicators = ind
    st.session_state.breadth    = br

    pcr_data = compute_pcr(df_calls, df_puts)
    snapshot = compute_market_snapshot(df_calls, df_puts)
    if spot == 0:
        spot = snapshot.get("spot_price", 0)

    sent = compute_market_sentiment(
        pcr_data.get("pcr_oi", 1.0), snapshot, ind, live)
    st.session_state.sentiment = sent

    should_refresh_news = include_news or not st.session_state.news or _news_is_stale()
    if should_refresh_news:
        with _spin("Fetching news..."):
            news = fetch_market_news(symbol)
        st.session_state.news = news
        st.session_state.last_news_refresh = datetime.now()

    st.session_state.last_refresh = datetime.now()
    if reset_ai:
        st.session_state.ai_result = None
        st.session_state.ai_result_timestamp = None
    return True


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-header">
            <span class="logo-icon">📊</span>
            <span class="logo-text">OptionsIQ</span>
            <span class="logo-sub">v2.0 · AI Powered</span>
        </div>""", unsafe_allow_html=True)
        st.divider()

        if st.session_state.authenticated:
            st.success("✅ Connected")
            if st.button("🔓 Logout", use_container_width=True):
                for k in ["authenticated","access_token","read_access_token","public_access_token","token_response","api_key","api_secret",
                           "request_token","option_data","expiry_dates",
                           "live_price","indicators","breadth","sentiment",
                           "news","ai_result","ai_result_timestamp"]:
                    st.session_state[k] = False if k=="authenticated" else ([] if k=="expiry_dates" else None if k not in ["api_key","api_secret","access_token","request_token","read_access_token","public_access_token"] else "")
                st.session_state.step = "credentials"
                st.query_params.clear()
                st.rerun()

            st.divider()
            st.markdown("### ⚙️ Controls")
            sym_list = ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]
            symbol = st.selectbox("📈 Symbol", sym_list,
                index=sym_list.index(st.session_state.selected_symbol))
            st.session_state.selected_symbol = symbol

            # ── Risk Profile Selector ────────────────────────────────────
            st.markdown("### 🎯 Risk Profile")
            profile_options = {
                "🛡️ Conservative": "CONSERVATIVE",
                "⚖️ Moderate":      "MODERATE",
                "🔥 Aggressive":    "AGGRESSIVE",
            }
            profile_labels = list(profile_options.keys())
            # Find current index
            cur_val = st.session_state.risk_profile
            cur_label = next((k for k,v in profile_options.items() if v == cur_val), profile_labels[1])
            cur_idx = profile_labels.index(cur_label)

            selected_label = st.radio(
                "Select your risk appetite:",
                profile_labels,
                index=cur_idx,
                key="risk_radio",
                label_visibility="collapsed"
            )
            new_profile = profile_options[selected_label]
            if new_profile != st.session_state.risk_profile:
                st.session_state.risk_profile = new_profile
                st.session_state.ai_result = None  # reset AI result on profile change
                st.session_state.ai_result_timestamp = None

            # Show profile details
            from ai_engine import get_profile
            pdata = get_profile(new_profile)
            st.markdown(f"""
            <div style="background:#f8f9ff;border:1px solid {pdata['color']}33;border-radius:8px;
                        padding:0.6rem 0.8rem;margin:0.3rem 0 0.8rem">
                <div style="color:{pdata['color']};font-size:0.75rem;font-weight:700">{pdata['label']}</div>
                <div style="color:#546e7a;font-size:0.7rem;margin-top:0.2rem">{pdata['description']}</div>
                <div style="display:flex;gap:0.5rem;margin-top:0.4rem;flex-wrap:wrap">
                    <span style="background:#ffffff;color:#2c3e50;border-radius:4px;padding:1px 6px;font-size:0.65rem">
                        Min conf: {pdata['min_confidence']}%
                    </span>
                    <span style="background:#ffffff;color:#2c3e50;border-radius:4px;padding:1px 6px;font-size:0.65rem">
                        Min R:R 1:{pdata['min_rr']}
                    </span>
                    <span style="background:#ffffff;color:#2c3e50;border-radius:4px;padding:1px 6px;font-size:0.65rem">
                        Max {pdata['max_lots']} lots
                    </span>
                    <span style="background:#ffffff;color:#2c3e50;border-radius:4px;padding:1px 6px;font-size:0.65rem">
                        SL: {pdata['sl_pct']}%
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.divider()

            if st.button("🔄 Load Expiry Dates", use_container_width=True):
                with st.spinner("Loading..."):
                    dates, err = fetch_expiry_dates(st.session_state.access_token, symbol)
                if err:
                    st.error(f"❌ {err}")
                else:
                    st.session_state.expiry_dates = dates
                    if dates: st.session_state.selected_expiry = dates[0]
                    st.success(f"✅ {len(dates)} expiries loaded")

            if st.session_state.expiry_dates:
                expiry = st.selectbox("📅 Expiry", st.session_state.expiry_dates)
                st.session_state.selected_expiry = expiry
                st.divider()
                if st.button("🚀 Fetch All Data", use_container_width=True, type="primary"):
                    ok = fetch_all_data(st.session_state.access_token, symbol, expiry, include_news=True)
                    if ok:
                        st.session_state.last_chain_refresh = datetime.now()
                        st.success("✅ All data loaded!")
                        st.rerun()

                st.markdown("---")
                if st.button("🌐 Load Market Intelligence", use_container_width=True):
                    with st.spinner("Loading intelligence..."):
                        try:
                            from market_intelligence import fetch_fii_dii, fetch_global_cues, fetch_india_vix, fetch_market_breadth, fetch_news
                            st.session_state.fii_dii     = fetch_fii_dii(force=True)
                            st.session_state.global_cues = fetch_global_cues(force=True)
                            st.session_state.india_vix   = fetch_india_vix(force=True)
                            st.session_state.mkt_breadth = fetch_market_breadth(force=True)
                            st.session_state.news_items  = fetch_news(max_items=20, force=True)
                            st.session_state.intelligence_loaded = True
                            st.session_state.last_intelligence_refresh = datetime.now()
                            st.success("✅ Intelligence loaded!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Intelligence error: {e}")

            st.divider()
            st.markdown("### 🔁 Auto Refresh")
            auto = st.toggle("Enable", value=st.session_state.auto_refresh)
            st.session_state.auto_refresh = auto
            if auto:
                iv = st.slider("Interval (sec)", 2, 300,
                               st.session_state.refresh_interval, 1)
                st.session_state.refresh_interval = iv
                st.caption(f"⚡ Refreshing every {iv}s")

            st.divider()
            with st.expander("🔧 API Diagnostics"):
                if st.button("Run Test", use_container_width=True):
                    _run_diagnostics(st.session_state.access_token, symbol)
        else:
            st.info("Complete login on the right →")
            if st.button("🧪 Demo Mode", use_container_width=True):
                st.session_state.access_token = "DEMO"
                st.session_state.authenticated = True
                st.session_state.step = "dashboard"
                st.rerun()

        st.divider()
        with st.expander("🤖 AI Settings (Anthropic)"):
            st.caption("Get your key: console.anthropic.com")
            ak = st.text_input(
                "Anthropic API Key",
                value=st.session_state.anthropic_api_key,
                type="password",
                placeholder="sk-ant-...",
                key="ant_key_input"
            )
            if ak:
                clean_key = ak.strip().strip('"').strip("'").replace("\n","").replace("\r","").replace(" ","")
                st.session_state.anthropic_api_key = clean_key
                st.success("✅ AI key set")
            if not st.session_state.anthropic_api_key:
                st.warning("Required for AI recommendations")

        st.divider()
        with st.expander("📊 Trade History & Win Rate"):
            render_trade_history()

        with st.expander("⚡ Live WebSocket Feed"):
            st.caption("WebSocket is optional. The app now uses Paytm Live Market Data API polling automatically when public_access_token is unavailable.")
            token_response = st.session_state.get("token_response") or {}
            if st.session_state.public_access_token:
                st.success("Public access token loaded from login response.")
            elif token_response:
                st.warning("Login token response did not include public_access_token.")
            pub_tok = st.text_input(
                "Public Access Token",
                value=st.session_state.public_access_token,
                type="password",
                placeholder="public_access_token from login response",
                key="pub_tok_input"
            )
            if pub_tok:
                st.session_state.public_access_token = pub_tok
            elif st.session_state.ws_enabled and not st.session_state.public_access_token:
                st.session_state.ws_enabled = False

            feed = get_feed()
            ws_sym = st.session_state.selected_symbol

            if st.session_state.ws_enabled and feed.is_connected:
                tick_age = feed.last_tick_age
                age_str = f"{tick_age:.0f}s ago" if tick_age is not None else "—"
                st.success(f"🟢 Connected — last tick {age_str}")
                if st.button("⏹ Stop WebSocket", use_container_width=True):
                    feed.stop()
                    st.session_state.ws_enabled = False
                    st.rerun()
            else:
                if st.session_state.ws_enabled:
                    err = feed.error or "Connecting..."
                    st.warning(f"⏳ {err}")
                if st.button("▶ Start Live Feed", use_container_width=True, type="primary"):
                    if st.session_state.public_access_token:
                        ok = feed.start(st.session_state.public_access_token, ws_sym)
                        if ok:
                            st.session_state.ws_enabled = True
                            st.session_state.ws_symbol  = ws_sym
                            st.rerun()
                        else:
                            st.error(feed.error or "Failed to start")
                    else:
                        st.warning("Enter public_access_token first")

            if token_response and not st.session_state.public_access_token:
                with st.expander("Show token response", expanded=False):
                    st.json(token_response)

            st.caption("📌 Get public_access_token from the generate token response (field: public_access_token)")

        st.markdown("<div class='sidebar-footer'>OptionsIQ v2.0 | Educational use only</div>",
                    unsafe_allow_html=True)


def _run_diagnostics(token, symbol):
    import datetime as dt
    hdrs = {"x-jwt-token": token, "Content-Type": "application/json"}

    # Step 1: Get real expiry from config
    test_expiry = "13-04-2026"
    try:
        cfg = requests.get(f"{PAYTM_BASE}/fno/v1/option-chain/config",
                           headers=hdrs, params={"symbol": symbol}, timeout=10)
        expires = cfg.json().get("data", {}).get("expires", [])
        if expires:
            test_expiry = dt.datetime.fromtimestamp(int(expires[0])/1000).strftime("%d-%m-%Y")
    except Exception:
        pass

    st.info(f"Using expiry **{test_expiry}** for option chain test")

    # ── Test 1: Option Chain — show full structure ─────────────────────────
    st.markdown("### 1. Option Chain (CALL) — Full Structure")
    try:
        r = requests.get(f"{PAYTM_BASE}/fno/v1/option-chain",
                         headers=hdrs,
                         params={"type":"CALL","symbol":symbol,"expiry":test_expiry},
                         timeout=12)
        st.markdown(f"{'🟢' if r.status_code==200 else '🔴'} **Status: {r.status_code}**")
        body = r.json()
        data = body.get("data", {})
        results = data.get("results", []) if isinstance(data, dict) else []

        st.markdown(f"**Top-level keys:** `{list(body.keys())}`")
        st.markdown(f"**data keys:** `{list(data.keys()) if isinstance(data,dict) else type(data)}`")

        if results:
            st.markdown(f"**results:** {len(results)} batches")
            # Flatten
            flat = []
            for batch in results:
                if isinstance(batch, list): flat.extend(batch)
                elif isinstance(batch, dict): flat.append(batch)
            st.markdown(f"**Total records after flatten:** {len(flat)}")
            if flat:
                st.markdown(f"**First record keys:** `{list(flat[0].keys())}`")
                st.markdown("**First record (full):**")
                st.json(flat[0])
                if len(flat) > 1:
                    st.markdown("**Second record (full):**")
                    st.json(flat[1])
        else:
            st.warning("No 'results' key found")
            st.json(body)
    except Exception as e:
        st.error(str(e))
    st.divider()

    # ── Test 2: Live Price — try all pref formats ─────────────────────────
    st.markdown("### 2. Live Price — Testing All Pref Formats")
    pref_list = [
        f"NSE|INDEX|NIFTY 50",
        f"NSE|INDEX|{symbol}",
        f"NSE|EQ|{symbol}",
        f"NSE:NIFTY 50",
        f"NSE:NIFTY",
        f"NSE|INDEX|NIFTY50",
    ]
    for pref in pref_list:
        try:
            r2 = requests.get(f"{PAYTM_BASE}/data/v1/price/live",
                              headers=hdrs,
                              params={"mode":"LTP","pref":pref},
                              timeout=8)
            body2 = r2.json()
            d = body2.get("data",[{}])
            item = d[0] if isinstance(d,list) and d else {}
            found = item.get("found","?")
            ltp = item.get("last_price", item.get("ltp","?"))
            icon = "✅" if found is True else "❌"
            st.markdown(f"{icon} `{pref}` → found={found} ltp={ltp}")
            if found is True:
                st.json(item)
                break
        except Exception as e:
            st.markdown(f"❌ `{pref}` → error: {e}")
    st.divider()

    # ── Test 3: User Profile ──────────────────────────────────────────────
    st.markdown("### 3. User Profile")
    try:
        r3 = requests.get(f"{PAYTM_BASE}/accounts/v1/user/details", headers=hdrs, timeout=8)
        st.markdown(f"{'🟢' if r3.status_code==200 else '🔴'} **{r3.status_code}**")
        st.json(r3.json())
    except Exception as e:
        st.error(str(e))


# ── Auth Pages ────────────────────────────────────────────────────────────────
def page_credentials():
    st.markdown("""
    <div class="main-header">
        <h1>📊 OptionsIQ v2.0</h1>
        <p class="header-sub">Complete Options Intelligence Platform · Login to get started</p>
    </div>""", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("### 🔑 Step 1 — API Credentials")
        st.caption("From: developer.paytmmoney.com → My Apps → Trading API")
        api_key = st.text_input("API Key", value=st.session_state.api_key,
                                placeholder="Paste API Key")
        api_secret = st.text_input("API Secret", value=st.session_state.api_secret,
                                   type="password", placeholder="Paste API Secret")
        if api_key: st.session_state.api_key = api_key
        if api_secret: st.session_state.api_secret = api_secret

    with col2:
        st.markdown("### 🚀 Step 2 — Login")
        if api_key and api_secret:
            login_url = f"https://login.paytmmoney.com/merchant-login?apiKey={api_key}&state=optionsiq"
            st.markdown("Click below → login → redirected back automatically:")
            st.link_button("🔗 Open Paytm Money Login →", login_url,
                          use_container_width=True, type="primary")
            st.divider()
            st.caption("If not redirected automatically, paste requestToken here:")
            manual = st.text_input("requestToken (manual)", placeholder="abc123...")
            if manual:
                st.session_state.request_token = manual
                st.session_state.step = "exchange"
                st.rerun()
        else:
            st.info("Enter API credentials on the left first.")


def page_exchange():
    st.markdown("""<div class="main-header">
        <h1>🔄 Authenticating...</h1>
        <p class="header-sub">Exchanging token with Paytm Money</p>
    </div>""", unsafe_allow_html=True)

    req = st.session_state.request_token
    key = st.session_state.api_key
    sec = st.session_state.api_secret

    if not req:
        st.error("Missing request token. Please log in again.")
        if st.button("← Back"):
            st.session_state.step = "credentials"
            st.rerun()
        return

    if not key or not sec:
        st.warning("API credentials were not available after redirect. Enter them once more to complete token exchange.")
        key = st.text_input("API Key", value=key, placeholder="Paste API Key")
        sec = st.text_input("API Secret", value=sec, type="password", placeholder="Paste API Secret")
        if key:
            st.session_state.api_key = key
        if sec:
            st.session_state.api_secret = sec
        if not key or not sec:
            return

    st.info(f"✅ requestToken: `{req[:20]}...`")

    with st.spinner("Getting access token from Paytm Money..."):
        try:
            resp = requests.post(
                f"{PAYTM_BASE}/accounts/v2/gettoken",
                json={"api_key": key, "api_secret_key": sec, "request_token": req},
                timeout=15)
            body = resp.json()
            if resp.status_code == 200:
                token = (body.get("data",{}).get("access_token") or
                         body.get("data",{}).get("read_access_token") or
                         body.get("access_token"))
                if token:
                    st.session_state.access_token = token
                    token_data = body.get("data", {}) if isinstance(body, dict) else {}
                    st.session_state.read_access_token = token_data.get("read_access_token") or body.get("read_access_token") or ""
                    st.session_state.public_access_token = token_data.get("public_access_token") or body.get("public_access_token") or ""
                    st.session_state.token_response = body
                    st.session_state.authenticated = True
                    st.session_state.step = "dashboard"
                    st.query_params.clear()
                    if st.session_state.public_access_token:
                        st.success("public_access_token captured from login response.")
                    else:
                        st.warning("Login succeeded, but public_access_token was not found in the token response.")
                        with st.expander("Token response received from Paytm", expanded=False):
                            st.json(body)
                    st.success("✅ Authenticated! Loading dashboard...")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Token not in response:"); st.json(body)
            else:
                st.error(f"❌ Error {resp.status_code}")
                st.json(body)
                if resp.status_code == 400:
                    st.warning("requestToken expired (2-3 min limit). Login again.")
                if st.button("← Login Again", type="primary"):
                    st.session_state.request_token = ""
                    st.session_state.step = "credentials"
                    st.query_params.clear()
                    st.rerun()
        except Exception as e:
            st.error(f"❌ {e}")


# ── Dashboard ─────────────────────────────────────────────────────────────────

def _render_intelligence(fii_dii, global_cues, india_vix, mkt_breadth, news_items):
    t1, t2, t3, t4 = st.tabs(["🌍 Global Cues", "💰 FII/DII", "📊 VIX & Breadth", "📰 News"])
    with t1:
        gdata = global_cues.get("data", {}); gsent = global_cues.get("global_sentiment", "N/A")
        gc = "#1b5e20" if "BULLISH" in gsent else "#c62828" if "BEARISH" in gsent else "#3949ab"
        bg = "#e8f5e9" if "BULLISH" in gsent else "#fce8e8" if "BEARISH" in gsent else "#e8eaf6"
        st.markdown(f'''<div style="background:{bg};border:1px solid {gc}55;border-radius:8px;padding:8px 14px;margin-bottom:8px;display:flex;justify-content:space-between">
            <span style="color:{gc};font-weight:700">Global: {gsent}</span>
            <span style="color:#1565c0">🎯 {global_cues.get("gift_nifty_signal","N/A")}</span></div>
        <div style="font-size:.7rem;color:#78909c;margin-bottom:6px">GIFT Nifty source: {global_cues.get("gift_nifty_source","Unknown")}</div>''', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, name in enumerate(["GIFT Nifty","Dow Jones","Nasdaq","S&P 500","Nikkei 225","Hang Seng","DAX","FTSE 100","Crude Oil","Gold","USD/INR","India VIX"]):
            d = gdata.get(name, {}); p = d.get("price", 0); chg = d.get("chg_pct", 0)
            clr = "#1b5e20" if chg > 0 else "#c62828" if chg < 0 else "#555"
            bg2 = "#f0faf4" if chg > 0 else "#fdf0f0" if chg < 0 else "#f8f8f8"
            with cols[i % 4]:
                st.markdown(f'''<div style="background:{bg2};border:1px solid #ddd;border-radius:8px;padding:8px;text-align:center;margin-bottom:4px">
                    <div style="font-size:.6rem;color:#78909c">{name}</div>
                    <div style="font-family:monospace;font-size:.9rem;color:#1a237e;font-weight:600">{p:,.1f}</div>
                    <div style="font-size:.72rem;color:{clr}">{"▲" if chg>0 else "▼" if chg<0 else "—"} {chg:+.2f}%</div>
                </div>''', unsafe_allow_html=True)
    with t2:
        fn2 = fii_dii.get("fii_net",0); dn2 = fii_dii.get("dii_net",0); nc = fn2+dn2
        c1, c2, c3 = st.columns(3)
        for col, lbl, val, sig, sub in [
            (c1,"FII NET (₹Cr)",fn2,fii_dii.get("fii_signal","N/A"),f"Buy ₹{fii_dii.get('fii_buy',0):,.0f} | Sell ₹{fii_dii.get('fii_sell',0):,.0f}"),
            (c2,"DII NET (₹Cr)",dn2,fii_dii.get("dii_signal","N/A"),f"Buy ₹{fii_dii.get('dii_buy',0):,.0f} | Sell ₹{fii_dii.get('dii_sell',0):,.0f}"),
            (c3,"COMBINED (₹Cr)",nc,fii_dii.get("combined_signal","N/A"),f"As of {fii_dii.get('date','today')} · {fii_dii.get('source','')}"),
        ]:
            clr = "#1b5e20" if val>0 else "#c62828" if val<0 else "#555"
            bg  = "#e8f5e9" if val>0 else "#fce8e8" if val<0 else "#f8f8f8"
            with col:
                st.markdown(f'''<div style="background:{bg};border-top:3px solid {clr};border:1px solid #ddd;border-radius:8px;padding:1rem;text-align:center">
                    <div style="font-size:.65rem;color:#78909c">{lbl}</div>
                    <div style="font-size:1.8rem;font-weight:700;font-family:monospace;color:{clr}">{val:+,.0f}</div>
                    <div style="font-size:.78rem;color:{clr}">{sig}</div>
                    <div style="font-size:.65rem;color:#999">{sub}</div>
                </div>''', unsafe_allow_html=True)
        st.caption("FII >+500Cr = BULLISH · FII <-500Cr = BEARISH · DII typically counters FII")
        hist = fii_dii.get("recent_history", [])
        if len(hist) >= 2:
            with st.expander("📊 FII/DII 5-Day Trend"):
                for h in hist:
                    fn3 = h.get("fii_net",0); dn3 = h.get("dii_net",0)
                    st.markdown(f"**{h.get('date','')}** — FII {'🟢' if fn3>0 else '🔴'} ₹{fn3:+,.0f}Cr | DII {'🟢' if dn3>0 else '🔴'} ₹{dn3:+,.0f}Cr")
    with t3:
        vix = india_vix.get("vix",0); vc = "#e65100" if vix>20 else "#1b5e20" if vix<15 else "#7f5a00"
        adv = mkt_breadth.get("advances",0); dec = mkt_breadth.get("declines",0); t_ = adv+dec or 1
        adr = mkt_breadth.get("adv_dec_ratio",0); bs = mkt_breadth.get("breadth_signal","N/A")
        bc = "#1b5e20" if "BULLISH" in bs else "#c62828" if "BEARISH" in bs else "#555"
        v1, v2 = st.columns([1, 2])
        with v1:
            st.markdown(f'''<div style="background:#fff8e1;border-top:3px solid {vc};border:1px solid #ddd;border-radius:8px;padding:1rem;text-align:center">
                <div style="font-size:.65rem;color:#78909c">INDIA VIX</div>
                <div style="font-size:2.8rem;font-weight:700;color:{vc}">{vix:.2f}</div>
                <div style="font-size:.8rem;color:{vc}">{india_vix.get("chg_pct",0):+.2f}% · {india_vix.get("level","N/A")}</div>
                <div style="font-size:.72rem;color:#555;margin-top:4px">{india_vix.get("signal","N/A")}</div>
            </div>''', unsafe_allow_html=True)
        with v2:
            st.markdown(f'''<div style="background:#f8f9ff;border:1px solid #ddd;border-radius:8px;padding:1rem">
                <div style="font-size:.7rem;color:#78909c;margin-bottom:8px">MARKET BREADTH (NIFTY 50)</div>
                <div style="display:flex;gap:8px;margin-bottom:6px">
                    <div style="flex:1;background:#e8f5e9;border-radius:6px;padding:8px;text-align:center">
                        <div style="font-size:.6rem;color:#78909c">ADV</div><div style="font-size:1.3rem;font-weight:700;color:#1b5e20">{adv}</div><div style="font-size:.65rem;color:#1b5e20">{adv/t_*100:.0f}%</div></div>
                    <div style="flex:1;background:#fce8e8;border-radius:6px;padding:8px;text-align:center">
                        <div style="font-size:.6rem;color:#78909c">DEC</div><div style="font-size:1.3rem;font-weight:700;color:#c62828">{dec}</div><div style="font-size:.65rem;color:#c62828">{dec/t_*100:.0f}%</div></div>
                    <div style="flex:1;background:#f0f0f8;border-radius:6px;padding:8px;text-align:center">
                        <div style="font-size:.6rem;color:#78909c">A/D</div><div style="font-size:1.3rem;font-weight:700;color:{bc}">{adr:.2f}</div><div style="font-size:.65rem;color:{bc}">{bs}</div></div>
                </div>
                <div style="font-size:.7rem;color:#555">A/D > 1.5 = buy calls · A/D < 0.67 = buy puts</div>
            </div>''', unsafe_allow_html=True)
    with t4:
        if not news_items:
            st.info("Click 🌐 Load Market Intelligence in the sidebar to fetch news.")
            return
        fc1, fc2, fc3 = st.columns(3)
        with fc1: show_bull = st.checkbox("🟢 Bullish", key="nf_bull")
        with fc2: show_bear = st.checkbox("🔴 Bearish", key="nf_bear")
        with fc3: show_rel  = st.checkbox("⭐ Relevant", key="nf_rel")
        filtered = news_items
        if show_bull:   filtered = [n for n in news_items if n.get("sentiment") == "BULLISH"]
        elif show_bear: filtered = [n for n in news_items if n.get("sentiment") == "BEARISH"]
        elif show_rel:  filtered = sorted(news_items, key=lambda x: x.get("score",0), reverse=True)[:10]
        for item in filtered[:20]:
            s  = item.get("sentiment","NEUTRAL")
            sc = "#1b5e20" if s == "BULLISH" else "#c62828" if s == "BEARISH" else "#555"
            bg = "#e8f5e9" if s == "BULLISH" else "#fce8e8" if s == "BEARISH" else "#f8f8f8"
            lnk = item.get("link",""); ttl = item.get("title","")
            src2 = item.get("source",""); pub = item.get("pubdate","")
            href = (f'<a href="{lnk}" target="_blank" style="color:#1565c0;text-decoration:none">{ttl}</a>'
                    if lnk else f'<span>{ttl}</span>')
            st.markdown(f'''<div style="border-left:3px solid {sc};padding:6px 12px;margin:3px 0;background:{bg};border-radius:0 6px 6px 0">
                <div style="font-size:.84rem">{href}</div>
                <div style="font-size:.66rem;color:#78909c"><span style="color:{sc};font-weight:600">{s}</span> · {src2} · {pub}</div>
            </div>''', unsafe_allow_html=True)
        st.caption(f"Showing {len(filtered)} of {len(news_items)} headlines")



def page_dashboard():
    # Header
    sym  = st.session_state.selected_symbol
    exp  = st.session_state.selected_expiry or "—"
    ts   = st.session_state.last_refresh

    # ═══════════════════════════════════════════════════════════════════════
    # AUTO-REFRESH TIMERS — must be registered BEFORE any rendering so that
    # the fetch happens early in the rerun and the rendered widgets read the
    # fresh data.
    #
    # IMPORTANT: auto-refresh pauses while AI is generating, so it cannot
    # interrupt the AI API call or produce a stale "AI key not set" error.
    # ═══════════════════════════════════════════════════════════════════════
    _ai_in_progress = st.session_state.get("ai_loading", False)
    _user_paused = st.session_state.get("_refresh_paused", False)
    _auto_refresh_active = (
        st.session_state.get("auto_refresh")
        and st.session_state.get("option_data") is not None
        and st.session_state.get("selected_expiry")
        and not _ai_in_progress                     # ← pause during AI call
        and not _user_paused                        # ← user-controlled pause
    )

    if _auto_refresh_active:
        _slow_interval_s = max(2, int(st.session_state.refresh_interval))
        _feed_live = (
            st.session_state.get("ws_enabled")
            and get_feed().is_connected
        )

        # Tier 1 — 1-second WS tick (only when WS connected). No API call,
        # just triggers a rerun so the render picks up the latest in-memory
        # WS price dict.
        if _feed_live:
            st_autorefresh(interval=1000, key="options_ws_tick_refresh")

        # Tier 2 — Full data refresh on the user-configured interval.
        _slow_tick = st_autorefresh(
            interval=_slow_interval_s * 1000,
            key="options_full_refresh",
        )

        # Only fetch on a scheduled rerun (tick > 0) and guard against
        # double-fetches if WS ticks race the slow tick.
        if _slow_tick and _slow_tick > 0:
            _last = st.session_state.get("_last_auto_fetch_ts")
            _now_ts = datetime.now()
            if _last is None or (_now_ts - _last).total_seconds() >= (_slow_interval_s - 0.5):
                st.session_state._last_auto_fetch_ts = _now_ts
                try:
                    fetch_all_data(
                        st.session_state.access_token,
                        sym,
                        st.session_state.selected_expiry,
                        include_news=_news_is_stale(),
                        reset_ai=False,   # ← preserves AI recommendation
                        silent=True,      # ← no spinners, no grey-out
                    )
                except Exception as _e:
                    # Swallow transient errors so the loop keeps running
                    st.toast(f"⚠️ Refresh skipped: {_e}", icon="⚠️")

    st.markdown(f"""
    <div class="main-header">
        <h1>📊 OptionsIQ — {sym}</h1>
        <p class="header-sub">
            Expiry: <strong>{exp}</strong> &nbsp;|&nbsp;
            {'Last updated: <strong>' + (st.session_state.last_refresh.strftime('%H:%M:%S') if st.session_state.last_refresh else '—') + '</strong>' if st.session_state.last_refresh else 'No data loaded yet'}
        </p>
    </div>""", unsafe_allow_html=True)

    # ── Market Status Banner ─────────────────────────────────────────────────
    mkt = get_market_status()
    st.session_state.market_status = mkt
    st.markdown(market_status_banner(mkt), unsafe_allow_html=True)

    # ── Auto-start WebSocket if token is available and market is open ──────
    # This saves the user a manual click. Only runs once per session — if the
    # WS disconnects mid-session, the user can restart from the sidebar.
    if (
        mkt.get("is_open")
        and st.session_state.get("public_access_token")
        and not st.session_state.get("ws_enabled")
        and not st.session_state.get("ws_auto_start_tried")
    ):
        try:
            _feed = get_feed()
            if _feed.start(
                st.session_state.public_access_token,
                st.session_state.selected_symbol,
            ):
                st.session_state.ws_enabled = True
                st.session_state.ws_symbol = st.session_state.selected_symbol
        except Exception:
            pass
        # Mark as tried so we don't loop on failure
        st.session_state.ws_auto_start_tried = True
    # ── Data freshness strip
    _lv = st.session_state.get("live_price") or {}
    _src, _ltp = _lv.get("source","?"), float(_lv.get("ltp",0) or 0)
    _ls = ("✅ Real-time" if ("Paytm" in _src or "NSE" in _src) and _ltp>0
           else "⚠️ 15-min delay" if "Yahoo" in _src and _ltp>0 else "❌ No data")
    _lc = "#1b5e20" if "✅" in _ls else "#f57f17" if "⚠️" in _ls else "#c62828"
    _rc = st.session_state.get("last_chain_refresh") or st.session_state.get("last_refresh")
    _rs = (f"✅ Fresh ({int((datetime.now()-_rc).total_seconds())}s)" if _rc and int((datetime.now()-_rc).total_seconds())<60
           else f"⚠️ {int((datetime.now()-_rc).total_seconds())//60}m old" if _rc else "❌ Not loaded")
    _rcl = "#1b5e20" if "✅" in _rs else "#f57f17" if "⚠️" in _rs else "#c62828"
    _ir, _il = st.session_state.get("last_intelligence_refresh"), st.session_state.get("intelligence_loaded")
    _is = ("✅ Fresh" if _il and _ir and int((datetime.now()-_ir).total_seconds())<300
           else f"⚠️ {int((datetime.now()-_ir).total_seconds())//60}m old" if _il and _ir else "⚪ Not loaded")
    _icl = "#1b5e20" if "✅" in _is else "#f57f17" if "⚠️" in _is else "#78909c"
    st.markdown(f"""<div style="display:flex;gap:8px;margin:8px 0 12px 0;flex-wrap:wrap">
        <div style="flex:1;background:#fff;border:1px solid #e0e4ec;border-left:4px solid {_lc};border-radius:6px;padding:6px 10px;font-size:.72rem"><span style="color:#78909c;font-size:.62rem">LIVE PRICE</span><span style="color:{_lc};margin-left:8px;font-weight:600">{_ls}</span></div>
        <div style="flex:1;background:#fff;border:1px solid #e0e4ec;border-left:4px solid {_rcl};border-radius:6px;padding:6px 10px;font-size:.72rem"><span style="color:#78909c;font-size:.62rem">OPTION CHAIN</span><span style="color:{_rcl};margin-left:8px;font-weight:600">{_rs}</span></div>
        <div style="flex:1;background:#fff;border:1px solid #e0e4ec;border-left:4px solid {_icl};border-radius:6px;padding:6px 10px;font-size:.72rem"><span style="color:#78909c;font-size:.62rem">MARKET INTEL</span><span style="color:{_icl};margin-left:8px;font-weight:600">{_is}</span></div>
    </div>""", unsafe_allow_html=True)

    if st.session_state.option_data is None:
        st.markdown("""<div class="info-card">
            <h3>📡 Ready — Load Your Data</h3>
            <ol>
                <li>Select symbol in sidebar</li>
                <li>Click <strong>Load Expiry Dates</strong></li>
                <li>Select an expiry</li>
                <li>Click <strong>🚀 Fetch All Data</strong></li>
            </ol>
        </div>""", unsafe_allow_html=True)
        return

    # ── Prepare data ──────────────────────────────────────────────────────────
    if mkt.get("is_open") and not st.session_state.ws_enabled:
        try:
            _poll_runtime_market_data(
                st.session_state.access_token,
                sym,
                st.session_state.selected_expiry,
            )
        except Exception:
            pass

    data = st.session_state.option_data
    if not data:
        st.warning("⚠️ Data temporarily unavailable — will reload on next refresh.")
        return
    df_calls = pd.DataFrame(data.get("calls", []))
    df_puts  = pd.DataFrame(data.get("puts",  []))
    for df in [df_calls, df_puts]:
        for col in ["strike_price","price","oi","oi_perc_chg","net_chg",
                    "volume","delta","theta","gamma","vega"]:
            df = num(df, col)

    live     = st.session_state.live_price or {}
    ind      = st.session_state.indicators or {}

    # ── WebSocket live data override ──────────────────────────────────────
    feed = get_feed()
    if st.session_state.ws_enabled and feed.is_connected:
        ws_price = feed.get_price()
        if ws_price and ws_price.get("ltp", 0) > 0:
            live = ws_price   # Override with real-time WebSocket data
            st.session_state.live_price = live
    _record_live_tick(sym, live)
    candles_5m = _build_candles(5)
    br       = st.session_state.breadth or {}
    sent     = st.session_state.sentiment or {"score": 0, "sentiment": "NEUTRAL", "color": "#8888cc", "signals": []}
    news     = st.session_state.news or []

    snapshot = compute_market_snapshot(df_calls, df_puts)
    spot     = live.get("ltp") or snapshot.get("spot_price", 0)
    atm      = identify_atm_strike(df_calls, df_puts, spot)
    df_calls, df_puts = classify_strikes(df_calls, df_puts, atm)
    pcr_data = compute_pcr(df_calls, df_puts)
    sup, res = find_support_resistance(df_calls, df_puts, spot=spot, nearby_range=500)
    intraday_lvls = find_intraday_levels(df_calls, df_puts, spot=spot, timeframe_pts=150)
    sup, res = _derive_hybrid_levels(sup, res, intraday_lvls, candles_5m)
    buildup  = analyze_oi_buildup(df_calls, df_puts)
    smart    = track_smart_money(df_calls, df_puts)
    sent     = compute_market_sentiment(
        pcr_data.get("pcr_oi", 1.0), snapshot, ind, live
    )
    st.session_state.sentiment = sent

    top_calls = df_calls.nlargest(5,"oi").to_dict("records") if not df_calls.empty else []
    top_puts  = df_puts.nlargest(5,"oi").to_dict("records")  if not df_puts.empty else []

    # ── Refresh button ────────────────────────────────────────────────────────
    rc1, _, rc3 = st.columns([3,1,1])
    with rc3:
        if st.button("🔄 Refresh All", use_container_width=True):
            ok = fetch_all_data(
                st.session_state.access_token,
                sym,
                st.session_state.selected_expiry,
                include_news=_news_is_stale(),
            )
            if ok: st.rerun()

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 1 — LIVE PRICE
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("### 💹 Live Market Price")
    chg   = live.get("change", 0)
    chgp  = live.get("change_pct", 0)
    c_cls = "up" if chg >= 0 else "dn"
    chg_sym = "▲" if chg >= 0 else "▼"

    c1, c2, c3, c4, c5 = st.columns(5)
    src = live.get("source","")
    if "WebSocket" in src or "websocket" in src.lower():
        price_note = "⚡ WebSocket (live)"
        tick_ts = live.get("timestamp","")
    elif "NSE India" in src:
        price_note = "NSE API (~15s delay)"
        tick_ts = ""
    elif "Yahoo" in src:
        price_note = "Yahoo Finance (~15min delay)"
        tick_ts = ""
    elif "option_chain" in src:
        price_note = "Option chain spot"
        tick_ts = ""
    else:
        price_note = src or "Demo"
        tick_ts = ""
    cards = [
        ("LTP", f"₹{spot:,.2f}", f'<span class="{c_cls}">{chg_sym} {abs(chg):,.2f} ({abs(chgp):.2f}%)</span> <span style="font-size:0.6rem;color:#78909c">({price_note})</span>'),
        ("Open", f"₹{live.get('open',0):,.2f}", "Today's open"),
        ("High", f"₹{live.get('high',0):,.2f}", '<span class="up">Intraday high</span>'),
        ("Low",  f"₹{live.get('low',0):,.2f}",  '<span class="dn">Intraday low</span>'),
        ("Prev Close", f"₹{live.get('prev_close',0):,.2f}", "Yesterday close"),
    ]
    for col, (label, val, sub) in zip([c1,c2,c3,c4,c5], cards):
        with col:
            st.markdown(f"""<div class="metric-card" style="--accent:#00d4ff">
                <div class="metric-label">{label}</div>
                <div class="metric-value" style="font-size:1.3rem">{val}</div>
                <div class="metric-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    # Always build candles — sent to AI regardless of show/hide
    candles_1m  = _build_candles(1)
    candles_5m  = _build_candles(5)
    candles_15m = _build_candles(15)
    candle_summary  = _summarize_candle_context()
    breakout_alerts = _compute_breakout_alerts(
        {"1M": candles_1m, "5M": candles_5m, "15M": candles_15m}, sup, res,
    )
    # Show/Hide toggle
    _ch1, _ch2 = st.columns([6, 1])
    with _ch1: st.markdown("### 📉 Live Candle Chart")
    with _ch2:
        if st.button("Hide 👁" if st.session_state.get("show_chart", True) else "Show 👁",
                     key="chart_toggle", use_container_width=True):
            st.session_state["show_chart"] = not st.session_state.get("show_chart", True)
            st.rerun()
    if st.session_state.get("show_chart", True):
        tf1, tf5, tf15 = st.tabs(["1M", "5M", "15M"])
        with tf1: _render_candlestick_chart(candles_1m, "1 Minute Candles")
        with tf5: _render_candlestick_chart(candles_5m, "5 Minute Candles")
        with tf15: _render_candlestick_chart(candles_15m, "15 Minute Candles")

    st.caption(
        "AI candle context: "
        + " | ".join([f"{k.upper()}: {v}" for k, v in candle_summary.items()])
    )
    if breakout_alerts:
        st.markdown("**🚨 Breakout Alerts**")
        for alert in breakout_alerts:
            alert_color = "#2e7d32" if alert["direction"] == "BULLISH" else "#ff5252"
            st.markdown(
                f'<div style="background:#ffffff;border-left:3px solid {alert_color};'
                f'padding:0.45rem 0.8rem;border-radius:0 8px 8px 0;margin:0.25rem 0;'
                f'font-size:0.82rem;color:#2c3e50">{alert["timeframe"]}: {alert["message"]}</div>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("No live breakout alerts yet.")

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 2 — MARKET INTERNALS (Sentiment + PCR + Levels in one row)
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("### 🧠 Market Internals")
    m1, m2, m3 = st.columns([1.2, 1, 1.8])

    with m1:
        # Sentiment Score
        score  = sent.get("score", 0)
        scolor = sent.get("color", "#8888cc")
        slabel = sent.get("sentiment", "NEUTRAL")
        bar_w  = int((score + 100) / 2)  # -100..100 → 0..100%
        st.markdown(f"""<div class="section-card">
            <div class="section-title">📊 Sentiment Score</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:2.5rem;
                        font-weight:700;color:{scolor};text-align:center">{score:+d}</div>
            <div style="text-align:center;color:{scolor};font-weight:700;
                        font-size:0.9rem;letter-spacing:1px">{slabel}</div>
            <div class="sentiment-bar">
                <div style="width:{bar_w}%;height:100%;background:{scolor};border-radius:7px"></div>
            </div>
            <div style="display:flex;justify-content:space-between;font-size:0.6rem;color:#2a4a6a">
                <span>-100 BEARISH</span><span>0</span><span>+100 BULLISH</span>
            </div>
        </div>""", unsafe_allow_html=True)

    with m2:
        # PCR
        pcr   = pcr_data.get("pcr_oi", 0)
        pcr_c = "#2e7d32" if pcr > 1.2 else "#c62828" if pcr < 0.8 else "#8888cc"
        bar_p = int(min(100, (pcr / 2.0) * 100))
        st.markdown(f"""<div class="section-card">
            <div class="section-title">⚖️ Put-Call Ratio</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:2.5rem;
                        font-weight:700;color:{pcr_c};text-align:center">{pcr:.3f}</div>
            <div style="text-align:center;color:{pcr_c};font-size:0.85rem;font-weight:700">
                {pcr_data.get('interpretation','N/A')}
            </div>
            <div class="sentiment-bar">
                <div style="width:{bar_p}%;height:100%;background:{pcr_c};border-radius:7px"></div>
            </div>
            <div style="font-size:0.65rem;color:#2a4a6a;text-align:center">
                Vol PCR: {pcr_data.get('pcr_volume',0):.3f}
            </div>
        </div>""", unsafe_allow_html=True)

    with m3:
        # Key Levels compact
        sup_s = sup.get("strike", 0)
        res_s = res.get("strike", 0)
        mp    = ind.get("max_pain", 0)
        gw    = br.get("gamma_wall", 0)
        rng_pct = ""
        if sup_s > 0 and res_s > 0 and res_s != sup_s:
            p = ((spot - sup_s) / (res_s - sup_s)) * 100
            rng_pct = f"{p:.0f}%"
        st.markdown(f"""<div class="section-card">
            <div class="section-title">🎯 Key Levels</div>
            <div class="kpi-row">
                <div class="kpi-box">
                    <div class="kpi-label">Support</div>
                    <div class="kpi-val up">{sup_s:,.0f}</div>
                    <div class="kpi-sub">Max PUT OI</div>
                </div>
                <div class="kpi-box">
                    <div class="kpi-label">Spot</div>
                    <div class="kpi-val" style="color:#1565c0">{spot:,.0f}</div>
                    <div class="kpi-sub">Range: {rng_pct}</div>
                </div>
                <div class="kpi-box">
                    <div class="kpi-label">Resistance</div>
                    <div class="kpi-val dn">{res_s:,.0f}</div>
                    <div class="kpi-sub">Max CALL OI</div>
                </div>
                <div class="kpi-box">
                    <div class="kpi-label">Max Pain</div>
                    <div class="kpi-val nt">{mp:,.0f}</div>
                    <div class="kpi-sub">Expiry gravity</div>
                </div>
                <div class="kpi-box">
                    <div class="kpi-label">Gamma Wall</div>
                    <div class="kpi-val" style="color:#f57f17">{gw:,.0f}</div>
                    <div class="kpi-sub">{br.get('gamma_wall_strength','')}</div>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    with st.expander("🏦 BankNifty Divergence + PCR Comparison", expanded=False):
        render_divergence_pcr(live, pcr_data)

    # SECTION 3 — TECHNICAL INDICATORS
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("### 📈 Technical Indicators")
    t1, t2, t3, t4 = st.columns(4)

    iv_skew = ind.get("iv_skew", 0)
    skew_cls = "dn" if iv_skew > 0 else "up" if iv_skew < 0 else "nt"
    skew_lbl = "Bearish skew" if iv_skew > 0 else "Bullish skew" if iv_skew < 0 else "Neutral"

    oi_mom = ind.get("oi_momentum","NEUTRAL")
    mom_cls = "up" if oi_mom=="BULLISH" else "dn" if oi_mom=="BEARISH" else "nt"

    unusual = ind.get("unusual_activity", [])
    ua_count = len(unusual)

    net_delta = ind.get("net_delta", 0)
    nd_cls = "up" if net_delta > 0 else "dn" if net_delta < 0 else "nt"

    for col, (label, val, sub, cls) in zip([t1,t2,t3,t4], [
        ("IV Skew (P-C)", f'{iv_skew:+.2f}', skew_lbl, skew_cls),
        ("OI Momentum", oi_mom, f"Call OI conc: {ind.get('call_oi_concentration',0):.0f}%", mom_cls),
        ("Unusual Activity", f"{ua_count} strikes", "High vol/OI ratio detected", "up" if ua_count > 0 else "nt"),
        ("Net Delta", f"{net_delta:+.1f}", "Directional bias", nd_cls),
    ]):
        with col:
            st.markdown(f"""<div class="metric-card" style="--accent:#aa44ff">
                <div class="metric-label">{label}</div>
                <div class="metric-value {cls}" style="font-size:1.3rem">{val}</div>
                <div class="metric-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    # Unusual activity detail
    if unusual:
        with st.expander(f"⚡ Unusual Activity Detail ({ua_count} strikes)", expanded=False):
            rows = [{"Strike": f"{u['strike']:,}", "Type": u['type'],
                     "Vol/OI Ratio": u['ratio'], "Volume": f"{u['volume']:,}"} for u in unusual]
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 4 — OI BUILD-UP + SMART MONEY (compact)
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("### 🔍 OI Build-Up & Smart Money")
    ob1, ob2 = st.columns(2)

    with ob1:
        lb = len(buildup.get("long_buildup",[]))
        sb = len(buildup.get("short_buildup",[]))
        sc = len(buildup.get("short_covering",[]))
        lu = len(buildup.get("long_unwinding",[]))
        dom = max({"LONG BUILD (🟢 Bullish)": lb+sc, "SHORT BUILD (🔴 Bearish)": sb+lu}, key=lambda k: {"LONG BUILD (🟢 Bullish)": lb+sc, "SHORT BUILD (🔴 Bearish)": sb+lu}[k])
        st.markdown(f"""<div class="section-card">
            <div class="section-title">OI Build-Up Patterns</div>
            <div class="kpi-row">
                <div class="kpi-box"><div class="kpi-label">Long Buildup</div>
                    <div class="kpi-val up">{lb}</div><div class="kpi-sub">OI↑ Price↑</div></div>
                <div class="kpi-box"><div class="kpi-label">Short Buildup</div>
                    <div class="kpi-val dn">{sb}</div><div class="kpi-sub">OI↑ Price↓</div></div>
                <div class="kpi-box"><div class="kpi-label">Covering</div>
                    <div class="kpi-val up">{sc}</div><div class="kpi-sub">OI↓ Price↑</div></div>
                <div class="kpi-box"><div class="kpi-label">Unwinding</div>
                    <div class="kpi-val dn">{lu}</div><div class="kpi-sub">OI↓ Price↓</div></div>
            </div>
            <div style="text-align:center;margin-top:0.6rem;font-size:0.78rem;color:#546e7a">
                Dominant: <strong>{dom}</strong>
            </div>
        </div>""", unsafe_allow_html=True)

    with ob2:
        cs = smart.get("call_signal","")
        ps = smart.get("put_signal","")
        ca = smart.get("call_accumulation",[])[:3]
        pa = smart.get("put_accumulation",[])[:3]
        call_rows = "".join([f"<div class='factor-pill'>CE {r['strike']:,} Z:{r['z_score']:.1f}</div>" for r in ca])
        put_rows  = "".join([f"<div class='factor-pill'>PE {r['strike']:,} Z:{r['z_score']:.1f}</div>" for r in pa])
        st.markdown(f"""<div class="section-card">
            <div class="section-title">💰 Smart Money Tracking</div>
            <div style="margin-bottom:0.5rem">
                <span style="color:#1565c0;font-size:0.78rem;font-weight:700">CALL: {cs}</span>
                <div>{call_rows or '<span style="color:#78909c;font-size:0.75rem">No unusual call activity</span>'}</div>
            </div>
            <div>
                <span style="color:#ff8800;font-size:0.78rem;font-weight:700">PUT: {ps}</span>
                <div>{put_rows or '<span style="color:#78909c;font-size:0.75rem">No unusual put activity</span>'}</div>
            </div>
        </div>""", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    with st.expander("⚡ OI Change Velocity — Live Writing & Covering Detection", expanded=False):
        _vel = compute_oi_velocity(df_calls, df_puts, spot)
        render_oi_velocity(_vel)

    # SECTION 5 — OPTION CHAIN (Minimized / Collapsible)
    # ═══════════════════════════════════════════════════════════════════════
    with st.expander(f"📋 Option Chain — {sym} {exp} (click to expand)", expanded=False):
        _render_compact_chain(df_calls, df_puts, atm, spot)

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 6 — MARKET NEWS
    # ═══════════════════════════════════════════════════════════════════════
    # Raw data debug — shows actual field names from Paytm Money
    with st.expander("🔬 Raw Data Inspector (Debug)", expanded=False):
        st.caption("Shows actual field names returned by Paytm Money API — useful for fixing parsing issues")
        if not df_calls.empty:
            st.markdown("**Sample CALL record (first row):**")
            sample = data.get("calls", [{}])[0]
            st.json(sample)
            st.markdown(f"**Field names:** `{list(sample.keys())}`")
        if not df_puts.empty:
            st.markdown("**Sample PUT record (first row):**")
            sample2 = data.get("puts", [{}])[0]
            st.json(sample2)
        st.markdown(f"**Total CALL records:** {len(data.get('calls',[]))} | **PUT records:** {len(data.get('puts',[]))}")

    with st.expander("📰 Market News & Sentiment", expanded=False):
        if news:
            for item in news[:6]:
                sent_color = "#2e7d32" if item.get("sentiment")=="BULLISH" else \
                             "#c62828" if item.get("sentiment")=="BEARISH" else "#8888cc"
                title = item.get("title","")
                source = item.get("source","")
                pub = item.get("published","")
                url = item.get("url","")
                link = f'<a href="{url}" target="_blank" style="color:#1565c0">{title}</a>' if url else title
                st.markdown(f"""<div class="news-item">
                    <div style="font-size:0.85rem;color:#2c3e50">{link}</div>
                    <div style="font-size:0.7rem;color:#78909c;margin-top:0.2rem">
                        {source} · {pub} ·
                        <span style="color:{sent_color};font-weight:700">{item.get('sentiment','')}</span>
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("No news available. Configure a news API for live headlines.")

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 7 — SENTIMENT SIGNALS BREAKDOWN
    # ═══════════════════════════════════════════════════════════════════════
    with st.expander("🔬 Sentiment Signal Breakdown", expanded=False):
        for icon, msg, pts in sent.get("signals", []):
            color = "#1b5e20" if pts > 0 else "#c62828" if pts < 0 else "#5c6bc0"
            sbg = "#e8f5e9" if pts > 0 else "#fce8e8" if pts < 0 else "#f3f4fb"
            st.markdown(f"""<div style="background:#ffffff;border:1px solid #e0e4ec;
                border-radius:6px;padding:0.5rem 0.9rem;margin:0.3rem 0;
                display:flex;justify-content:space-between;align-items:center">
                <span style="font-size:0.82rem;color:#546e7a">{icon} {msg}</span>
                <span style="font-family:'JetBrains Mono',monospace;color:{color};
                      font-weight:700;font-size:0.85rem">{pts:+d}</span>
            </div>""", unsafe_allow_html=True)

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 8 — MARKET INTELLIGENCE
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown("### 🌐 Market Intelligence")
    if st.session_state.get("intelligence_loaded"):
        try:
            _render_intelligence(
                st.session_state.get("fii_dii") or {},
                st.session_state.get("global_cues") or {"data": {}},
                st.session_state.get("india_vix") or {},
                st.session_state.get("mkt_breadth") or {},
                st.session_state.get("news_items") or [],
            )
        except Exception as e:
            st.warning(f"Intelligence display error: {e}")
    else:
        st.markdown('''<div style="background:#e8eaf6;border:1px solid #c5cae9;border-left:4px solid #3949ab;border-radius:8px;padding:1rem 1.2rem;margin-bottom:.5rem">
<div style="font-weight:600;color:#1a237e;margin-bottom:6px">🌐 Market Intelligence — FII/DII · GIFT Nifty · VIX · Global Cues · News</div>
<div style="font-size:.85rem;color:#3949ab">Click <strong>🌐 Load Market Intelligence</strong> in the sidebar to activate.</div>
</div>''', unsafe_allow_html=True)
    st.divider()
    # ═══════════════════════════════════════════════════════════════════════
    # SECTION 9 — AI TRADE DECISION ENGINE
    # Active profile badge
    active_p = get_profile(st.session_state.risk_profile)
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem">
        <h2 style="margin:0">🤖 AI Trade Decision Engine</h2>
        <div style="background:#ffffff;border:1px solid {active_p['color']};
                    border-radius:8px;padding:0.3rem 0.8rem;font-size:0.8rem;
                    color:{active_p['color']};font-weight:700">
            {active_p['label']} Profile Active
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("""<div class="section-card">
        <div class="section-title">Powered by Claude AI — Analyzes all market factors simultaneously</div>
    </div>""", unsafe_allow_html=True)

    ai_c1, ai_c2 = st.columns([2, 1])
    with ai_c2:
        st.markdown("**Optional context for AI:**")
        user_note = st.text_area(
            "Additional notes / your view",
            value=st.session_state.user_note,
            placeholder="e.g. 'RBI policy tomorrow', 'Expecting breakout above 22500', 'I prefer OTM options'...",
            height=100, label_visibility="collapsed"
        )
        st.session_state.user_note = user_note

    with ai_c1:
        st.markdown("**What AI will analyze:**")
        st.markdown("""<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.8rem">
            <span class="factor-pill">📍 Live Price + OHLC</span>
            <span class="factor-pill">⚖️ PCR Analysis</span>
            <span class="factor-pill">🎯 Support & Resistance</span>
            <span class="factor-pill">📊 OI Build-Up</span>
            <span class="factor-pill">💰 Smart Money</span>
            <span class="factor-pill">📈 IV Skew</span>
            <span class="factor-pill">🔒 Max Pain</span>
            <span class="factor-pill">⚡ Gamma Wall</span>
            <span class="factor-pill">🧠 Sentiment Score</span>
            <span class="factor-pill">📉 Market Breadth</span>
            <span class="factor-pill">🔬 Unusual Activity</span>
            <span class="factor-pill">📰 News Sentiment</span>
            <span class="factor-pill">⚡ OI Velocity</span>
            <span class="factor-pill">⏱️ Entry Timing</span>
            <span class="factor-pill">🏦 BankNifty Div.</span>
            <span class="factor-pill">💰 FII/DII Flow</span>
            <span class="factor-pill">🌍 GIFT Nifty</span>
        </div>""", unsafe_allow_html=True)

    # AI Trigger Button
    col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
    with col_btn2:
        profile_label = get_profile(st.session_state.risk_profile)["label"]

        # ── Time-to-close banner — tells user what kind of trade AI will suggest
        _btn_tc = _compute_ai_time_context(mkt, st.session_state.get("selected_expiry"))
        _mtc = _btn_tc["minutes_to_close"]
        if _btn_tc["session_phase"] == "CLOSED":
            st.info("🌙 Market closed — AI will suggest a **PRE-MARKET PLAN** for next session", icon="🌙")
        elif _mtc <= 15:
            st.error(
                f"🔴 **CRITICAL: only {_mtc} min to close** — AI will force scalp mode "
                f"(single fast target, book full at T1). Consider waiting for next session if no clean setup.",
                icon="⏰",
            )
        elif _mtc <= 30:
            st.warning(
                f"🟡 **{_mtc} min to close** — AI will use tight targets (capped at ~20-35% gain) "
                f"that must complete before 15:30 IST.",
                icon="⏰",
            )
        elif _mtc <= 60:
            st.info(
                f"🟢 **{_mtc} min to close** — AI targets will be capped to fit before 15:30 IST.",
                icon="⏰",
            )
        if _btn_tc["is_expiry_day"]:
            st.warning(
                "🔥 **EXPIRY DAY** — Theta decay is brutal. "
                "AI will prefer ATM/ITM and very short timeframes.",
                icon="🔥",
            )

        # ── Pause Refresh toggle right above the AI button ─────────────────
        # Gives the user a reliable way to stop auto-refresh so they can
        # comfortably click the AI button without widgets being disabled
        # mid-rerun.
        _paused = st.session_state.get("_refresh_paused", False)
        pause_cols = st.columns([1, 1])
        with pause_cols[0]:
            if not _paused and st.session_state.get("auto_refresh"):
                if st.button("⏸ Pause Refresh", use_container_width=True,
                             key="pause_refresh_btn",
                             help="Temporarily pause auto-refresh so you can click AI without interruption"):
                    st.session_state._refresh_paused = True
                    st.rerun()
            elif _paused:
                if st.button("▶ Resume Refresh", use_container_width=True,
                             key="resume_refresh_btn", type="secondary"):
                    st.session_state._refresh_paused = False
                    st.rerun()
        with pause_cols[1]:
            if _paused:
                st.caption("⏸️ **Auto-refresh paused** — click AI safely now")
            elif st.session_state.get("auto_refresh"):
                st.caption(f"🔄 Refreshing every {st.session_state.refresh_interval}s")

        # Callback fires BEFORE the rerun — this guarantees ai_loading is set
        # in session state before the next auto-refresh tick can read it.
        def _on_ai_click():
            st.session_state.ai_loading = True
            st.session_state._ai_click_pending = True  # processed this rerun
            st.session_state._refresh_paused = True    # belt-and-suspenders

        # Disable the button while AI is already running — prevents double-clicks
        _ai_running = st.session_state.get("ai_loading", False)

        st.button(
            f"🧠 Get AI Recommendation ({profile_label})"
            if not _ai_running else "🧠 AI is analyzing... please wait",
            use_container_width=True,
            type="primary",
            key="ai_btn",
            on_click=_on_ai_click,
            disabled=_ai_running,
        )

        if st.session_state.get("_ai_click_pending"):
            st.session_state._ai_click_pending = False  # consume the flag
            try:
                # Build context
                context = build_market_context_with_candles(
                    symbol=sym, expiry=exp, live_price=live,
                    snapshot=snapshot, pcr_data=pcr_data,
                    support=sup, resistance=res,
                    buildup=buildup, smart_money=smart,
                    indicators=ind, breadth=br, sentiment=sent,
                    top_calls=top_calls, top_puts=top_puts,
                    candle_context=candle_summary,
                    breakout_alerts=breakout_alerts,
                    profile_name=st.session_state.risk_profile,
                )

                # ── Append extra feature data to AI context ──────────────────────
                _extra = []
                try:
                    _v = compute_oi_velocity(df_calls, df_puts, spot)
                    _cs = "; ".join([f"{s['strike']:.0f}CE {s['oi_chg']:+.0f}%" for s in _v.get("call_surges",[])[:3]])
                    _ps = "; ".join([f"{s['strike']:.0f}PE {s['oi_chg']:+.0f}%" for s in _v.get("put_surges",[])[:3]])
                    _extra.append(f"\n━━━ OI CHANGE VELOCITY ━━━\nBias:{_v.get('bias')}\n" + "; ".join(_v.get("signals",[])) + f"\nCALL:{_cs}\nPUT:{_ps}")
                except Exception:
                    pass
                try:
                    _ai_a = st.session_state.ai_result.get("action","BUY CALL") if st.session_state.ai_result else "BUY CALL"
                    _t2 = compute_entry_timing_score(live, df_calls, df_puts, candles_5m, sent, _ai_a)
                    _extra.append(f"\n━━━ ENTRY TIMING SCORE ━━━\nScore:{_t2['score']}/100 Grade:{_t2['grade']} Action:{'ENTER NOW' if not _t2['wait'] else 'WAIT'}\n" + " | ".join(_t2["signals"][:4]))
                except Exception:
                    pass
                try:
                    from features import fetch_banknifty_spot, compute_divergence
                    _bn = st.session_state.get("banknifty_spot", 0) or fetch_banknifty_spot()
                    if _bn > 0:
                        st.session_state["banknifty_spot"] = _bn
                    _d = compute_divergence(float(live.get("change_pct", 0) or 0), _bn)
                    if _d.get("available"):
                        _extra.append(f"\n━━━ BANKNIFTY DIVERGENCE ━━━\n₹{_bn:,.0f} {_d['signal']}\n{_d['description']}")
                except Exception:
                    pass
                if st.session_state.get("intelligence_loaded"):
                    try:
                        from market_intelligence import build_intelligence_summary
                        _extra.append("\n" + build_intelligence_summary(
                            st.session_state.get("fii_dii") or {},
                            st.session_state.get("global_cues") or {"data": {}},
                            st.session_state.get("india_vix") or {},
                            st.session_state.get("mkt_breadth") or {},
                            st.session_state.get("news_items") or [],
                        ))
                    except Exception:
                        pass
                if _extra:
                    context += "\n" + "\n".join(_extra)
                # Add market context note WITH time-to-close and VWAP/candle stats
                _tc = _compute_ai_time_context(mkt, st.session_state.get("selected_expiry"))
                _vs = _compute_vwap_and_stats()

                mkt_note = (
                    f"Market Status: {mkt['status']} as of {mkt['current_ist']}. "
                    f"Minutes until 15:30 IST close: {_tc['minutes_to_close']} "
                    f"({'MARKET CLOSED — pre-market plan only' if not _tc['is_open'] else 'session active'}). "
                    f"Session phase: {_tc['session_phase']}. "
                    f"Max holding allowed: {_tc['max_hold_minutes']} minutes.\n"
                )
                if not mkt['is_open']:
                    mkt_note += f"Data is from last trading session ({mkt['last_trading_day']}). "
                    mkt_note += "DO NOT suggest intraday targets — this is pre-market planning only.\n"
                if _tc['is_expiry_day']:
                    mkt_note += "EXPIRY DAY: Today is expiry — extreme theta decay on OTM options.\n"

                # VWAP + candle stats — hard numbers for target calibration
                vwap_note = ""
                if _vs["vwap"] > 0:
                    vwap_note += (
                        f"\nINTRADAY PRICE STATS (for target calibration):\n"
                        f"VWAP:                  ₹{_vs['vwap']:,.2f}\n"
                        f"Price vs VWAP:         {_vs['price_vs_vwap']}\n"
                        f"Session High/Low:      ₹{_vs['session_high']:,.0f} / ₹{_vs['session_low']:,.0f}\n"
                        f"Opening Range High:    ₹{_vs['opening_range_high']:,.0f}\n"
                        f"Opening Range Low:     ₹{_vs['opening_range_low']:,.0f}\n"
                        f"Avg 5-min candle range (last 8 candles): {_vs['avg_range_5m']:.1f} pts\n"
                        f"Avg 1-min candle range (last 8 candles): {_vs['avg_range_1m']:.1f} pts\n"
                        f"Typical 30-min range at this session phase: {_tc['typical_30min_pts']} pts\n"
                        f"Typical 15-min range at this session phase: {_tc['typical_15min_pts']} pts\n"
                        f"\n⚡ TARGET CALIBRATION RULE: "
                        f"T1 index move MUST be ≤ {_tc['typical_15min_pts']} pts "
                        f"(15-min range at {_tc['session_phase']}). "
                        f"T2 index move MUST be ≤ {_tc['typical_30min_pts']} pts "
                        f"(30-min range). Both targets MUST complete before 15:30 IST. "
                        f"Recent 5-min avg range is {_vs['avg_range_5m']:.1f} pts — "
                        f"if T1 requires more than 2× this, T1 is unrealistic.\n"
                    )

                full_note = mkt_note + vwap_note + ("\n" + user_note if user_note else "")

                with st.spinner(f"🧠 AI analyzing... ({_tc['minutes_to_close']} min to close)"):
                    result = get_ai_analysis(
                        context,
                        full_note,
                        profile_name=st.session_state.risk_profile,
                        time_context=_tc,
                    )
                result = normalize_trade_recommendation(
                    result,
                    market_inputs={
                        "symbol": sym,
                        "live_price": live,
                        "snapshot": snapshot,
                        "pcr_data": pcr_data,
                        "support": sup,
                        "resistance": res,
                        "intraday_levels": intraday_lvls,
                        "buildup": buildup,
                        "smart_money": smart,
                        "indicators": ind,
                        "sentiment": sent,
                        "calls_data": data.get("calls", []),
                        "puts_data": data.get("puts", []),
                    },
                    profile_name=st.session_state.risk_profile,
                )
                # Only replace the existing ai_result if the new one looks
                # valid — this prevents a transient error from wiping a
                # working pinned recommendation.
                if isinstance(result, dict) and "error" not in result:
                    st.session_state.ai_result = result
                    st.session_state.ai_result_timestamp = datetime.now()
                    st.session_state._last_ai_error = None   # clear stale errors
                elif isinstance(result, dict) and "error" in result:
                    # Persist the error across rerun so the user actually sees it.
                    # The rerun below would wipe any st.error() called here.
                    st.session_state._last_ai_error = {
                        "message":      result.get("error", "Unknown error"),
                        "status_code":  result.get("status_code"),
                        "raw_response": result.get("raw_response", ""),
                        "traceback":    result.get("traceback", ""),
                        "at":           datetime.now(),
                    }
            except Exception as _ai_e:
                import traceback as _tb
                st.session_state._last_ai_error = {
                    "message":   f"AI call exception: {type(_ai_e).__name__}: {_ai_e}",
                    "traceback": _tb.format_exc()[:1000],
                    "at":        datetime.now(),
                }
            finally:
                st.session_state.ai_loading = False
                # Auto-resume refresh if it was paused by the AI click flow.
                st.session_state._refresh_paused = False
            st.rerun()

    # ── AI Error Display (persists across rerun) ─────────────────────────
    _ai_err = st.session_state.get("_last_ai_error")
    if _ai_err:
        err_age = (datetime.now() - _ai_err["at"]).total_seconds()
        # Auto-clear errors older than 5 minutes so they don't haunt the UI
        if err_age > 300:
            st.session_state._last_ai_error = None
        else:
            status = _ai_err.get("status_code")
            status_badge = f" (HTTP {status})" if status else ""
            st.error(
                f"❌ **AI Error{status_badge}:** {_ai_err['message']}  \n"
                f"_Previous pinned recommendation (if any) has been kept._",
                icon="🔧",
            )
            # Expandable debug info for diagnosing API issues
            if _ai_err.get("raw_response") or _ai_err.get("traceback"):
                with st.expander("🔍 Debug details (click to expand)"):
                    if _ai_err.get("raw_response"):
                        st.caption("Raw API response:")
                        st.code(_ai_err["raw_response"], language="json")
                    if _ai_err.get("traceback"):
                        st.caption("Exception traceback:")
                        st.code(_ai_err["traceback"], language="python")
            col_dismiss = st.columns([4, 1])[1]
            with col_dismiss:
                if st.button("✖ Dismiss", key="dismiss_ai_err", use_container_width=True):
                    st.session_state._last_ai_error = None
                    st.rerun()

    # ── AI Result Display ─────────────────────────────────────────────────
    if st.session_state.ai_result:
        _render_ai_result(st.session_state.ai_result, sym, atm)

    # ── WebSocket auto-update price display ──────────────────────────────
    if mkt.get("is_open"):
        st.caption("🔴 Live · Auto-refreshes every interval | Click 🔄 Refresh All for instant update")

    # ── Auto-refresh status strip (timers are registered at top of function) ──
    if st.session_state.auto_refresh:
        slow_interval_s = max(2, int(st.session_state.refresh_interval))
        ws_live = st.session_state.get("ws_enabled") and get_feed().is_connected
        ws_status = "🟢 WS live (1s price ticks)" if ws_live else "🟡 REST only"
        last_fetch = st.session_state.get("_last_auto_fetch_ts")
        age_str = ""
        if last_fetch:
            _age = int((datetime.now() - last_fetch).total_seconds())
            age_str = f" · last full refresh {_age}s ago"
        st.caption(
            f"⚡ Auto-refresh ON · {ws_status} · full data every {slow_interval_s}s{age_str} "
            f"| AI recommendation is preserved across refreshes"
        )


def _render_compact_chain(df_calls, df_puts, atm, spot):
    """Render a compact ATM-centered option chain."""
    if df_calls.empty and df_puts.empty:
        st.warning("No data"); return

    try:
        # Show only ±10 strikes from ATM
        if not df_calls.empty:
            all_strikes = sorted(df_calls["strike_price"].unique())
            atm_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - atm))
            visible = set(all_strikes[max(0, atm_idx-8): atm_idx+9])
            df_c = df_calls[df_calls["strike_price"].isin(visible)].copy()
        else:
            df_c = df_calls.copy()

        if not df_puts.empty:
            df_p = df_puts[df_puts["strike_price"].isin(visible)].copy() if not df_calls.empty else df_puts.copy()
        else:
            df_p = df_puts.copy()

        if not df_c.empty and not df_p.empty:
            merged = pd.merge(
                df_c[["strike_price","oi","volume","price","oi_perc_chg"]].rename(
                    columns={"oi":"C_OI","volume":"C_Vol","price":"C_LTP","oi_perc_chg":"C_OI%"}),
                df_p[["strike_price","oi","volume","price","oi_perc_chg"]].rename(
                    columns={"oi":"P_OI","volume":"P_Vol","price":"P_LTP","oi_perc_chg":"P_OI%"}),
                on="strike_price", how="outer"
            ).sort_values("strike_price", ascending=False).fillna(0)

            merged["ATM"] = merged["strike_price"].apply(lambda x: "◀ ATM" if x == atm else "")

            for col in ["C_OI","C_Vol","P_OI","P_Vol"]:
                merged[col] = merged[col].astype(int).apply(fmt_lakh)
            for col in ["C_LTP","P_LTP"]:
                merged[col] = merged[col].apply(lambda x: f"{float(x):.2f}")
            for col in ["C_OI%","P_OI%"]:
                merged[col] = merged[col].apply(lambda x: f"{float(x):+.1f}%")
            merged["Strike"] = merged["strike_price"].astype(int)
            merged = merged.drop("strike_price", axis=1)

            display_cols = ["C_OI","C_OI%","C_Vol","C_LTP","Strike","ATM","P_LTP","P_Vol","P_OI%","P_OI"]
            display_cols = [c for c in display_cols if c in merged.columns]
            st.dataframe(merged[display_cols], hide_index=True, use_container_width=True, height=380)
    except Exception as e:
        st.error(f"Chain render error: {e}")


def _render_ai_result(result: dict, symbol: str, atm: float):
    """Render AI recommendation panel — all variables defined locally."""
    if "error" in result:
        st.error(f"❌ AI Error: {result['error']}")
        if "raw_response" in result:
            with st.expander("Raw response"):
                st.code(result["raw_response"])
        return

    def ss(val, default="—"):
        if val is None: return default
        s = str(val).strip()
        return s if s else default

    def sf(val, default=0.0):
        try: return float(str(val).replace(",","").replace("₹","").strip())
        except: return default

    # Extract all fields safely
    action        = ss(result.get("action"),            "NO TRADE")
    no_trade_rsn  = ss(result.get("no_trade_reason"),   "")
    conf          = max(0, min(100, int(sf(result.get("confidence"), 0))))
    win_rate      = max(0, min(100, int(sf(result.get("estimated_win_rate"), conf))))
    profile_used  = ss(result.get("risk_profile"),      st.session_state.risk_profile)
    entry_s       = sf(result.get("entry_strike"),      atm)
    entry_t       = ss(result.get("entry_type"),        "ATM")
    entry_r       = ss(result.get("entry_price_range"), "N/A")
    target        = sf(result.get("target_price"),      0)
    # Two-tier targets
    target1       = sf(result.get("target1_price"),     target)
    target2       = sf(result.get("target2_price"),     0)
    target1_time  = ss(result.get("target1_time"),      "15-25 min")
    target2_time  = ss(result.get("target2_time"),      "30-60 min")
    target1_move  = ss(result.get("target1_index_move"),"")
    target2_move  = ss(result.get("target2_index_move"),"")
    pos_mgmt      = ss(result.get("position_management"), "")
    sl            = sf(result.get("stop_loss_price"),   0)
    rr            = ss(result.get("risk_reward"),       "—")
    rr_t1         = ss(result.get("risk_reward_t1"),    "—")
    max_lots      = int(sf(result.get("max_lots"),      2))
    margin        = ss(result.get("approx_margin"),     "—")
    hold          = ss(result.get("holding_period"),    "—")
    struct        = ss(result.get("market_structure"),  "—")
    bias          = ss(result.get("bias_strength"),     "—")
    primary       = ss(result.get("primary_reason"),    "")
    summary       = ss(result.get("sentiment_summary"), "")
    trade_plan    = ss(result.get("trade_plan"),        "")
    factors       = result.get("supporting_factors",    []) or []
    risks         = result.get("key_risks",             []) or []
    avoid         = ss(result.get("avoid_if"),          "")
    timeframe     = ss(result.get("timeframe"),         "15-60 min")
    source        = ss(result.get("analysis_source"),   "ANTHROPIC")
    fallback_note = ss(result.get("fallback_reason"),   "")

    act_color = "#2e7d32" if action=="BUY CALL" else "#c62828" if action=="BUY PUT" else "#5566cc"
    act_emoji = "📈" if action=="BUY CALL" else "📉" if action=="BUY PUT" else "🚫"
    card_css  = "ai-buy-call" if action=="BUY CALL" else "ai-buy-put" if action=="BUY PUT" else "ai-no-trade"

    # Profile color
    from ai_engine import get_profile
    p = get_profile(profile_used)
    p_color = p.get("color","#2e7d32")

    st.divider()
    # Pre-market plan banner
    mkt_now = st.session_state.get("market_status", {})
    if mkt_now and not mkt_now.get("is_open", True) and action != "NO TRADE":
        st.markdown(f"""<div style="background:#fff8e1;border:1px solid #ffe082;border-left:4px solid #f57f17;border-radius:8px;padding:10px 16px;margin-bottom:12px;display:flex;align-items:center;gap:10px"><span style="font-size:1.2rem">🌙</span><div><div style="font-weight:700;color:#7f5a00">PRE-MARKET PLAN — For Next Trading Session</div><div style="font-size:.82rem;color:#9e6c00">Market is closed. Verify all cues at 9:15 AM open. Do NOT use today's expiry strikes.</div></div></div>""", unsafe_allow_html=True)

    # Stale recommendation warning — if generated > 15 min ago and market is open
    ai_ts = st.session_state.get("ai_result_timestamp")
    if ai_ts and mkt_now and mkt_now.get("is_open"):
        age_min = (datetime.now() - ai_ts).total_seconds() / 60
        if age_min > 15:
            st.warning(
                f"⚠️ **This recommendation is {int(age_min)}m old.** "
                f"Market conditions may have changed — regenerate before entering any trade.",
                icon="🕐",
            )

    # NO TRADE — show a clear explanation instead of a blank trade card
    if action == "NO TRADE":
        st.markdown(f"""
        <div style="background:#f3f4fb;border:2px solid #5566cc;border-radius:14px;
                    padding:1.5rem;text-align:center;margin-bottom:1rem">
            <div style="font-size:2rem">🚫</div>
            <div style="font-size:1.4rem;font-weight:700;color:#3949ab;margin:0.5rem 0">
                NO TRADE
            </div>
            {f'<div style="font-size:0.9rem;color:#546e7a;margin-top:0.5rem;max-width:600px;margin:auto">{no_trade_rsn}</div>' if no_trade_rsn else ''}
            <div style="font-size:0.78rem;color:#78909c;margin-top:0.8rem">
                Confidence: {conf}/100 &nbsp;|&nbsp; Timeframe: {timeframe}
            </div>
        </div>""", unsafe_allow_html=True)
        if trade_plan and trade_plan != "—":
            st.info(f"**When conditions improve:** {trade_plan}", icon="📋")
        return   # Don't render the full trade card for NO TRADE

    # AI generation timestamp + age (pinned indicator so users know the
    # recommendation survives auto-refresh).
    ai_ts = st.session_state.get("ai_result_timestamp")
    if ai_ts:
        age_s = int((datetime.now() - ai_ts).total_seconds())
        if age_s < 60:
            age_str = f"{age_s}s ago"
        elif age_s < 3600:
            age_str = f"{age_s // 60}m {age_s % 60}s ago"
        else:
            age_str = f"{age_s // 3600}h {(age_s % 3600) // 60}m ago"
        ts_str = ai_ts.strftime("%H:%M:%S")
    else:
        age_str = "just now"
        ts_str = datetime.now().strftime("%H:%M:%S")

    st.markdown(
        f"### 🤖 AI Recommendation "
        f'<span style="font-size:0.75rem;background:#e8f5e9;border:1px solid #81c784;'
        f'border-radius:6px;padding:2px 8px;color:#2e7d32;margin-left:6px">📌 Pinned</span>'
        f'<span style="font-size:0.75rem;color:#546e7a;margin-left:8px">generated {ts_str} · {age_str}</span>'
        f'<span style="font-size:0.8rem;background:#ffffff;border:1px solid {p_color};'
        f'border-radius:6px;padding:3px 10px;color:{p_color};margin-left:8px">{p["label"]} Profile</span>',
        unsafe_allow_html=True
    )

    left, right = st.columns([1, 1.5])

    # ── LEFT: Signal + Metrics ─────────────────────────────────────────────
    with left:
        st.markdown(f"""
        <div class="ai-card {card_css}">
            <div style="font-size:0.65rem;color:#78909c;letter-spacing:2px;text-transform:uppercase">AI Signal</div>
            <div class="ai-action" style="color:{act_color};margin:0.3rem 0;font-size:2rem">{act_emoji} {action}</div>
            <div style="font-size:0.7rem;color:#78909c;margin-top:0.8rem">CONFIDENCE: {conf}/100</div>
            <div style="background:#e8eaf6;border-radius:8px;height:10px;overflow:hidden;margin:0.3rem 0">
                <div style="width:{conf}%;height:100%;background:{act_color};border-radius:8px"></div>
            </div>
            <div style="font-size:0.7rem;color:#78909c;margin-top:0.5rem">
                🎯 Estimated win rate: <span style="color:#1a237e">{win_rate}%</span><br>
                ⏱️ Timeframe: <span style="color:#1a237e">{timeframe}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("**📌 Trade Setup**")

        # Entry + SL row
        c_es1, c_es2 = st.columns(2)
        with c_es1:
            st.metric("Entry Strike", f"₹{int(entry_s):,}" if entry_s > 0 else "—")
            st.metric("Entry Type", entry_t)
        with c_es2:
            st.metric("Entry Range", f"₹{entry_r}")
            st.metric("Stop Loss (LTP)", f"₹{sl:.0f}" if sl > 0 else "—")

        # ── Two-tier Target Cards ─────────────────────────────────────────
        st.markdown("**🎯 Targets (Book in Stages)**")
        if target1 > 0 or target2 > 0:
            # Compute percentage gains for display
            entry_mid = 0.0
            try:
                parts = entry_r.replace("₹", "").split("-")
                if len(parts) == 2:
                    entry_mid = (float(parts[0]) + float(parts[1])) / 2
            except Exception:
                pass
            t1_gain = ((target1 - entry_mid) / entry_mid * 100) if (entry_mid > 0 and target1 > 0) else 0
            t2_gain = ((target2 - entry_mid) / entry_mid * 100) if (entry_mid > 0 and target2 > 0) else 0

            ct1, ct2 = st.columns(2)
            with ct1:
                st.markdown(f"""
                <div style="background:linear-gradient(135deg,#e8f5e9,#fff);
                            border:2px solid #66bb6a;border-radius:10px;padding:12px 14px;
                            box-shadow:0 1px 4px rgba(0,0,0,.05)">
                    <div style="font-size:0.6rem;color:#2e7d32;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;margin-bottom:4px">🎯 Target 1 · PRIMARY</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;
                                color:#1b5e20;line-height:1.1">₹{target1:.0f}</div>
                    <div style="font-size:0.72rem;color:#546e7a;margin-top:4px">
                        {f'+{t1_gain:.0f}% gain' if t1_gain > 0 else ''}
                        {'·' if t1_gain > 0 and target1_time != '—' else ''}
                        {target1_time if target1_time != '—' else ''}
                    </div>
                    {f'<div style="font-size:0.66rem;color:#78909c;margin-top:3px;font-style:italic">{target1_move}</div>' if target1_move and target1_move != '—' else ''}
                    <div style="font-size:0.7rem;color:#2e7d32;margin-top:6px;font-weight:600">
                        ✂️ Book 50-75% here
                    </div>
                </div>""", unsafe_allow_html=True)

            with ct2:
                st.markdown(f"""
                <div style="background:linear-gradient(135deg,#fff3e0,#fff);
                            border:2px solid #ffb74d;border-radius:10px;padding:12px 14px;
                            box-shadow:0 1px 4px rgba(0,0,0,.05)">
                    <div style="font-size:0.6rem;color:#e65100;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;margin-bottom:4px">🚀 Target 2 · STRETCH</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;
                                color:#bf360c;line-height:1.1">{'₹' + f'{target2:.0f}' if target2 > 0 else '—'}</div>
                    <div style="font-size:0.72rem;color:#546e7a;margin-top:4px">
                        {f'+{t2_gain:.0f}% gain' if t2_gain > 0 else ''}
                        {'·' if t2_gain > 0 and target2_time != '—' else ''}
                        {target2_time if target2_time != '—' else ''}
                    </div>
                    {f'<div style="font-size:0.66rem;color:#78909c;margin-top:3px;font-style:italic">{target2_move}</div>' if target2_move and target2_move != '—' else ''}
                    <div style="font-size:0.7rem;color:#e65100;margin-top:6px;font-weight:600">
                        💨 Trail rest to here
                    </div>
                </div>""", unsafe_allow_html=True)

            # Position management callout
            if pos_mgmt and pos_mgmt != "—":
                st.markdown(f"""
                <div style="background:#f0f4ff;border:1px solid #c5cae9;border-left:4px solid #3949ab;
                            border-radius:0 8px 8px 0;padding:10px 14px;margin-top:10px">
                    <div style="font-size:0.62rem;color:#1a237e;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;margin-bottom:4px">📋 Position Management</div>
                    <div style="font-size:0.82rem;color:#283593;line-height:1.5">{pos_mgmt}</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Target data not available in this recommendation.")

        # Trade summary row
        c3, c4, c5 = st.columns(3)
        with c3: st.metric("Risk:Reward (T2)", rr)
        with c4: st.metric("R:R @ T1", rr_t1 if rr_t1 != "—" else "—")
        with c5: st.metric("Hold", hold.replace("Intraday","Intrad.") if hold else "—")

        c6, c7, c8 = st.columns(3)
        with c6: st.metric("Max Lots", f"{max_lots} lots")
        with c7: st.metric("Capital Needed", margin)
        with c8: st.metric("Win Rate", f"{win_rate}%")

        st.caption(f"Structure: **{struct}** | Bias: **{bias}** | Profile: **{profile_used}** | Source: **{source}**")

    # ── RIGHT: Analysis ────────────────────────────────────────────────────
    with right:
        st.markdown("**🧠 AI Analysis**")

        st.markdown(f"""
        <div style="background:#f0f4ff;border:1px solid #c5cae9;border-left:4px solid {act_color};
                    padding:12px 14px;border-radius:0 8px 8px 0;margin-bottom:10px">
            <div style="font-size:0.62rem;color:#78909c;font-weight:600;text-transform:uppercase;margin-bottom:4px">PRIMARY REASON</div>
            <div style="font-size:0.88rem;color:#1a237e;line-height:1.5">{primary}</div>
        </div>
        <div style="font-size:0.82rem;color:#546e7a;margin-bottom:0.6rem">{summary}</div>
        """, unsafe_allow_html=True)

        if fallback_note and fallback_note != "—":
            st.markdown(
                f'<div style="background:#ffffff;border-left:3px solid #00d4ff;'
                f'padding:0.55rem 0.9rem;border-radius:0 8px 8px 0;margin-bottom:0.6rem;'
                f'font-size:0.79rem;color:#9bc7e8"><strong>Fallback logic:</strong> {fallback_note}</div>',
                unsafe_allow_html=True,
            )

        if trade_plan and trade_plan != "—":
            st.markdown(f"""
            <div style="background:#f8f9ff;border-left:3px solid #ffcc00;
                        padding:0.6rem 0.9rem;border-radius:0 8px 8px 0;margin-bottom:0.6rem">
                <div style="font-size:0.65rem;color:#78909c;margin-bottom:0.2rem">📋 TRADE PLAN</div>
                <div style="font-size:0.82rem;color:#2c3e50">{trade_plan}</div>
            </div>
            """, unsafe_allow_html=True)

        if factors:
            st.markdown("**✅ Supporting Factors:**")
            for f in (factors if isinstance(factors, list) else [str(factors)]):
                st.markdown(
                    f'<div style="background:#e8f5e9;border:1px solid #0a3a1a;border-radius:6px;'
                    f'padding:0.35rem 0.7rem;margin:0.2rem 0;font-size:0.79rem;color:#546e7a">✅ {ss(f)}</div>',
                    unsafe_allow_html=True)

        if risks:
            st.markdown("**⚠️ Key Risks:**")
            for r in (risks if isinstance(risks, list) else [str(risks)]):
                st.markdown(
                    f'<div style="background:#fce8e8;border:1px solid #3a0a0a;border-radius:6px;'
                    f'padding:0.35rem 0.7rem;margin:0.2rem 0;font-size:0.79rem;color:#cc8888">⚠️ {ss(r)}</div>',
                    unsafe_allow_html=True)

        if avoid and avoid != "—":
            st.markdown(
                f'<div style="background:#1a1400;border:1px solid #3a2a00;border-radius:6px;'
                f'padding:0.5rem 0.9rem;margin-top:0.5rem;font-size:0.79rem;color:#ccaa44">'
                f'🚫 <strong>Avoid if:</strong> {avoid}</div>',
                unsafe_allow_html=True)

        st.caption("⚠️ Educational only. Always do your own research before trading.")

    with st.expander("🔍 View Raw AI Response", expanded=False):
        st.json({k: v for k, v in result.items() if k != "raw_context"})


# ── Router ────────────────────────────────────────────────────────────────────
render_sidebar()
step = st.session_state.step

if st.session_state.authenticated and step == "dashboard":
    page_dashboard()
elif step == "exchange":
    page_exchange()
else:
    page_credentials()


def _render_intraday_levels(sup, res, spot, intraday):
    """Render intraday-relevant support/resistance levels with trading range."""
    imm_sup  = intraday.get("immediate_support", 0)
    imm_res  = intraday.get("immediate_resistance", 0)
    str_sup  = intraday.get("strong_support", 0)
    str_res  = intraday.get("strong_resistance", 0)
    rng_lo   = intraday.get("trading_range_low", 0)
    rng_hi   = intraday.get("trading_range_high", 0)
    rng_w    = intraday.get("range_width", 0)

    # Row 1: Immediate levels (15-30 min)
    st.markdown("**⚡ Immediate Levels (15-30 min range)**")
    c1, c2, c3, c4, c5 = st.columns(5)
    cards = [
        ("Imm. Support",     f"{imm_sup:,.0f}"  if imm_sup  else "—", "#2e7d32", "Nearest PUT wall"),
        ("Strong Support",   f"{str_sup:,.0f}"  if str_sup  else "—", "#69f0ae", "60-min PUT floor"),
        ("Spot",             f"{spot:,.2f}",                           "#00d4ff", "Current price"),
        ("Strong Resist.",   f"{str_res:,.0f}"  if str_res  else "—", "#ff7043", "60-min CALL cap"),
        ("Imm. Resistance",  f"{imm_res:,.0f}"  if imm_res  else "—", "#c62828", "Nearest CALL wall"),
    ]
    for col, (lbl, val, color, sub) in zip([c1,c2,c3,c4,c5], cards):
        with col:
            st.markdown(f"""
            <div style="background:#ffffff;border:1px solid #e0e4ec;border-radius:8px;
                        padding:0.7rem;text-align:center;border-top:2px solid {color}">
                <div style="font-size:0.62rem;color:#78909c;text-transform:uppercase;
                            letter-spacing:1px">{lbl}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:1.2rem;
                            font-weight:700;color:{color}">{val}</div>
                <div style="font-size:0.65rem;color:#4a7aaa">{sub}</div>
            </div>""", unsafe_allow_html=True)

    # Row 2: Trading range bar
    if rng_lo > 0 and rng_hi > 0 and spot > 0 and rng_hi != rng_lo:
        spot_pct = max(0, min(100, ((spot - rng_lo) / (rng_hi - rng_lo)) * 100))
        pts_to_res = rng_hi - spot
        pts_to_sup = spot - rng_lo
        st.markdown(f"""
        <div style="background:#ffffff;border:1px solid #e0e4ec;border-radius:8px;
                    padding:0.8rem 1.2rem;margin-top:0.6rem">
            <div style="display:flex;justify-content:space-between;
                        font-size:0.7rem;color:#78909c;margin-bottom:0.4rem">
                <span>🟢 Support {rng_lo:,.0f} ({pts_to_sup:.0f} pts away)</span>
                <span style="color:#1565c0">Spot @ {spot_pct:.0f}% | Range: {rng_w:.0f} pts</span>
                <span>🔴 Resistance {rng_hi:,.0f} ({pts_to_res:.0f} pts away)</span>
            </div>
            <div style="background:#e8eaf6;border-radius:6px;height:12px;
                        position:relative;overflow:visible">
                <div style="position:absolute;left:0;width:{spot_pct}%;
                            background:linear-gradient(90deg,#2e7d32,#00d4ff);
                            height:100%;border-radius:6px"></div>
                <div style="position:absolute;left:calc({spot_pct}% - 6px);top:-3px;
                            width:18px;height:18px;background:#00d4ff;
                            border-radius:50%;border:2px solid #f0f2f6"></div>
            </div>
            <div style="font-size:0.68rem;color:#78909c;margin-top:0.4rem;text-align:center">
                📊 This is your intraday trading range — support & resistance within 300 pts of spot
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Row 3: Global OI walls (informational only)
    with st.expander("📊 Global OI Walls (beyond intraday range — for context only)", expanded=False):
        gc1, gc2 = st.columns(2)
        with gc1:
            st.metric("Global Support (Max PUT OI)", f"₹{sup.get('strike',0):,.0f}",
                      f"OI: {sup.get('oi',0):,}")
        with gc2:
            st.metric("Global Resistance (Max CALL OI)", f"₹{res.get('strike',0):,.0f}",
                      f"OI: {res.get('oi',0):,}")
        st.caption("These are far OI walls — relevant for swing trades, not 15-60 min intraday.")
