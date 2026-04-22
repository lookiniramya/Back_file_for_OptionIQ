"""
ai_engine.py — AI Analysis Engine for OptionsIQ
Supports three risk profiles: CONSERVATIVE, MODERATE, AGGRESSIVE
Each profile changes strike selection, R:R requirements, confidence thresholds,
position sizing, holding period, and SL/target calculations.
"""

import requests
import json
from typing import Dict, Any, List
import datetime

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# ─── Risk Profile Definitions ─────────────────────────────────────────────────
RISK_PROFILES = {
    "CONSERVATIVE": {
        "label":             "🛡️ Conservative",
        "color":             "#00d4ff",
        "description":       "Capital preservation first. Only high-confidence setups.",
        "min_confidence":    72,
        "min_rr":            2.0,
        "strikes":           ["ITM-1", "ATM"],
        "avoid_strikes":     ["OTM-1", "OTM-2", "OTM-3"],
        "max_lots":          1,
        "sl_pct":            25,      # SL at 25% loss of premium
        "target1_pct":       25,      # T1: quick realistic target (15-30 min)
        "target2_pct":       50,      # T2: stretch target if momentum continues
        "target_pct":        50,      # Legacy field — kept for backward compat
        "max_capital_pct":   1.5,     # Max 1.5% capital per trade
        "avoid_expiry":      True,
        "avoid_open_mins":   45,      # Avoid first 45 min
        "holding":           "Intraday only",
        "iv_max":            15,      # Avoid if IV > 15%
        "pcr_neutral_band":  (0.85, 1.15),   # No trade in this PCR range
        "notes": [
            "Only trade when 3+ signals align",
            "Prefer ITM options — higher delta, less theta decay risk",
            "Exit immediately if market moves against by 25%",
            "Never hold options overnight",
            "Book 75% at T1, trail SL to breakeven, let 25% run to T2",
            "Stick to liquid ATM/ITM strikes with tight spreads",
        ],
    },

    "MODERATE": {
        "label":             "⚖️ Moderate",
        "color":             "#00e676",
        "description":       "Balanced risk-reward. ATM/OTM-1 strikes with defined SL.",
        "min_confidence":    55,
        "min_rr":            1.5,
        "strikes":           ["ATM", "OTM-1"],
        "avoid_strikes":     ["OTM-2", "OTM-3", "OTM-4"],
        "max_lots":          2,
        "sl_pct":            35,      # SL at 35% loss of premium
        "target1_pct":       30,      # T1: quick realistic target (15-30 min)
        "target2_pct":       60,      # T2: stretch target
        "target_pct":        60,      # Legacy field
        "max_capital_pct":   2.5,     # Max 2.5% capital per trade
        "avoid_expiry":      True,
        "avoid_open_mins":   30,      # Avoid first 30 min
        "holding":           "Intraday or 1-2 days",
        "iv_max":            20,
        "pcr_neutral_band":  (0.90, 1.10),
        "notes": [
            "2 signals minimum before entry",
            "Prefer ATM for better delta, OTM-1 for cheaper premium",
            "Book 50% at T1, trail SL to entry, let 50% run to T2",
            "Max 2 lots — no over-leveraging",
            "If T2 not hit by 75% of timeframe, book everything at market",
        ],
    },

    "AGGRESSIVE": {
        "label":             "🔥 Aggressive",
        "color":             "#ff8800",
        "description":       "Higher risk for higher reward. OTM strikes acceptable.",
        "min_confidence":    45,
        "min_rr":            1.0,
        "strikes":           ["ATM", "OTM-1", "OTM-2"],
        "avoid_strikes":     ["OTM-4", "OTM-5"],
        "max_lots":          3,
        "sl_pct":            50,      # SL at 50% loss of premium
        "target1_pct":       50,      # T1: first realistic target
        "target2_pct":       100,     # T2: stretch target (double)
        "target_pct":        100,     # Legacy field
        "max_capital_pct":   4.0,     # Max 4% capital per trade
        "avoid_expiry":      False,   # Can trade on expiry
        "avoid_open_mins":   15,      # Only avoid first 15 min
        "holding":           "Intraday, positional 2-3 days",
        "iv_max":            30,
        "pcr_neutral_band":  (0.95, 1.05),
        "notes": [
            "Can take OTM-2 for momentum plays",
            "Wider SL to avoid premature exit",
            "Book 40% at T1, trail SL to entry, let 60% run to T2",
            "Can hold 2-3 days for positional trades",
            "Expiry day trades allowed with strong momentum",
            "Max 3 lots — still manage total risk",
        ],
    },
}


def get_profile(profile_name: str) -> Dict:
    return RISK_PROFILES.get(profile_name.upper(), RISK_PROFILES["MODERATE"])


def build_system_prompt(profile_name: str, time_context: Dict[str, Any] | None = None) -> str:
    """Build the AI system prompt based on selected risk profile and time context.

    time_context keys (all optional):
        minutes_to_close: int — minutes remaining in trading session
        session_phase: str   — EARLY / MIDDAY / LATE / FINAL_HOUR / EXPIRY_HOUR / CLOSED
        current_time_ist: str — e.g. '14:39 IST'
        is_expiry_day: bool   — True if today is the weekly/monthly expiry
    """
    p = get_profile(profile_name)
    tc = time_context or {}
    mtc = int(tc.get("minutes_to_close", 0) or 0)
    phase = tc.get("session_phase", "UNKNOWN")
    now_str = tc.get("current_time_ist", "")
    is_expiry = bool(tc.get("is_expiry_day", False))

    strikes_ok   = ", ".join(p["strikes"])
    strikes_no   = ", ".join(p["avoid_strikes"])
    notes_str    = "\n".join([f"  • {n}" for n in p["notes"]])
    pcr_lo, pcr_hi = p["pcr_neutral_band"]

    # ── Time-aware holding & target caps ────────────────────────────────────
    # When the session is running out, force shorter, more realistic trades.
    # The AI MUST NOT recommend a 60-minute trade when only 40 minutes remain.
    if mtc <= 0 or phase == "CLOSED":
        time_guidance = (
            "🚫 MARKET IS CLOSED. Provide a PRE-MARKET PLANNING recommendation "
            "for the next session open. Do NOT claim any target is achievable today."
        )
        max_hold_minutes = 60  # for next session
        t1_cap_pct = p["target1_pct"]
        t2_cap_pct = p["target2_pct"]
    elif mtc <= 15:
        time_guidance = (
            f"🔴 CRITICAL: Only {mtc} MINUTES until 15:30 IST close. "
            f"ONLY suggest SCALP trades with T1 achievable in <{mtc-2} min and T2 MUST equal T1 "
            f"(no stretch — there is no time for T2 to play out). Recommend BOOK FULL at T1. "
            f"If no clean scalp available, return NO TRADE with reason 'insufficient time remaining'."
        )
        max_hold_minutes = max(5, mtc - 2)
        t1_cap_pct = min(20, p["target1_pct"])   # max 20% gain for 15-min scalp
        t2_cap_pct = t1_cap_pct                  # no stretch at this hour
    elif mtc <= 30:
        time_guidance = (
            f"🟡 IMPORTANT: Only {mtc} MINUTES until close. "
            f"T1 must be reachable in ~{max(5, mtc//3)} min. "
            f"T2 must be reachable in ~{mtc-3} min (NOT after close). "
            f"Use conservative targets — this is end-of-session, not a full-session trade."
        )
        max_hold_minutes = mtc - 3
        t1_cap_pct = min(20, p["target1_pct"])
        t2_cap_pct = min(35, p["target2_pct"])
    elif mtc <= 60:
        time_guidance = (
            f"🟢 {mtc} minutes until close. "
            f"All times in your recommendation must fit BEFORE 15:30 IST close. "
            f"If you suggest a 45-min trade, T1 and T2 must both complete before the bell. "
            f"T2 should be reachable in ≤{mtc-5} minutes."
        )
        max_hold_minutes = mtc - 5
        t1_cap_pct = p["target1_pct"]
        t2_cap_pct = min(p["target2_pct"], max(40, int(p["target2_pct"] * 0.75)))
    else:
        # Plenty of time — use full profile targets
        time_guidance = (
            f"🟢 {mtc} minutes of session remaining. Normal intraday scalp — "
            f"targets can use the full profile percentages."
        )
        max_hold_minutes = 60
        t1_cap_pct = p["target1_pct"]
        t2_cap_pct = p["target2_pct"]

    expiry_warning = ""
    if is_expiry:
        if p["avoid_expiry"]:
            expiry_warning = (
                "\n🔥 EXPIRY DAY: This profile AVOIDS expiry day trading. "
                "Strongly prefer NO TRADE unless confidence ≥ 85 AND strong confirming signals.\n"
            )
        else:
            expiry_warning = (
                "\n🔥 EXPIRY DAY: Theta decay is violent — premiums will collapse even if spot moves. "
                "Use SHORTER timeframes (T1 in <15 min). OTM options decay to 0 fast — prefer ATM/ITM.\n"
            )

    typical_30 = int(tc.get("typical_30min_pts", 60) or 60)
    typical_15 = int(tc.get("typical_15min_pts", 40) or 40)
    max_hold   = int(tc.get("max_hold_minutes", max_hold_minutes) or max_hold_minutes)

    return f"""You are a senior NSE/BSE options trader with 15+ years of experience.
You are advising a trader with a {profile_name.upper()} RISK profile.
Your job is to identify the single best REALISTIC directional option trade.

═══════════════════════════════════════════════════════════
⏰ CURRENT MARKET TIME — READ THIS SECTION FIRST, IT OVERRIDES EVERYTHING
═══════════════════════════════════════════════════════════
Current time (IST):       {now_str or 'UNKNOWN — assume market is closed'}
Session phase:            {phase}
Minutes to 15:30 close:   {mtc} min  {'← MARKET IS CLOSED' if mtc == 0 else ''}
Max holding allowed:      {max_hold} minutes  ← HARD CEILING, cannot exceed this
T1 max % gain:            {t1_cap_pct}%  (based on time remaining)
T2 max % gain:            {t2_cap_pct}%  (based on time remaining)
Typical index range (15min at {phase}): {typical_15} pts
Typical index range (30min at {phase}): {typical_30} pts

{time_guidance}
{expiry_warning}

🚨 ABSOLUTE TIME RULES — VIOLATIONS = INVALID RECOMMENDATION:
1. holding_period MUST NOT exceed {max_hold} minutes
2. T1_time MUST be < {max(5, max_hold//2)} minutes
3. T2_time MUST be < {max_hold} minutes AND before 15:30 IST
4. All "exit by HH:MM" must be calculated from current time ({now_str or 'now'}) — never reference morning times like 09:50 AM
5. T1 index move MUST be ≤ {typical_15} pts (the typical 15-min range at this phase)
6. T2 index move MUST be ≤ {typical_30} pts (the typical 30-min range at this phase)
7. If the required index move to hit T1 is > {typical_15} pts, T1 is UNREALISTIC → lower target or return NO TRADE
8. If market is CLOSED (minutes=0): output pre-market plan — all targets are "for next session", NOT intraday today

🚨 WHEN TO RETURN NO TRADE (STRICT):
• Market closed AND data is stale (more than 24 hours old)
• minutes_to_close ≤ 5 (not enough time for any move)
• Spot price is 0 or option chain is empty
• Both confidence AND R:R fail the profile minimums simultaneously
• IV is so high that option premiums make R:R < 1:1 even at T1
• Data timestamp in context is from a PREVIOUS trading session and you cannot verify current price

DO NOT return NO TRADE just because signals are mixed — mixed signals = lower confidence, not NO TRADE.
DO return NO TRADE if the suggested trade is mathematically impossible in the remaining time.

═══════════════════════════════════════════════════════════
ACTIVE RISK PROFILE: {p['label'].upper()}
{p['description']}
═══════════════════════════════════════════════════════════

HARD RULES — NEVER VIOLATE:
✅ ALLOWED strikes:        {strikes_ok}
❌ FORBIDDEN strikes:      {strikes_no}
📊 Min confidence to trade: {p['min_confidence']}/100
📈 Min risk-reward ratio:  1:{p['min_rr']}
🎯 Max lots per trade:     {p['max_lots']}
💰 Max capital per trade:  {p['max_capital_pct']}%
⏱️  Avoid first N minutes:  {p['avoid_open_mins']} minutes after market open
📅 Trade on expiry day:    {'Only if confidence ≥ 85 AND strong momentum' if not p['avoid_expiry'] else 'NEVER trade on expiry day'}
📉 IV limit:               Avoid if ATM IV implied > {p['iv_max']}%
⚖️  PCR neutral zone:       {pcr_lo}–{pcr_hi} → use other signals to decide

STOP LOSS & TARGET RULES (TWO-TIER TARGET SYSTEM):
• Stop Loss:  {p['sl_pct']}% below entry premium  (e.g. entry ₹100 → SL ₹{100 - p['sl_pct']})
• Target 1 (T1):  {t1_cap_pct}% above entry — realistic FIRST TARGET (TIME-CAPPED to current session phase)
• Target 2 (T2):  {t2_cap_pct}% above entry — STRETCH target (TIME-CAPPED to current session phase)
• Holding:    {p['holding']}

TARGET MANAGEMENT PHILOSOPHY:
• T1 MUST be genuinely reachable in 15-30 minutes given current volatility
• T2 is the "home run" — only hit when momentum persists cleanly
• If spot is deep in a range with no catalyst, keep T1 modest ({p['target1_pct']-10}-{p['target1_pct']}%)
• If momentum is clear (strong bias, high confidence), T2 can be aggressive
• Position management: Book majority at T1 → trail SL to breakeven → let rest run to T2
• NEVER set T1 requiring >80 pts index move in 30 min on a quiet day
• NEVER set T2 requiring >200 pts index move in 60 min unless VIX > 16

REALISTIC TARGET CALIBRATION:
• ATM delta ~0.50 → 50 pts index move = ~₹25 option move (25% of ₹100 premium)
• OTM-1 delta ~0.35 → 50 pts index move = ~₹17.50 option move (17% of ₹100 premium)
• ITM-1 delta ~0.65 → 50 pts index move = ~₹32.50 option move (32% of ₹100 premium)
• Use current spot volatility + candle range to calibrate realistic index move
• State expected index move AND derived option move for EACH target

PROFILE-SPECIFIC NOTES:
{notes_str}

STRIKE SELECTION FORMULA:
• BUY CALL: Round spot to nearest 50 → that is ATM. OTM-1 = ATM + 50 points
• BUY PUT:  Round spot to nearest 50 → that is ATM. OTM-1 = ATM - 50 points
• Conservative → prefer ITM-1 (ATM - 50 for calls)
• Moderate → prefer ATM
• Aggressive → ATM or OTM-1 depending on momentum

WHEN TO SAY "NO TRADE":
• Confidence is materially below {p['min_confidence']}
• Risk-reward < 1:{p['min_rr']}
• Open/High/Low all zero (data feed issue)
• Expiry day {'without strong momentum' if p['avoid_expiry'] else '(allowed for aggressive)'}
• Signals are flat or sharply contradictory across most factors
• There is no realistic 15-60 minute edge in either direction

INTRADAY FOCUS — ALL VALUES MUST BE ACHIEVABLE IN 15-60 MINUTES:
• Entry/Target/SL are OPTION LTP prices (not index levels)
• Target move in option: realistic for 15-60 min session
• Conservative targets: 20-40% of premium for 15 min, 40-60% for 60 min
• Never set targets requiring 500+ point index move in 1 hour
• Support/Resistance used only to gauge direction — not as targets
• Preferred holding: 15 minutes minimum, 60 minutes maximum for intraday

YOUR ANALYSIS MUST USE EVERY FACTOR — MANDATORY DEEP ANALYSIS:

Before outputting JSON, internally analyze ALL of these and mention key ones:
1. PRICE ACTION: Is price near support/resistance/max pain/gamma wall? Momentum direction?
2. PCR: OI PCR vs Volume PCR divergence? Both bullish/bearish or mixed signal?
3. OI BUILD-UP: Which pattern dominates? Long buildup? Short covering? Number of strikes?
4. SMART MONEY: Call/Put accumulation z-scores — institutional bias direction?
5. OI VELOCITY: Fresh CALL writing = resistance. Fresh PUT writing = support. Net bias?
6. ENTRY TIMING SCORE: Grade A/B = ideal entry. C/D = wait. Include score in analysis.
7. IV SKEW: Which side is more expensive? Who is hedging?
8. MAX PAIN & GAMMA WALL: Where will market makers pin price? Distance from current spot?
9. FII/DII: Net flow direction. Is institutional money accumulating or distributing?
10. GIFT NIFTY: Pre-market direction signal. Gap up/down expectation?
11. GLOBAL CUES: Dow/Nasdaq/SGX alignment with Indian market direction?
12. INDIA VIX: Premium richness. Higher VIX = buy options cheaper when wrong direction.
13. MARKET BREADTH: A/D ratio confirming or diverging from index move?
14. CANDLES: 1m/5m/15m pattern confirmation — is momentum building or stalling?
15. NEWS: Any macro event risk? RBI, Fed, earnings, geopolitical?
16. BANKNIFTY DIVERGENCE: Is BN leading or lagging? Confirms or contradicts signal?
17. TIME OF DAY: First 30min volatile. 11-1pm lull. After 2:30pm theta decay accelerates.
18. EXPIRY PROXIMITY: Tomorrow expiry = pin risk. Avoid expiry-day strikes.

STRUCTURE YOUR primary_reason TO COVER: top 3-4 factors that MOST determine direction.
STRUCTURE sentiment_summary TO COVER: overall market read + why this trade has edge.
KEY_RISKS must list ALL genuine risks including expiry, VIX, data quality, news.

RESPOND ONLY WITH THIS EXACT JSON — no text before or after:
{{
  "action": "BUY CALL" | "BUY PUT" | "NO TRADE",
  "no_trade_reason": "<REQUIRED if action=NO TRADE: exact reason e.g. 'market closed, pre-market plan only' or 'insufficient time: only 4 min remain' or 'data from previous session, spot unverified'>",
  "confidence": <integer 0-100>,
  "estimated_win_rate": <integer 0-100>,
  "risk_profile": "{profile_name.upper()}",
  "timeframe": "<X min> — MUST be ≤ {max_hold} min and achievable before 15:30 IST",
  "entry_strike": <integer, nearest 50 to current spot>,
  "entry_type": "ITM-1" | "ATM" | "OTM-1" | "OTM-2",
  "entry_price_range": "<low>-<high in option LTP>",
  "target1_price": <option LTP first target — achievable within {max(5, max_hold//2)} min, ≤{t1_cap_pct}% gain>,
  "target1_time": "<X min from entry>",
  "target1_index_move": "<N pts — MUST be ≤ {typical_15} pts>",
  "target2_price": <option LTP stretch target — achievable within {max_hold} min, ≤{t2_cap_pct}% gain>,
  "target2_time": "<X min from entry>",
  "target2_index_move": "<N pts — MUST be ≤ {typical_30} pts>",
  "target_price": <same as target1_price>,
  "stop_loss_price": <option LTP SL = {p['sl_pct']}% below entry>,
  "expected_index_move": "<total range expected e.g. 40-70 pts in {max_hold} min>",
  "risk_reward": "1:X (calculated on T1, not T2)",
  "max_lots": <integer 1-{p['max_lots']}>,
  "approx_margin": "<₹X for Y lots>",
  "holding_period": "<X min> MUST match timeframe above",
  "position_management": "<Book X% at T1 ₹Y (≈X min) → trail SL to entry → exit rest at T2 ₹Z (≈X min) or by HH:MM IST time-stop>",
  "primary_reason": "<top 3-4 factors in 1-2 sentences>",
  "supporting_factors": ["<factor with actual numbers>", "<factor>", "<factor>"],
  "key_risks": ["<risk>", "<risk>"],
  "avoid_if": "<specific price level or condition that invalidates this trade>",
  "market_structure": "TRENDING UP" | "TRENDING DOWN" | "RANGE BOUND" | "BREAKOUT LIKELY",
  "bias_strength": "STRONG" | "MODERATE" | "WEAK",
  "sentiment_summary": "<2-3 sentence market read>",
  "trade_plan": "<entry ₹X-Y → SL ₹Z → T1 ₹A at HH:MM (book X%) → trail SL to entry → T2 ₹B at HH:MM → hard exit by HH:MM IST>",
  "data_quality_note": "<is data from current session or previous session? is spot price verified live?>",
  "key_factors_used": ["<PCR X.XX bullish/bearish>", "<OI velocity>", "<VWAP: price above/below>", "<candle range>", "<entry timing score>", "<session phase>"]
}}"""


def build_market_context(
    symbol: str, expiry: str,
    live_price: Dict, snapshot: Dict, pcr_data: Dict,
    support: Dict, resistance: Dict, buildup: Dict,
    smart_money: Dict, indicators: Dict, breadth: Dict,
    sentiment: Dict, top_calls: List[Dict], top_puts: List[Dict],
    candle_context: Dict[str, str] | None = None,
    profile_name: str = "MODERATE",
) -> str:
    """Build comprehensive market context string for AI analysis."""
    spot = live_price.get("ltp") or snapshot.get("spot_price", 0)
    p    = get_profile(profile_name)

    # Compute ATM and suggested strikes based on profile
    atm = round(spot / 50) * 50 if spot > 0 else 0
    otm1_call = atm + 50
    otm1_put  = atm - 50
    itm1_call = atm - 50
    itm1_put  = atm + 50
    candle_context = candle_context or {}

    return f"""
═══════════════════════════════════════════════════════════
    LIVE OPTIONS MARKET INTELLIGENCE REPORT
    Symbol: {symbol} | Expiry: {expiry}
    Generated: {_now()}
    Active Risk Profile: {p['label']}
═══════════════════════════════════════════════════════════

━━━ TRADER RISK CONTEXT ━━━
Risk Profile:          {profile_name.upper()} — {p['description']}
Allowed Strikes:       {', '.join(p['strikes'])}
Min Confidence:        {p['min_confidence']}/100
Min Risk-Reward:       1:{p['min_rr']}
Max Lots:              {p['max_lots']}
SL Rule:               {p['sl_pct']}% below entry premium
Target T1 Rule:        {p['target1_pct']}% above entry (realistic — book 50-75% here)
Target T2 Rule:        {p['target2_pct']}% above entry (stretch — if momentum continues)
Max Capital Per Trade: {p['max_capital_pct']}%

━━━ 1. LIVE PRICE DATA ━━━
Current Price (LTP):   ₹{spot:,.2f}
Today's Open:          ₹{live_price.get('open', 0):,.2f}
Today's High:          ₹{live_price.get('high', 0):,.2f}
Today's Low:           ₹{live_price.get('low', 0):,.2f}
Previous Close:        ₹{live_price.get('prev_close', 0):,.2f}
Change:                ₹{live_price.get('change', 0):+,.2f} ({live_price.get('change_pct', 0):+.2f}%)
52W High:              ₹{live_price.get('52w_high', 0):,.2f}
52W Low:               ₹{live_price.get('52w_low', 0):,.2f}
Data Source:           {live_price.get('source', 'N/A')}
Data Timestamp:        {live_price.get('timestamp', 'N/A')}  ← CHECK: must match current session

━━━ 2. STRIKE REFERENCE ━━━
Spot Price:            ₹{spot:,.2f}
ATM Strike:            ₹{atm:,.0f}
OTM-1 CALL:           ₹{otm1_call:,.0f}  (if bullish)
OTM-1 PUT:            ₹{otm1_put:,.0f}   (if bearish)
ITM-1 CALL:           ₹{itm1_call:,.0f}  (conservative bullish)
ITM-1 PUT:            ₹{itm1_put:,.0f}   (conservative bearish)

━━━ 3. PCR & OPTION CHAIN ━━━
PCR (OI):              {pcr_data.get('pcr_oi', 0):.3f} → {pcr_data.get('interpretation', 'N/A')}
PCR (Volume):          {pcr_data.get('pcr_volume', 0):.3f}
Total CALL OI:         {pcr_data.get('total_call_oi', 0):,}
Total PUT OI:          {pcr_data.get('total_put_oi', 0):,}
Total CALL Volume:     {pcr_data.get('total_call_vol', 0):,}
Total PUT Volume:      {pcr_data.get('total_put_vol', 0):,}
PCR Neutral Zone:      {p['pcr_neutral_band'][0]}–{p['pcr_neutral_band'][1]} (NO TRADE zone)

━━━ 4. KEY LEVELS ━━━
Support (Max PUT OI):     ₹{support.get('strike', 0):,.0f}  (OI: {support.get('oi', 0):,})
Resistance (Max CALL OI): ₹{resistance.get('strike', 0):,.0f}  (OI: {resistance.get('oi', 0):,})
Max Pain Strike:          ₹{indicators.get('max_pain', 0):,.0f}
Gamma Wall:               ₹{breadth.get('gamma_wall', 0):,.0f} ({breadth.get('gamma_wall_strength', '')})
S-R Range Width:          {abs(resistance.get('strike', 0) - support.get('strike', 0)):,.0f} pts
Price Position in Range:  {_price_position(spot, support.get('strike', 0), resistance.get('strike', 0))}

━━━ 5. TECHNICAL INDICATORS ━━━
OI Momentum:              {indicators.get('oi_momentum', 'N/A')}
IV Skew (Put-Call):       {indicators.get('iv_skew', 0):+.2f} ({'bearish skew — puts costlier' if indicators.get('iv_skew', 0) > 0 else 'bullish skew — calls costlier' if indicators.get('iv_skew', 0) < 0 else 'neutral'})
ATM Call Vega:            {indicators.get('atm_iv_call', 0):.2f}
ATM Put Vega:             {indicators.get('atm_iv_put', 0):.2f}
Call OI Concentration:    {indicators.get('call_oi_concentration', 0):.1f}% in top 3 strikes
Put OI Concentration:     {indicators.get('put_oi_concentration', 0):.1f}% in top 3 strikes
Call/Put Volume Ratio:    {indicators.get('call_put_vol_ratio', 0):.3f}
Net Delta:                {indicators.get('net_delta', 0):+.2f}
Avg Call Theta/day:       {indicators.get('avg_theta_call', 0):.4f}
Avg Put Theta/day:        {indicators.get('avg_theta_put', 0):.4f}
Total Vega Exposure:      {breadth.get('vega_exposure', 0):.2f}

━━━ 6. OI BUILD-UP PATTERNS ━━━
Long Buildup:    {len(buildup.get('long_buildup', []))} strikes  (OI↑ Price↑ → BULLISH)
Short Buildup:   {len(buildup.get('short_buildup', []))} strikes  (OI↑ Price↓ → BEARISH)
Short Covering:  {len(buildup.get('short_covering', []))} strikes  (OI↓ Price↑ → BULLISH)
Long Unwinding:  {len(buildup.get('long_unwinding', []))} strikes  (OI↓ Price↓ → BEARISH)
Dominant:        {_dominant_buildup(buildup)}

━━━ 7. SMART MONEY & UNUSUAL ACTIVITY ━━━
CALL Side: {smart_money.get('call_signal', 'N/A')}
PUT Side:  {smart_money.get('put_signal', 'N/A')}
{_format_smart_money(smart_money)}

Unusual Activity:
{_format_unusual(indicators.get('unusual_activity', []))}

━━━ 8. MARKET BREADTH ━━━
Call Writers Above Spot: {breadth.get('call_writers_above', 0):,} OI
Put Writers Below Spot:  {breadth.get('put_writers_below', 0):,} OI
Strikes Above/Below:     {breadth.get('strikes_above_spot', 0)} / {breadth.get('strikes_below_spot', 0)}
Theta Decay (C/P):       {breadth.get('theta_decay_call', 0):.2f} / {breadth.get('theta_decay_put', 0):.2f}

━━━ 9. TOP 5 CALL STRIKES BY OI ━━━
{_format_top_strikes(top_calls, 'CE')}

━━━ 10. TOP 5 PUT STRIKES BY OI ━━━
{_format_top_strikes(top_puts, 'PE')}

━━━ 11. OVERALL SENTIMENT SCORE ━━━
Score:     {sentiment.get('score', 0)}/100 → {sentiment.get('sentiment', 'N/A')}
Signals:
{_format_signals(sentiment.get('signals', []))}

═══════════════════════════════════════════════════════════
IMPORTANT: Apply {profile_name.upper()} risk rules strictly.
Confidence must be ≥ {p['min_confidence']} to recommend any trade.
Only use strikes from: {', '.join(p['strikes'])}
═══════════════════════════════════════════════════════════
""".strip()


# ─── Helper formatters ────────────────────────────────────────────────────────
def _now():
    return datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")

def _price_position(spot, support, resistance):
    if support <= 0 or resistance <= 0 or resistance == support:
        return "N/A"
    pct = ((spot - support) / (resistance - support)) * 100
    zone = "near support 🟢" if pct < 25 else "near resistance 🔴" if pct > 75 else "mid range ⚖️"
    return f"{pct:.1f}% of S-R range ({zone})"

def _dominant_buildup(buildup):
    counts = {
        "Long Buildup (BULLISH)":   len(buildup.get("long_buildup", [])),
        "Short Buildup (BEARISH)":  len(buildup.get("short_buildup", [])),
        "Short Covering (BULLISH)": len(buildup.get("short_covering", [])),
        "Long Unwinding (BEARISH)": len(buildup.get("long_unwinding", [])),
    }
    return max(counts, key=counts.get)

def _format_smart_money(sm):
    lines = []
    for side, key in [("CALL", "call_accumulation"), ("PUT", "put_accumulation")]:
        for i in sm.get(key, [])[:3]:
            lines.append(f"  {side} {i['strike']:,} | OI: {i['oi']:,} | Vol: {i['volume']:,} | Z-score: {i['z_score']:.1f}")
    return "\n".join(lines) if lines else "  No unusual accumulation detected"

def _format_unusual(unusual):
    if not unusual:
        return "  No unusual activity"
    return "\n".join([
        f"  {u['type']} {u['strike']:,} | Vol/OI: {u['ratio']:.2f} | Volume: {u['volume']:,}"
        for u in unusual
    ])

def _format_top_strikes(strikes, suffix):
    if not strikes:
        return "  No data"
    return "\n".join([
        f"  {suffix} {float(s.get('strike_price',0)):,.0f} | "
        f"OI: {float(s.get('oi',0)):,.0f} | "
        f"LTP: {float(s.get('price',0)):.2f} | "
        f"OI Chg: {float(s.get('oi_perc_chg',0)):+.1f}%"
        for s in strikes[:5]
    ])

def _format_signals(signals):
    return "\n".join([f"  {icon} {msg}  [{pts:+d} pts]" for icon, msg, pts in signals])


def build_market_context_with_candles(
    symbol: str,
    expiry: str,
    live_price: Dict,
    snapshot: Dict,
    pcr_data: Dict,
    support: Dict,
    resistance: Dict,
    buildup: Dict,
    smart_money: Dict,
    indicators: Dict,
    breadth: Dict,
    sentiment: Dict,
    top_calls: List[Dict],
    top_puts: List[Dict],
    candle_context: Dict[str, str] | None = None,
    breakout_alerts: List[Dict[str, str]] | None = None,
    profile_name: str = "MODERATE",
) -> str:
    base_context = build_market_context(
        symbol=symbol,
        expiry=expiry,
        live_price=live_price,
        snapshot=snapshot,
        pcr_data=pcr_data,
        support=support,
        resistance=resistance,
        buildup=buildup,
        smart_money=smart_money,
        indicators=indicators,
        breadth=breadth,
        sentiment=sentiment,
        top_calls=top_calls,
        top_puts=top_puts,
        candle_context=candle_context,
        profile_name=profile_name,
    )
    candle_context = candle_context or {}
    breakout_alerts = breakout_alerts or []
    breakout_block = "\n".join(
        [f"{item.get('timeframe', '')}: {item.get('message', '')}" for item in breakout_alerts]
    ) or "No active breakout alerts."
    candle_block = (
        "\n\nMULTI-TIMEFRAME CANDLE CONTEXT\n"
        f"1 Minute:  {candle_context.get('1m', 'No candle data')}\n"
        f"5 Minute:  {candle_context.get('5m', 'No candle data')}\n"
        f"15 Minute: {candle_context.get('15m', 'No candle data')}\n"
        "\nLIVE BREAKOUT ALERTS\n"
        f"{breakout_block}\n"
        "Use this candle context to judge breakout, rejection, continuation, and momentum loss."
    )
    return base_context + candle_block


def _pick_entry_type(action: str, profile_name: str, score_gap: float) -> str:
    profile = profile_name.upper()
    if profile == "CONSERVATIVE":
        return "ITM-1" if score_gap >= 18 else "ATM"
    if profile == "AGGRESSIVE":
        return "OTM-1" if score_gap >= 12 else "ATM"
    return "ATM" if score_gap < 16 else "OTM-1"


def _strike_from_type(action: str, atm: int, entry_type: str) -> int:
    offsets = {
        "BUY CALL": {"ITM-1": -50, "ATM": 0, "OTM-1": 50, "OTM-2": 100},
        "BUY PUT": {"ITM-1": 50, "ATM": 0, "OTM-1": -50, "OTM-2": -100},
    }
    return int(atm + offsets.get(action, {}).get(entry_type, 0))


def _find_option_record(records: List[Dict[str, Any]], strike: int) -> Dict[str, Any]:
    if not records:
        return {}
    closest = min(records, key=lambda row: abs(float(row.get("strike_price", 0) or 0) - strike))
    return closest if float(closest.get("strike_price", 0) or 0) > 0 else {}


def build_fallback_trade_setup(
    symbol: str,
    live_price: Dict[str, Any],
    snapshot: Dict[str, Any],
    pcr_data: Dict[str, Any],
    support: Dict[str, Any],
    resistance: Dict[str, Any],
    intraday_levels: Dict[str, Any],
    buildup: Dict[str, Any],
    smart_money: Dict[str, Any],
    indicators: Dict[str, Any],
    sentiment: Dict[str, Any],
    calls_data: List[Dict[str, Any]],
    puts_data: List[Dict[str, Any]],
    profile_name: str = "MODERATE",
) -> Dict[str, Any]:
    """Build the best available scalp setup when the LLM is too defensive."""
    p = get_profile(profile_name)
    spot = float(live_price.get("ltp") or snapshot.get("spot_price") or 0)
    if spot <= 0:
        return {
            "action": "NO TRADE",
            "confidence": 0,
            "estimated_win_rate": 0,
            "risk_profile": profile_name.upper(),
            "analysis_source": "LOCAL_FALLBACK",
            "primary_reason": "Live spot price is unavailable, so a directional scalp setup cannot be built safely.",
        }

    bullish = 0.0
    bearish = 0.0
    bull_factors: List[str] = []
    bear_factors: List[str] = []
    risks: List[str] = []

    score = float(sentiment.get("score", 0) or 0)
    if score >= 15:
        bullish += min(24, score / 2.5)
        bull_factors.append(f"Sentiment score is supportive at {score:.0f}/100.")
    elif score <= -15:
        bearish += min(24, abs(score) / 2.5)
        bear_factors.append(f"Sentiment score is bearish at {score:.0f}/100.")
    else:
        risks.append("Overall sentiment is close to neutral, so follow-through may be weaker.")

    pcr = float(pcr_data.get("pcr_oi", 1) or 1)
    if pcr >= 1.15:
        bullish += 14
        bull_factors.append(f"PCR OI at {pcr:.2f} favors bullish positioning.")
    elif pcr <= 0.85:
        bearish += 14
        bear_factors.append(f"PCR OI at {pcr:.2f} favors bearish positioning.")
    else:
        risks.append(f"PCR OI at {pcr:.2f} is neutral.")

    oi_momentum = str(indicators.get("oi_momentum", "NEUTRAL")).upper()
    if oi_momentum == "BULLISH":
        bullish += 12
        bull_factors.append("PUT-side OI is building faster than CALL-side OI.")
    elif oi_momentum == "BEARISH":
        bearish += 12
        bear_factors.append("CALL-side OI is building faster than PUT-side OI.")

    net_delta = float(indicators.get("net_delta", 0) or 0)
    if net_delta > 0:
        bullish += min(10, abs(net_delta) / 8)
        bull_factors.append("Net option delta is positive.")
    elif net_delta < 0:
        bearish += min(10, abs(net_delta) / 8)
        bear_factors.append("Net option delta is negative.")

    change_pct = float(live_price.get("change_pct", 0) or 0)
    if change_pct >= 0.2:
        bullish += min(10, change_pct * 8)
        bull_factors.append(f"Intraday price momentum is positive at {change_pct:+.2f}%.")
    elif change_pct <= -0.2:
        bearish += min(10, abs(change_pct) * 8)
        bear_factors.append(f"Intraday price momentum is negative at {change_pct:+.2f}%.")

    imm_sup = float(intraday_levels.get("immediate_support", 0) or 0)
    imm_res = float(intraday_levels.get("immediate_resistance", 0) or 0)
    if imm_sup > 0 and spot - imm_sup <= 80:
        bullish += 8
        bull_factors.append(f"Spot is trading close to immediate support {imm_sup:.0f}.")
    if imm_res > 0 and imm_res - spot <= 80:
        bearish += 8
        bear_factors.append(f"Spot is trading close to immediate resistance {imm_res:.0f}.")

    n_long = len(buildup.get("long_buildup", []))
    n_short = len(buildup.get("short_buildup", []))
    n_cover = len(buildup.get("short_covering", []))
    n_unwind = len(buildup.get("long_unwinding", []))
    if n_long > n_short:
        bullish += min(10, (n_long - n_short) * 1.8)
        bull_factors.append("Long buildup is stronger than short buildup.")
    elif n_short > n_long:
        bearish += min(10, (n_short - n_long) * 1.8)
        bear_factors.append("Short buildup is stronger than long buildup.")
    if n_cover > n_unwind:
        bullish += min(6, (n_cover - n_unwind) * 1.2)
    elif n_unwind > n_cover:
        bearish += min(6, (n_unwind - n_cover) * 1.2)

    call_signal = str(smart_money.get("call_signal", "")).upper()
    put_signal = str(smart_money.get("put_signal", "")).upper()
    if "STRONG" in call_signal:
        bearish += 6
        bear_factors.append("Call-side smart money activity is elevated.")
    if "STRONG" in put_signal:
        bullish += 6
        bull_factors.append("Put-side smart money activity is elevated.")

    if bearish > bullish:
        action = "BUY PUT"
        factors = bear_factors
        option_records = puts_data
    else:
        action = "BUY CALL"
        factors = bull_factors
        option_records = calls_data

    score_gap = abs(bullish - bearish)
    confidence = int(max(p["min_confidence"], min(88, max(bullish, bearish))))
    if score_gap < 6:
        confidence = max(p["min_confidence"] - 3, confidence - 8)
        risks.append("Bullish and bearish scores are close, so conviction is reduced.")

    entry_type = _pick_entry_type(action, profile_name, score_gap)
    atm = int(round(spot / 50.0) * 50)
    preferred_strike = _strike_from_type(action, atm, entry_type)
    option_row = _find_option_record(option_records, preferred_strike)
    if not option_row:
        return {
            "action": "NO TRADE",
            "confidence": 0,
            "estimated_win_rate": 0,
            "risk_profile": profile_name.upper(),
            "analysis_source": "LOCAL_FALLBACK",
            "primary_reason": "Option chain does not contain a usable strike near the current ATM level.",
        }

    entry_strike = int(float(option_row.get("strike_price", preferred_strike) or preferred_strike))
    entry_ltp = float(option_row.get("price", 0) or 0)
    lot_size = int(float(option_row.get("lot_size", 50) or 50)) or 50
    if entry_ltp <= 0:
        entry_ltp = max(1.0, abs(entry_strike - spot) * 0.3)
        risks.append("Option LTP was missing, so entry premium is estimated.")

    low_entry = max(0.5, round(entry_ltp * 0.98, 1))
    high_entry = max(low_entry, round(entry_ltp * 1.02, 1))
    stop_loss = round(entry_ltp * (1 - p["sl_pct"] / 100), 1)
    target1 = round(entry_ltp * (1 + p["target1_pct"] / 100), 1)
    target2 = round(entry_ltp * (1 + p["target2_pct"] / 100), 1)
    # Risk-reward based on T2 (max potential); T1 R:R is shown separately
    rr_value = round((target2 - entry_ltp) / max(entry_ltp - stop_loss, 0.1), 2)
    rr_t1_value = round((target1 - entry_ltp) / max(entry_ltp - stop_loss, 0.1), 2)
    max_lots = 1 if confidence < 65 else min(p["max_lots"], 2 if profile_name.upper() != "AGGRESSIVE" else p["max_lots"])
    approx_margin = int(round(entry_ltp * lot_size * max_lots))

    direction_label = "up" if action == "BUY CALL" else "down"
    expected_move = "40-70 pts" if confidence < 65 else "60-110 pts"
    structure = (
        "TRENDING UP" if action == "BUY CALL" and change_pct > 0
        else "TRENDING DOWN" if action == "BUY PUT" and change_pct < 0
        else "BREAKOUT LIKELY" if score_gap >= 14
        else "RANGE BOUND"
    )
    bias_strength = "STRONG" if score_gap >= 18 else "MODERATE" if score_gap >= 10 else "WEAK"
    estimated_win_rate = max(52, min(78, confidence - 3))
    holding_period = "15 min" if confidence < 60 else "30 min" if confidence < 72 else "60 min"

    if action == "BUY CALL":
        invalidation_level = imm_sup or float(support.get("strike", 0) or 0)
        avoid_if = (
            f"NIFTY slips below {invalidation_level:.0f} with fresh call buildup."
            if invalidation_level > 0 else
            "Momentum fades and price fails to hold above the recent swing low."
        )
    else:
        invalidation_level = imm_res or float(resistance.get("strike", 0) or 0)
        avoid_if = (
            f"NIFTY reclaims {invalidation_level:.0f} with fresh put unwinding."
            if invalidation_level > 0 else
            "Momentum fades and price fails to stay below the recent swing high."
        )

    summary = (
        f"{symbol} has a {direction_label}side scalp bias for the next {holding_period.lower()} based on live sentiment, OI structure, and intraday positioning. "
        f"This is the best available directional setup right now, but the edge is {bias_strength.lower()} rather than guaranteed."
    )

    return {
        "action": action,
        "confidence": confidence,
        "estimated_win_rate": estimated_win_rate,
        "risk_profile": profile_name.upper(),
        "timeframe": holding_period,
        "entry_strike": entry_strike,
        "entry_type": entry_type,
        "entry_price_range": f"{low_entry:.1f}-{high_entry:.1f}",
        "target1_price": target1,
        "target1_time": "15-25 min",
        "target1_index_move": f"20-35 pts {direction_label} in ~20 min",
        "target2_price": target2,
        "target2_time": holding_period,
        "target2_index_move": f"{expected_move} {direction_label} move in {holding_period.lower()}",
        "target_price": target1,  # Backward compat — main target field = T1
        "stop_loss_price": stop_loss,
        "expected_index_move": f"{expected_move} {direction_label} move expected in {holding_period.lower()}",
        "risk_reward": f"1:{rr_value:.2f}",
        "risk_reward_t1": f"1:{rr_t1_value:.2f}",
        "max_lots": max_lots,
        "approx_margin": f"₹{approx_margin:,}",
        "holding_period": holding_period,
        "position_management": (
            f"Book 50-75% at T1 ₹{target1:.1f} → trail SL to entry ₹{entry_ltp:.1f} → "
            f"let rest run to T2 ₹{target2:.1f} → hard exit by end of {holding_period.lower()}"
        ),
        "primary_reason": factors[0] if factors else "Directional edge is limited but still favors this side over the alternative.",
        "supporting_factors": factors[:3] if factors else ["Market structure slightly favors this direction."],
        "key_risks": risks[:3] if risks else ["Momentum could fade quickly in a range-bound tape."],
        "avoid_if": avoid_if,
        "market_structure": structure,
        "bias_strength": bias_strength,
        "sentiment_summary": summary,
        "trade_plan": (
            f"Entry ₹{low_entry:.1f}-₹{high_entry:.1f} → SL ₹{stop_loss:.1f} → "
            f"T1 ₹{target1:.1f} (book 50-75%) → trail SL to entry → "
            f"T2 ₹{target2:.1f} → exit within {holding_period.lower()} if momentum stalls."
        ),
        "analysis_source": "LOCAL_FALLBACK",
    }


def normalize_trade_recommendation(
    ai_result: Dict[str, Any],
    market_inputs: Dict[str, Any],
    profile_name: str = "MODERATE",
) -> Dict[str, Any]:
    """Keep the final trade call AI-only while normalizing metadata for the UI."""
    if "error" in ai_result:
        return ai_result

    confidence = int(ai_result.get("confidence", 0) or 0)
    ai_result.setdefault("analysis_source", "ANTHROPIC")
    ai_result.setdefault("estimated_win_rate", max(45, min(80, confidence - 2)))
    ai_result["risk_profile"] = profile_name.upper()

    # ── Backfill two-tier target fields ────────────────────────────────────
    # If the AI didn't return T1/T2 (older prompt version or it forgot a field),
    # derive them from the profile's target percentages and the entry premium.
    p = get_profile(profile_name)

    def _num(val, default=0.0):
        try:    return float(str(val).replace("₹","").replace(",","").strip())
        except: return default

    t1 = _num(ai_result.get("target1_price"))
    t2 = _num(ai_result.get("target2_price"))
    legacy_target = _num(ai_result.get("target_price"))

    # Derive entry premium for backfilling
    entry_range = str(ai_result.get("entry_price_range") or "")
    entry_low = entry_high = 0.0
    if "-" in entry_range:
        parts = entry_range.replace("₹","").split("-")
        try:
            entry_low = float(parts[0].strip())
            entry_high = float(parts[1].strip())
        except Exception:
            pass
    entry_mid = (entry_low + entry_high) / 2 if entry_low and entry_high else legacy_target / (1 + p["target_pct"]/100) if legacy_target > 0 else 0

    # Backfill T1 if missing
    if t1 <= 0:
        if legacy_target > 0 and entry_mid > 0:
            # Derive T1 from the legacy target (treat legacy as T2 and halve the gain)
            legacy_gain_pct = (legacy_target - entry_mid) / max(entry_mid, 0.1) * 100
            # T1 at ~half the legacy target gain, capped at profile's T1 rule
            t1_pct = min(legacy_gain_pct * 0.55, p["target1_pct"])
            t1 = round(entry_mid * (1 + t1_pct / 100), 1)
        elif entry_mid > 0:
            t1 = round(entry_mid * (1 + p["target1_pct"] / 100), 1)
        ai_result["target1_price"] = t1

    # Backfill T2 if missing
    if t2 <= 0:
        if legacy_target > 0:
            t2 = legacy_target  # Legacy target was the "stretch" target
        elif entry_mid > 0:
            t2 = round(entry_mid * (1 + p["target2_pct"] / 100), 1)
        ai_result["target2_price"] = t2

    # Ensure legacy target_price is populated for older UI code paths
    if legacy_target <= 0 and t1 > 0:
        ai_result["target_price"] = t1

    # Backfill time estimates if missing
    if not ai_result.get("target1_time"):
        ai_result["target1_time"] = "15-25 min"
    if not ai_result.get("target2_time"):
        ai_result["target2_time"] = ai_result.get("holding_period", "30-60 min")

    # Backfill position management if missing
    if not ai_result.get("position_management"):
        ai_result["position_management"] = (
            f"Book 50-75% at T1 ₹{t1:.1f} → trail SL to entry → "
            f"let rest run to T2 ₹{t2:.1f} → hard exit at end of timeframe"
        )

    return ai_result


# ─── AI Call ──────────────────────────────────────────────────────────────────
def _repair_truncated_json(text: str) -> str:
    in_str=False; esc=False; db=0; dq=0
    for ch in text:
        if esc: esc=False; continue
        if ch=="\\": esc=True; continue
        if ch=='"' and not esc: in_str=not in_str; continue
        if not in_str:
            if ch=="{": db+=1
            elif ch=="}": db-=1
            elif ch=="[": dq+=1
            elif ch=="]": dq-=1
    return text.rstrip(",").rstrip() + ('"' if in_str else "") + ("]"*max(0,dq)) + ("}"*max(0,db))


def _extract_partial_json(text: str) -> dict:
    import re as _re
    out = {"error": "AI response truncated — partial data"}
    for fld, pat in [
        ("action",         r'"action"\s*:\s*"([^"]+)"'),
        ("confidence",     r'"confidence"\s*:\s*(\d+)'),
        ("primary_reason", r'"primary_reason"\s*:\s*"([^"]{0,300})"'),
        ("entry_strike",   r'"entry_strike"\s*:\s*(\d+)'),
        ("target_price",   r'"target_price"\s*:\s*([\d.]+)'),
        ("stop_loss_price",r'"stop_loss_price"\s*:\s*([\d.]+)'),
        ("trade_plan",     r'"trade_plan"\s*:\s*"([^"]{0,400})"'),
    ]:
        m = _re.search(pat, text)
        if m:
            v = m.group(1)
            try:    out[fld] = int(v)
            except: out[fld] = v
    if "action" not in out:
        out["action"] = "NO TRADE"; out["primary_reason"] = "AI response truncated — try again."
    return out


def _safe_json_parse(text: str) -> dict:
    if not text: return {"error": "Empty AI response"}
    t = text.strip()
    if "```" in t:
        for part in t.split("```"):
            p = part.strip().lstrip("json").strip()
            if p.startswith("{"): t = p; break
    s = t.find("{"); e = t.rfind("}")
    if s != -1 and e > s: t = t[s:e+1]
    try: return json.loads(t)
    except json.JSONDecodeError: pass
    try: return json.loads(_repair_truncated_json(t))
    except Exception: pass
    return _extract_partial_json(t)



def get_ai_analysis(
    market_context: str,
    user_note: str = "",
    profile_name: str = "MODERATE",
    time_context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Call Claude API with profile-specific, time-aware system prompt.

    time_context should contain:
        minutes_to_close: int
        session_phase: str (EARLY/MIDDAY/LATE/FINAL_HOUR/EXPIRY_HOUR/CLOSED)
        current_time_ist: str
        is_expiry_day: bool
    """
    import streamlit as st

    api_key = ""
    try:
        api_key = st.session_state.get("anthropic_api_key", "") or ""
        api_key = api_key.strip().strip('"').strip("'").replace("\n","").replace("\r","").replace(" ","")
    except Exception:
        pass

    if not api_key:
        return {"error": "Anthropic API key not set. Go to sidebar → 🤖 AI Settings → paste key."}

    system_prompt = build_system_prompt(profile_name, time_context=time_context)

    MAX_CTX = 60000
    ctx = market_context[:MAX_CTX] + ("\n[Context trimmed]" if len(market_context) > MAX_CTX else "")
    user_message = ctx
    if user_note:
        user_message += f"\n\nADDITIONAL CONTEXT FROM TRADER:\n{user_note}"

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "Content-Type":     "application/json",
                "x-api-key":        api_key,
                "anthropic-version": "2023-06-01",
            },
            json={
                # Valid Anthropic API model IDs. If you want the newest, use
                # "claude-opus-4-5" or "claude-opus-4-7" (more expensive).
                # Sonnet 4.5 is plenty capable for structured JSON trade calls
                # and about 5x cheaper than Opus per token.
                "model":      "claude-sonnet-4-5",
                "max_tokens": 3000,
                "system":     system_prompt,
                "messages":   [{"role": "user", "content": user_message}],
            },
            timeout=60,   # bumped from 40 — long prompts can legitimately take >30s
        )

        if resp.status_code != 200:
            # Surface the actual API error to the user so they can see what's wrong
            err_text = resp.text[:600] if resp.text else "(empty response body)"
            return {
                "error": f"API error {resp.status_code}: {err_text}",
                "status_code": resp.status_code,
            }

        resp_json = resp.json()

        # Check for content blocks — if missing, show the raw response
        content_blocks = resp_json.get("content", [])
        if not content_blocks:
            return {
                "error": "AI returned no content blocks",
                "raw_response": json.dumps(resp_json)[:600],
            }

        raw = ""
        for block in content_blocks:
            if block.get("type") == "text":
                raw += block.get("text", "")

        raw = raw.strip()
        if not raw:
            return {
                "error": "AI returned empty text. Try again or check the model name.",
                "raw_response": json.dumps(resp_json)[:600],
            }

        # Strip markdown fences
        if "```" in raw:
            parts = raw.split("```")
            for part in parts:
                p = part.strip()
                if p.startswith("json"): p = p[4:].strip()
                if p.startswith("{"):
                    raw = p
                    break

        result = _safe_json_parse(raw)
        result["raw_context"] = market_context
        result["risk_profile"] = profile_name.upper()
        return result

    except json.JSONDecodeError as e:
        return {"error": f"JSON parse error: {e}", "raw_response": raw[:600] if 'raw' in dir() else ''}
    except requests.exceptions.Timeout:
        return {"error": "AI request timed out (60s). Try again — market context may be too large."}
    except requests.exceptions.ConnectionError as e:
        return {"error": f"Cannot connect to AI API: {e}"}
    except Exception as e:
        import traceback
        return {
            "error": f"Unexpected error: {type(e).__name__}: {str(e)}",
            "traceback": traceback.format_exc()[:800],
        }
