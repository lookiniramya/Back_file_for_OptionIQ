"""
ui_components.py — Light-mode professional trading dashboard — OptionsIQ v2.0
"""
import streamlit as st


def apply_custom_css():
    st.markdown("""<style>
/* ── PAGE ────────────────────────────────────────────────────── */
.stApp, .block-container          { background:#f0f2f6 !important; }
.block-container                   { padding-top:1rem !important; }

/* ── SIDEBAR ─────────────────────────────────────────────────── */
[data-testid="stSidebar"]         { background:#ffffff !important; border-right:1px solid #e0e4ec !important; }
[data-testid="stSidebar"] *       { color:#1a1a2e !important; }

/* ── BUTTONS ─────────────────────────────────────────────────── */
.stButton>button                   { border-radius:8px !important; font-weight:600 !important; transition:all .15s !important; }
.stButton>button[kind="primary"]   { background:linear-gradient(135deg,#1565c0,#1976d2) !important; color:#fff !important; border:none !important; box-shadow:0 2px 6px rgba(21,101,192,.3) !important; }
.stButton>button[kind="primary"]:hover { background:linear-gradient(135deg,#0d47a1,#1565c0) !important; box-shadow:0 4px 12px rgba(21,101,192,.4) !important; transform:translateY(-1px) !important; }
.stButton>button:not([kind="primary"]) { background:#fff !important; color:#1a237e !important; border:1px solid #c5cae9 !important; }
.stButton>button:not([kind="primary"]):hover { background:#e8eaf6 !important; }

/* ── TABS ────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] { background:#ffffff !important; border-radius:8px !important; border:1px solid #e0e4ec !important; padding:3px !important; gap:2px !important; }
.stTabs [data-baseweb="tab"]      { border-radius:6px !important; color:#5c6bc0 !important; font-weight:500 !important; font-size:0.85rem !important; }
.stTabs [aria-selected="true"]    { background:#1565c0 !important; color:#fff !important; }

/* ── EXPANDER ────────────────────────────────────────────────── */
.streamlit-expanderHeader         { background:#ffffff !important; border:1px solid #e0e4ec !important; border-radius:8px !important; color:#1a237e !important; font-weight:600 !important; }
.streamlit-expanderContent        { background:#fafbff !important; border:1px solid #e0e4ec !important; border-top:none !important; border-radius:0 0 8px 8px !important; }

/* ── SELECT / INPUT ──────────────────────────────────────────── */
.stSelectbox>div>div, .stTextInput>div>div>input, .stTextArea>div>div>textarea
                                   { background:#fff !important; border:1px solid #c5cae9 !important; border-radius:8px !important; color:#1a1a2e !important; }
.stSelectbox>div>div:focus-within, .stTextInput>div>div>input:focus
                                   { border-color:#1565c0 !important; box-shadow:0 0 0 3px rgba(21,101,192,.12) !important; }

/* ── DATAFRAME ───────────────────────────────────────────────── */
.dataframe                        { border-radius:8px !important; overflow:hidden !important; }
.dataframe thead tr th            { background:#e8eaf6 !important; color:#1a237e !important; font-weight:600 !important; border:none !important; }
.dataframe tbody tr:nth-child(even) { background:#f3f4fb !important; }
.dataframe tbody tr:hover          { background:#e8f0fe !important; }

/* ── TYPOGRAPHY ──────────────────────────────────────────────── */
.stMarkdown p, .stMarkdown li     { color:#2c3e50 !important; }
h1, h2, h3                        { color:#1a237e !important; }
.stCaption                        { color:#6c7a89 !important; }

/* ── ALERTS ──────────────────────────────────────────────────── */
.stAlert                          { border-radius:8px !important; }
.stInfo                           { background:#e3f2fd !important; border-left:4px solid #1565c0 !important; color:#0d47a1 !important; }
.stSuccess                        { background:#e8f5e9 !important; border-left:4px solid #2e7d32 !important; }
.stWarning                        { background:#fff8e1 !important; border-left:4px solid #f57f17 !important; }
.stError                          { background:#fce8e8 !important; border-left:4px solid #c62828 !important; }

/* ── FACTOR PILLS ────────────────────────────────────────────── */
.factor-pill                      { display:inline-block; background:#e8eaf6; border:1px solid #c5cae9; border-radius:20px; padding:3px 10px; font-size:0.72rem; color:#3949ab; font-weight:500; margin:2px; }

/* ── AI RESULT CARDS ─────────────────────────────────────────── */
.ai-card                          { background:#fff; border:1px solid #e0e4ec; border-radius:12px; padding:1.2rem; box-shadow:0 2px 8px rgba(0,0,0,.06); }
.ai-buy-call                      { border-left:4px solid #2e7d32 !important; background:linear-gradient(135deg,#f1f8f2,#fff) !important; }
.ai-buy-put                       { border-left:4px solid #c62828 !important; background:linear-gradient(135deg,#fdf3f3,#fff) !important; }
.ai-no-trade                      { border-left:4px solid #5c6bc0 !important; background:linear-gradient(135deg,#f3f4fb,#fff) !important; }
.ai-action                        { font-size:2rem; font-weight:800; letter-spacing:.05em; }

/* ── METRIC CARDS ────────────────────────────────────────────── */
.metric-card                      { background:#fff; border:1px solid #e0e4ec; border-radius:10px; padding:.9rem 1rem; box-shadow:0 1px 4px rgba(0,0,0,.05); }
.section-card                     { background:#fff; border:1px solid #e0e4ec; border-radius:10px; padding:.5rem 1rem; margin-bottom:.5rem; }
.section-title                    { font-size:.72rem; color:#5c6bc0; font-weight:600; text-transform:uppercase; letter-spacing:.05em; }

/* ── PRICE COLOURS ───────────────────────────────────────────── */
.up                               { color:#1b5e20 !important; }
.down                             { color:#7f1a1a !important; }

/* ── PROGRESS BAR ────────────────────────────────────────────── */
.stProgress>div>div               { background:#1565c0 !important; border-radius:4px !important; }

/* ── SCROLLBAR ───────────────────────────────────────────────── */
::-webkit-scrollbar               { width:6px; height:6px; }
::-webkit-scrollbar-track         { background:#f0f2f6; }
::-webkit-scrollbar-thumb         { background:#c5cae9; border-radius:3px; }
::-webkit-scrollbar-thumb:hover   { background:#9fa8da; }
</style>""", unsafe_allow_html=True)


def market_status_banner(mkt: dict) -> str:
    status = mkt.get("status","UNKNOWN"); ist = mkt.get("current_ist","")
    closes = mkt.get("closes_in_str","");  opens = mkt.get("opens_in_str","")
    date_s = mkt.get("date_str","")
    if mkt.get("is_open"):
        return (f'<div style="background:#e8f5e9;border:1px solid #81c784;border-radius:8px;'
                f'padding:10px 16px;color:#1b5e20;font-weight:600;display:flex;'
                f'justify-content:space-between;align-items:center">'
                f'<span>🟢 MARKET OPEN — Closes in {closes}</span>'
                f'<span style="font-size:0.85rem;font-weight:400">IST {ist} · {date_s}</span></div>')
    elif "PRE" in status.upper():
        return (f'<div style="background:#fff8e1;border:1px solid #ffe082;border-radius:8px;'
                f'padding:10px 16px;color:#7f5a00;font-weight:600;display:flex;'
                f'justify-content:space-between;align-items:center">'
                f'<span>🟡 PRE-MARKET — Opens in {opens}</span>'
                f'<span style="font-size:0.85rem;font-weight:400">IST {ist} · {date_s}</span></div>')
    else:
        return (f'<div style="background:#fce8e8;border:1px solid #ef9a9a;border-radius:8px;'
                f'padding:10px 16px;color:#7f1a1a;font-weight:600;display:flex;'
                f'justify-content:space-between;align-items:center">'
                f'<span>🔴 MARKET CLOSED</span>'
                f'<span style="font-size:0.85rem;font-weight:400">IST {ist} · {date_s} · Opens in {opens}</span></div>')
