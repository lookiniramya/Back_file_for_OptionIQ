# 📊 OptionsIQ — Paytm Money Options Intelligence Platform

A production-grade, AI-ready options trading assistant built with Streamlit and Paytm Money APIs.

---

## 🗂️ Project Structure

```
options_trader/
├── app.py              # Main Streamlit entry point
├── auth.py             # Authentication & token validation
├── api_client.py       # Paytm Money API calls + demo data
├── analytics.py        # Core analytics engine (PCR, OI, signals, etc.)
├── ui_components.py    # All UI components and CSS
├── requirements.txt    # Python dependencies
└── README.md
```

---

## ⚙️ Setup & Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the app
```bash
streamlit run app.py
```

### 3. Authentication
- **Demo Mode**: Click "Demo Mode" in the sidebar — no token needed. Uses realistic synthetic NIFTY data.
- **Live Mode**: Paste your Paytm Money `access_token` (obtained via OAuth flow) into the sidebar.

---

## 🔌 API Endpoints Used

| API | Endpoint | Purpose |
|-----|----------|---------|
| Config | `GET /fno/v1/option-chain/config` | Fetch expiry dates |
| Option Chain | `GET /fno/v1/option-chain` | Fetch CALL / PUT data |

**Headers required**: `x-jwt-token: <access_token>`

---

## 📊 Features

| Module | Description |
|--------|-------------|
| Market Snapshot | Spot price, trend, volume bias, OI summary |
| PCR Analysis | OI & volume PCR with gauge |
| Support & Resistance | Highest OI PUT = support, highest OI CALL = resistance |
| Option Chain Table | Combined + side-by-side with ATM highlighting |
| OI Buildup | Long/Short buildup, covering, unwinding classification |
| Smart Money | Z-score anomaly detection on OI × Volume |
| Trade Decision Engine | Rule-based 100-point scoring system |

---

## 🤖 Future AI Integration

The `generate_trade_decision()` function in `analytics.py` is designed to be a drop-in replacement with a GPT/Claude API call. Simply replace the rule-based scoring with an LLM prompt that receives the same structured data dict.

---

## ⚠️ Disclaimer

This software is for **educational purposes only**. Options trading involves significant risk of capital loss. Always consult a SEBI-registered investment advisor.
