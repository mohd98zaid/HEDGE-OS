# PROJECT HEDGE — Environment Variables & Credentials Setup Guide

This document lists every service that requires you to log in, obtain credentials, or set environment variables before the system can run in production.

---

## Quick Start (Dev Mode — No Credentials Needed)

For **local development**, `run.bat` works out of the box with built-in defaults:
- NATS uses dev passwords (`dev_hot_path`, `dev_warm_ai`, etc.)
- Postgres uses `hedge`/`hedge`
- Redis has no auth
- Broker adapters start in `ConfigError` state (no live trading)
- Ollama must be running locally but needs no API key

**For production/live trading, you MUST configure the items below.**

---

## 1. Broker Credentials (REQUIRED for live trading)

Each broker adapter needs credentials loaded via `/etc/hedge/config.yaml` or environment variables.

### Zerodha (Kite Connect)

| What to obtain | Where to get it | Config field |
|---|---|---|
| API Key | [Kite Connect Developer Portal](https://developers.kite.trade/) — create an app | `HEDGE_ZERODHA_API_KEY` |
| Access Token | Generated daily via Kite login flow (OAuth redirect) | `HEDGE_ZERODHA_ACCESS_TOKEN` |

**Login steps:**
1. Sign up at https://developers.kite.trade/
2. Create an app → get your `api_key` and `api_secret`
3. Each trading day, run the Kite login flow to get a fresh `access_token` (valid ~6am to ~6am next day)
4. Set the values in your environment or config file

### Dhan

| What to obtain | Where to get it | Config field |
|---|---|---|
| Client ID | [Dhan Developer Portal](https://dhanhq.co/docs/v2/) | `HEDGE_DHAN_CLIENT_ID` |
| Access Token | Generated via Dhan OAuth flow | `HEDGE_DHAN_ACCESS_TOKEN` |

**Login steps:**
1. Sign up at https://dhanhq.co/ and open a trading account
2. Go to API section → generate access token
3. Token is valid for the trading day

### Shoonya (Finvasia)

| What to obtain | Where to get it | Config field |
|---|---|---|
| User ID | Your Shoonya trading account user ID | `HEDGE_SHOONYA_USER_ID` |
| Password | Your Shoonya login password | `HEDGE_SHOONYA_PASSWORD` |
| TOTP Key | 2FA TOTP secret from Shoonya app settings | `HEDGE_SHOONYA_TOTP_KEY` |
| Vendor Code | API vendor code from Shoonya | `HEDGE_SHOONYA_VENDOR_CODE` |
| API Key | API key from Shoonya developer portal | `HEDGE_SHOONYA_API_KEY` |

**Login steps:**
1. Open a Shoonya/Finvasia trading account
2. Enable API access in account settings
3. Get your TOTP secret for programmatic 2FA
4. The adapter generates a session token at startup using these credentials

### Angel One (SmartAPI)

| What to obtain | Where to get it | Config field |
|---|---|---|
| API Key | [Angel One SmartAPI Portal](https://smartapi.angelone.in/) | `HEDGE_ANGELONE_API_KEY` |
| Client ID | Your Angel One trading account client ID | `HEDGE_ANGELONE_CLIENT_ID` |
| Password | Your Angel One login password | `HEDGE_ANGELONE_PASSWORD` |
| TOTP Key | 2FA TOTP secret from Angel One app | `HEDGE_ANGELONE_TOTP_KEY` |

**Login steps:**
1. Open an Angel One trading account
2. Register at https://smartapi.angelone.in/
3. Create an app → get `api_key`
4. Enable TOTP in your Angel One app for programmatic login

### Upstox (API v2)

| What to obtain | Where to get it | Config field |
|---|---|---|
| API Key | [Upstox Developer Portal](https://upstox.com/developer/) — create an app | `HEDGE_UPSTOX_API_KEY` |
| API Secret | Upstox Developer Portal — same app | `HEDGE_UPSTOX_API_SECRET` |
| Access Token | Generated daily via Upstox OAuth redirect flow | `HEDGE_UPSTOX_ACCESS_TOKEN` |

**Login steps:**
1. Sign up at https://upstox.com/ (open a trading account if you don't have one)
2. Go to https://upstox.com/developer/ → "Create an app"
3. Set a redirect URI (e.g. `http://localhost:8000/callback`)
4. Get your `api_key` and `api_secret` (one-time)
5. Each trading day, run the OAuth login redirect flow to mint a fresh `access_token`
   - Token expires at ~03:30 IST the following day
6. The adapter uses `access_token` as a Bearer token on every request

---

## 2. NATS Authentication (auto-configured for dev)

| Variable | Default (dev) | Production |
|---|---|---|
| `HEDGE_HOT_PATH_PASSWORD` | `dev_hot_path` | Set a strong password |
| `HEDGE_WARM_AI_PASSWORD` | `dev_warm_ai` | Set a strong password |
| `HEDGE_UI_GATEWAY_PASSWORD` | `dev_ui_gateway` | Set a strong password |
| `HEDGE_SUPERVISOR_PASSWORD` | `dev_supervisor` | Set a strong password |
| `HEDGE_OBS_COLLECTOR_PASSWORD` | `dev_obs_collector` | Set a strong password |
| `HEDGE_SYS_PASSWORD` | `dev_sys` | Set a strong password |

**For production:** Run `docker/nats/provision-creds.sh` to generate proper JWT-based credentials instead of passwords.

---

## 3. Database (auto-configured for dev)

### PostgreSQL + TimescaleDB

| Variable | Default | Notes |
|---|---|---|
| `HEDGE_POSTGRES_URL` | `postgresql://hedge:hedge@postgres:5432/hedge` | Full DSN |
| `HEDGE_POSTGRES_HOST` | `postgres` | Alternative: split fields |
| `HEDGE_POSTGRES_PORT` | `5432` | |
| `HEDGE_POSTGRES_DB` | `hedge` | |
| `HEDGE_POSTGRES_USER` | `hedge` | |
| `HEDGE_POSTGRES_PASSWORD` | `hedge` | **Change in production!** |

### Redis

| Variable | Default | Notes |
|---|---|---|
| `HEDGE_REDIS_URL` | `redis://redis:6379` | Add password for production: `redis://:password@host:6379` |

### Qdrant (Vector DB)

| Variable | Default | Notes |
|---|---|---|
| `HEDGE_QDRANT_URL` | `http://qdrant:6333` | |
| `HEDGE_QDRANT_API_KEY` | _(none)_ | Only needed for Qdrant Cloud deployments |

---

## 4. Ollama (Local LLM — no login needed)

Ollama runs locally and requires **no API key or login**. You just need it installed and running.

| What | How |
|---|---|
| Install Ollama | https://ollama.ai/download |
| Pull models | `ollama pull qwen2.5:14b` / `ollama pull mistral:7b` / `ollama pull deepseek-r1` / `ollama pull phi` |
| Start Ollama | `ollama serve` (runs on port 11434) |

| Variable | Default | Notes |
|---|---|---|
| `HEDGE_OLLAMA_URL` | `http://host.docker.internal:11434` | Points to your local Ollama |

**No cloud LLM API keys are needed.** The system is 100% local-first by design (R10.8).

---

## 5. Observability (auto-configured, no login needed)

These services run in Docker with no external credentials:

| Service | Port | Login |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `hedge` (change via `GF_SECURITY_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:9090 | No auth |
| Jaeger | http://localhost:16686 | No auth |
| Loki | http://localhost:3100 | No auth |

---

## 6. Market Data WebSocket Feeds

The Market_Data_Engine connects to NSE/BSE data feeds. These typically come through your broker's WebSocket API:

| Broker | WebSocket Source | Auth |
|---|---|---|
| Zerodha | Kite Ticker WebSocket | Uses same `api_key` + `access_token` from §1 |
| Dhan | Dhan WebSocket | Uses same `client_id` + `access_token` from §1 |
| Shoonya | Shoonya WebSocket | Uses session token from §1 login |
| Angel One | SmartAPI WebSocket | Uses session token from §1 login |
| Upstox | Upstox V2 WebSocket | Uses same `access_token` from §1 |

**No separate market data subscription is needed** — it's included with your broker account.

---

## 7. News Sources (Optional — for News_Intelligence_Engine)

These are optional and the system works without them (news scoring will be empty):

| Source | How to get access | Variable |
|---|---|---|
| Twitter/X API | [developer.twitter.com](https://developer.twitter.com/) | `HEDGE_TWITTER_BEARER_TOKEN` |
| Telegram Bot | [BotFather](https://t.me/BotFather) — create a bot | `HEDGE_TELEGRAM_BOT_TOKEN` |
| Reuters/Moneycontrol/ET | Public RSS feeds — no auth needed | _(none)_ |
| NSE Filings | Public — no auth needed | _(none)_ |
| RBI Announcements | Public — no auth needed | _(none)_ |

---

## 8. Complete `.env` File Template

Create a `.env` file in the project root for production:

```env
# ============================================================================
# PROJECT HEDGE — Production Environment Variables
# ============================================================================

# --- Broker Credentials (REQUIRED for live trading) ---
# Zerodha Kite Connect
HEDGE_ZERODHA_API_KEY=your_kite_api_key_here
HEDGE_ZERODHA_ACCESS_TOKEN=your_daily_access_token_here

# Dhan
HEDGE_DHAN_CLIENT_ID=your_dhan_client_id
HEDGE_DHAN_ACCESS_TOKEN=your_dhan_access_token

# Shoonya / Finvasia
HEDGE_SHOONYA_USER_ID=your_user_id
HEDGE_SHOONYA_PASSWORD=your_password
HEDGE_SHOONYA_TOTP_KEY=your_totp_secret
HEDGE_SHOONYA_VENDOR_CODE=your_vendor_code
HEDGE_SHOONYA_API_KEY=your_api_key

# Angel One SmartAPI
HEDGE_ANGELONE_API_KEY=your_smartapi_key
HEDGE_ANGELONE_CLIENT_ID=your_client_id
HEDGE_ANGELONE_PASSWORD=your_password
HEDGE_ANGELONE_TOTP_KEY=your_totp_secret

# Upstox API v2
HEDGE_UPSTOX_API_KEY=your_upstox_app_key
HEDGE_UPSTOX_API_SECRET=your_upstox_app_secret
HEDGE_UPSTOX_ACCESS_TOKEN=your_daily_access_token

# --- NATS Passwords (change from defaults!) ---
HEDGE_HOT_PATH_PASSWORD=change_me_strong_password_1
HEDGE_WARM_AI_PASSWORD=change_me_strong_password_2
HEDGE_UI_GATEWAY_PASSWORD=change_me_strong_password_3
HEDGE_SUPERVISOR_PASSWORD=change_me_strong_password_4
HEDGE_OBS_COLLECTOR_PASSWORD=change_me_strong_password_5
HEDGE_SYS_PASSWORD=change_me_strong_password_6

# --- Database ---
HEDGE_POSTGRES_PASSWORD=change_me_postgres_password

# --- Ollama (local, no key needed) ---
HEDGE_OLLAMA_URL=http://host.docker.internal:11434

# --- News Sources (optional) ---
HEDGE_TWITTER_BEARER_TOKEN=
HEDGE_TELEGRAM_BOT_TOKEN=

# --- Qdrant (only if using Qdrant Cloud) ---
HEDGE_QDRANT_API_KEY=

# --- Grafana ---
GF_SECURITY_ADMIN_PASSWORD=change_me_grafana_password
```

---

## Summary: What Needs a Login

| Service | Login Required? | Effort | When Needed |
|---|---|---|---|
| **Zerodha** | ✅ Yes — daily token refresh | Sign up + daily OAuth | Live trading with Zerodha |
| **Dhan** | ✅ Yes — daily token refresh | Sign up + daily OAuth | Live trading with Dhan |
| **Shoonya** | ✅ Yes — TOTP-based | Sign up + enable API | Live trading with Shoonya |
| **Angel One** | ✅ Yes — TOTP-based | Sign up + enable API | Live trading with Angel One |
| **Upstox** | ✅ Yes — daily token refresh | Sign up + daily OAuth | Live trading with Upstox |
| **Ollama** | ❌ No login | Just install + pull models | Always (AI inference) |
| **NATS** | ❌ Auto (dev passwords) | Change passwords for prod | Always |
| **PostgreSQL** | ❌ Auto (hedge/hedge) | Change password for prod | Always |
| **Redis** | ❌ No auth (dev) | Add password for prod | Always |
| **Qdrant** | ❌ No auth (local) | API key only for cloud | Always |
| **Grafana** | ❌ Auto (admin/hedge) | Change password for prod | Monitoring |
| **Twitter/X** | ⚡ Optional | Developer account | News scoring |
| **Telegram** | ⚡ Optional | Create bot via BotFather | News scoring |
| **News RSS** | ❌ No auth | Public feeds | News scoring |

---

## Minimum Setup for First Run (Dev/Paper Trading)

1. Install Docker Desktop
2. Install Node.js 18+
3. Install Ollama → `ollama pull qwen2.5:14b && ollama pull mistral:7b`
4. Run `ollama serve`
5. Double-click `run.bat`

That's it. The system starts with simulated broker (no live orders) and all AI running locally.

---

## Minimum Setup for Live Trading

1. Everything from dev setup above
2. Open a trading account with at least one broker (Zerodha recommended as primary)
3. Get API credentials from your broker's developer portal
4. Create `.env` file with your broker credentials (see template above)
5. Change all default passwords in `.env`
6. Run `run.bat`

The system will connect to your broker and the Risk_Engine will enforce all configured limits before any order is placed.
