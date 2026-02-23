# Setup Guide (Simulation First, Then Live)

This guide is a practical checklist to get the bot running safely.

## 1) System requirements

- Python **3.14+**
- Redis running locally or remotely
- Polymarket API credentials (only required for live)

## 2) Clone and create virtual environment

```bash
git clone <your-repo-url>
cd Polymarket-BTC-15-Minute-Trading-Bot
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

## 3) Install dependencies

```bash
pip install -r requirements.txt
# optional editable install
pip install -e .
```

## 4) Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set at least:

- Redis:
  - `REDIS_HOST`
  - `REDIS_PORT`
  - `REDIS_DB`
- Trading symbols and strategy knobs:
  - `TRADE_SYMBOLS`
  - `MAX_SPREAD_PCT`
  - `MAX_SLIPPAGE_PCT`
  - `MIN_FUSION_SCORE`
  - `MIN_FUSION_CONFIDENCE`

For **simulation only**, credentials can remain empty.

For **live**, you must set:

- `POLYMARKET_PK`
- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_PASSPHRASE`
- `LIVE_TRADING_ENABLED=YES_I_UNDERSTAND`

## 5) Start Redis

```bash
redis-server
```

If Redis is remote, verify connectivity from your machine/container first.

## 6) Safety controls (recommended before first run)

Set these in `.env`:

- `DISABLE_LIVE_ORDERS=1` → hard kill switch (blocks all live orders)
- `MAX_ORDERS_PER_HOUR=...` → hourly cap (0 disables)
- `MAX_DAILY_NOTIONAL=...` → daily USD notional cap (0 disables)

Recommended first live dry-start values:

```env
DISABLE_LIVE_ORDERS=1
MAX_ORDERS_PER_HOUR=2
MAX_DAILY_NOTIONAL=5
```

## 7) Run in simulation first

```bash
python run_bot.py --test-mode
# or normal cadence
python run_bot.py
```

What to watch:

- logs show selected condition and UP/DOWN paired instruments
- `decision_audit.jsonl` is being written
- `paper_trades.json` is being updated

## 8) Enable metrics (optional)

By default Grafana exporter starts with the bot unless disabled.

Disable metrics if needed:

```bash
python run_bot.py --no-grafana
```

## 9) Live enable sequence (when ready)

1. Keep hard block on initially:
   - `DISABLE_LIVE_ORDERS=1`
2. Start live process with confirmations:

```bash
python run_bot.py --live --confirm-live
```

3. Verify logs, instrument selection, and skip reasons.
4. When satisfied, remove hard block:
   - `DISABLE_LIVE_ORDERS=0`
5. Keep hourly/daily caps active.

## 10) Troubleshooting quick checks

- Syntax/import check:

```bash
python -m py_compile run_bot.py
```

- Confirm safety env loaded (from logs):
  - live blocked due to `DISABLE_LIVE_ORDERS=1`
  - live blocked due to `MAX_ORDERS_PER_HOUR` / `MAX_DAILY_NOTIONAL`

- If no trades occur:
  - confirm enough quote history has accumulated
  - confirm paired UP/DOWN condition selection is logged
  - verify spread/confidence thresholds are not too strict

## 11) Suggested setup-day checklist

- [ ] Simulation run for at least 1 hour
- [ ] `decision_audit.jsonl` reviewed
- [ ] `paper_trades.json` updated with closes
- [ ] Redis stable
- [ ] Live credentials validated
- [ ] Start live with `DISABLE_LIVE_ORDERS=1` first
- [ ] Remove kill switch only after observing stable behavior
