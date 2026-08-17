# Dip-Buy Bot (Binance)

Watches a list of coins, buys on a dip, sells on a bounce. Runs on **Binance Testnet** by default — fake money, real live prices — so you can watch it work before risking anything real.

## Strategy (v3 — multi-position, volume-confirmed, trailing stop)
1. Track rolling high price per symbol over `ROLLING_WINDOW` checks
2. If price drops `DIP_THRESHOLD_PCT`% from that high, it's a *candidate* dip — not an automatic buy
3. Before buying, it checks (all must pass):
   - **RSI** — RSI below `RSI_OVERSOLD` (default 35), confirming real oversold conditions
   - **Trend filter** — price at/above its `TREND_MA_PERIOD`-candle moving average, avoiding downtrend knife-catches
   - **Volume confirmation** (v3) — the dip candle's volume is at least `VOLUME_MULTIPLIER`x its recent average, filtering out thin, low-liquidity noise dips that aren't backed by real trading activity
   - **Capacity check** (v3) — only buys if fewer than `MAX_CONCURRENT_POSITIONS` are currently open, so the bot spreads risk across coins instead of going all-in on one
4. Once bought, each position is managed independently:
   - **Stop-loss** — sells if price falls `STOP_LOSS_PCT` below buy price
   - **Trailing stop** (v3) — once gain exceeds `TRAILING_ACTIVATION_PCT`, the stop trails `TRAILING_STOP_PCT` behind the highest price seen, so a real bounce isn't cut short at the first target, but a reversal still locks in gains
   - **Max hold** — force-closes after `MAX_HOLD_HOURS` regardless
5. Every decision (buys, sells, and *why* dips were skipped) is logged, and every trade is written to `state.json`

**Still true:** more filters and smarter exits improve trade *quality*, they don't manufacture guaranteed profit. Backtest and testnet-run any threshold changes before trusting them with real money.

## Setup (local test first)

1. Get **testnet** API keys: https://testnet.binance.vision/ (log in with GitHub, generate HMAC key)
2. Copy `.env.example` to `.env` and fill in your testnet keys
3. Install deps and run:
   ```bash
   pip install -r requirements.txt
   export $(cat .env | xargs)
   python bot.py
   ```
4. Watch the logs. It'll print every price check reasoning and every buy/sell.

## Dashboard

The bot serves a live dashboard at its root URL (`/`) — same URL you point UptimeRobot at. Shows:
- Realized P/L, win rate, closed trade count, open positions, loop count, uptime
- Every open position (buy price, current peak, whether trailing stop is active)
- Full trade history (buys/sells with reason and profit/loss)
- Live log feed (last 200 lines), including every skipped-dip reason

It auto-refreshes every 10 seconds. On Render, visit `https://your-app-name.onrender.com` in a browser to see it.

Raw JSON is also available if you want to build something else on top of it:
- `/api/status` — summary stats
- `/api/positions` — current open positions
- `/api/trades` — full trade log
- `/api/logs` — recent log lines

## Deploy to Render (free, no card required) + keep-alive with UptimeRobot

Render's free tier spins down web services after ~15 min of no HTTP traffic. The bot now runs a tiny health-check endpoint alongside the trading loop specifically so Render treats it as a live web service — UptimeRobot pings that endpoint regularly to keep it awake.

**1. Push this folder to a GitHub repo** (public or private, either works)

**2. Deploy on Render**
1. Go to https://render.com and sign up (no card needed for free tier)
2. New → Web Service → connect your GitHub repo
3. Render auto-detects the Dockerfile
4. Instance type: **Free**
5. Add all environment variables from `.env.example` in the Environment tab (including your Binance keys)
6. Deploy — Render builds the Docker image and starts the bot

**3. Get your service URL**
Once deployed, Render gives you a URL like `https://your-app-name.onrender.com`. Test it:
```bash
curl https://your-app-name.onrender.com
```
Should return JSON like `{"status": "alive", "loops_completed": 3, ...}` — that's the bot's health endpoint, separate from its trading logic.

**4. Set up UptimeRobot to keep it awake**
1. Go to https://uptimerobot.com, sign up free
2. Add New Monitor → HTTP(s)
3. URL: your Render service URL
4. Monitoring interval: **5 minutes** (Render free tier sleeps after ~15 min idle, so 5-min pings keep it well within that window)
5. Save

Now UptimeRobot pings your bot every 5 minutes, Render sees traffic and stays awake, and the bot's actual trading loop (checking prices every `CHECK_INTERVAL_SEC`) keeps running continuously in the background regardless of the pings.

**Reality check on uptime:** this keeps the *process* alive far more reliably than running on a phone, but free-tier Render can still have occasional restarts/redeploys. Check `state.json`'s trade_log after your test week — gaps in timestamps tell you if it went down at any point.

## Going live (real money)

**Do not rush this.** Watch it run on testnet for at least a few days first and read `state.json` trade_log to see if the strategy nets positive or negative over time at your chosen thresholds.

When ready:
1. Set `USE_TESTNET=false`
2. Use real Binance API keys (Account → API Management) with **only Spot Trading enabled, withdrawals disabled**
3. Start with `POSITION_SIZE_USDT` at the exchange minimum (~$5-10) — don't scale up until you trust it

## Testing with $1 for a week

Real Binance order minimums are ~$5-10, so a literal $1 real-money position will always get rejected — the bot checks this and skips the trade rather than erroring out. To actually watch a week of decisions with small numbers, keep `USE_TESTNET=true` and set `POSITION_SIZE_USDT=1` — testnet uses fake funds so the $1 framing works there without hitting real minimums. Watch `state.json`'s `trade_log` and the skip reasons in the logs (RSI too high, downtrend, etc.) to see the filters actually working before ever touching real funds.


## Important limits to know

- Binance minimum order size is typically $5-10 notional depending on the pair — a $1 position size will get rejected, the bot checks this and skips
- This strategy is NOT guaranteed to profit daily — some days it'll lose, some days it'll gain. Nothing legitimate guarantees fixed daily returns on this little capital
- Fees (0.1% per trade on spot) eat into thin margins — tighter thresholds trade more often but bleed more to fees; wider thresholds trade less but each trade matters more

## Tuning

Adjust in `.env`:
- Tighter `DIP_THRESHOLD_PCT`/`SELL_TARGET_PCT` (e.g. 1%/1.2%) = more trades, thinner margins, more fee drag
- Wider (e.g. 3%/4%) = fewer trades, bigger swings needed, less fee drag
- `WATCHLIST` — more coins = more chances to catch a dip, but also more noise
