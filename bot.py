"""
Dip-Buy Bot for Binance — v3
Adds on top of v2 (RSI filter, trend filter, stop-loss, max-hold):
  - Multiple concurrent positions (up to MAX_CONCURRENT_POSITIONS), so the bot
    doesn't sit idle watching other dips while holding one coin.
  - Volume confirmation — only buys a dip if recent volume is above its average,
    filtering out low-liquidity noise dips.
  - Trailing stop on winners — once a position is up enough, the stop trails
    the price up instead of selling the instant it touches the fixed target,
    letting real bounces run a bit further.

Runs on Binance TESTNET by default (fake money, real market data).
Still no guarantee of profit — these are risk/quality filters, not a money machine.
"""

import os
import json
import time
import logging
import threading
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ---------------- CONFIG ----------------
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
USE_TESTNET = os.getenv("USE_TESTNET", "true").lower() == "true"

WATCHLIST = os.getenv("WATCHLIST", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,DOGEUSDT").split(",")
DIP_THRESHOLD_PCT = float(os.getenv("DIP_THRESHOLD_PCT", "2.0"))
SELL_TARGET_PCT = float(os.getenv("SELL_TARGET_PCT", "2.5"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3.0"))
MAX_HOLD_HOURS = float(os.getenv("MAX_HOLD_HOURS", "24"))
POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", "10"))
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "20"))
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))

RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "35"))
TREND_MA_PERIOD = int(os.getenv("TREND_MA_PERIOD", "50"))
TREND_FILTER_ENABLED = os.getenv("TREND_FILTER_ENABLED", "true").lower() == "true"
KLINE_INTERVAL = os.getenv("KLINE_INTERVAL", "15m")

# --- v3 additions ---
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))
VOLUME_FILTER_ENABLED = os.getenv("VOLUME_FILTER_ENABLED", "true").lower() == "true"
VOLUME_MA_PERIOD = int(os.getenv("VOLUME_MA_PERIOD", "20"))
VOLUME_MULTIPLIER = float(os.getenv("VOLUME_MULTIPLIER", "1.2"))     # recent volume must be >= this x its average
TRAILING_STOP_ENABLED = os.getenv("TRAILING_STOP_ENABLED", "true").lower() == "true"
TRAILING_ACTIVATION_PCT = float(os.getenv("TRAILING_ACTIVATION_PCT", "1.5"))  # start trailing once gain exceeds this %
TRAILING_STOP_PCT = float(os.getenv("TRAILING_STOP_PCT", "1.0"))     # trail this % behind the peak once active

STATE_FILE = "state.json"
PORT = int(os.getenv("PORT", "10000"))  # Render sets $PORT automatically
DASHBOARD_LOG_BUFFER = int(os.getenv("DASHBOARD_LOG_BUFFER", "200"))  # how many recent log lines the dashboard keeps

# Shared status the health endpoint reports, updated by the bot loop
_status = {"started_at": datetime.utcnow().isoformat(), "last_check": None, "loops": 0}

# In-memory ring buffer of recent log lines, for the dashboard's live log view
_log_buffer = deque(maxlen=DASHBOARD_LOG_BUFFER)

class BufferHandler(logging.Handler):
    """Captures log records into an in-memory buffer the dashboard can read."""
    def emit(self, record):
        _log_buffer.append({
            "time": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": self.format(record),
        })

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dipbot")
_buffer_handler = BufferHandler()
_buffer_handler.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(_buffer_handler)

# ---------------- DASHBOARD SERVER (health check + profit/logs UI, for Render + UptimeRobot) ----------------
# The main loop keeps this updated so the HTTP handlers (running in their own thread)
# always see current data without needing to touch the exchange themselves.
_shared_state = {"positions": {}, "trade_log": []}

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dip Bot Dashboard</title>
<style>
  * { box-sizing: border-box; }
  body { margin:0; font-family: -apple-system, Segoe UI, Roboto, sans-serif; background:#0b0f14; color:#e6edf3; }
  header { padding:16px 20px; border-bottom:1px solid #1f2937; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; }
  header h1 { font-size:18px; margin:0; }
  .badge { padding:4px 10px; border-radius:999px; font-size:12px; font-weight:600; }
  .badge.live { background:#0d3b23; color:#3fb950; }
  .badge.testnet { background:#3b2d0d; color:#e3b341; }
  main { padding:20px; max-width:1000px; margin:0 auto; }
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; margin-bottom:24px; }
  .card { background:#111827; border:1px solid #1f2937; border-radius:10px; padding:14px; }
  .card .label { font-size:12px; color:#8b949e; margin-bottom:6px; }
  .card .value { font-size:22px; font-weight:700; }
  .value.pos { color:#3fb950; }
  .value.neg { color:#f85149; }
  section { margin-bottom:28px; }
  section h2 { font-size:14px; text-transform:uppercase; letter-spacing:.05em; color:#8b949e; margin-bottom:10px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:8px 10px; border-bottom:1px solid #1f2937; }
  th { color:#8b949e; font-weight:600; }
  tr:hover { background:#111827; }
  .tag { padding:2px 8px; border-radius:6px; font-size:11px; font-weight:600; }
  .tag.buy { background:#0d3b23; color:#3fb950; }
  .tag.sell-win { background:#0d3b23; color:#3fb950; }
  .tag.sell-loss { background:#3b0d0d; color:#f85149; }
  #logs { background:#0d1117; border:1px solid #1f2937; border-radius:10px; padding:12px; max-height:340px; overflow-y:auto; font-family: ui-monospace, monospace; font-size:12px; line-height:1.6; }
  .log-line { white-space:pre-wrap; word-break:break-word; }
  .log-ERROR { color:#f85149; }
  .log-WARNING { color:#e3b341; }
  .log-INFO { color:#c9d1d9; }
  .empty { color:#8b949e; font-size:13px; padding:10px 0; }
  footer { text-align:center; color:#8b949e; font-size:12px; padding:20px; }
</style>
</head>
<body>
<header>
  <h1>🤖 Dip Bot Dashboard</h1>
  <span id="mode-badge" class="badge testnet">loading...</span>
</header>
<main>
  <div class="stats">
    <div class="card"><div class="label">Realized P/L</div><div class="value" id="pl">--</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="winrate">--</div></div>
    <div class="card"><div class="label">Closed Trades</div><div class="value" id="closed">--</div></div>
    <div class="card"><div class="label">Open Positions</div><div class="value" id="open">--</div></div>
    <div class="card"><div class="label">Loops Run</div><div class="value" id="loops">--</div></div>
    <div class="card"><div class="label">Uptime Since</div><div class="value" id="since" style="font-size:13px;">--</div></div>
  </div>

  <section>
    <h2>Open Positions</h2>
    <table id="positions-table">
      <thead><tr><th>Symbol</th><th>Buy Price</th><th>Current Peak</th><th>Opened</th><th>Trailing</th></tr></thead>
      <tbody id="positions-body"><tr><td colspan="5" class="empty">Loading...</td></tr></tbody>
    </table>
  </section>

  <section>
    <h2>Trade History (most recent first)</h2>
    <table id="trades-table">
      <thead><tr><th>Time</th><th>Action</th><th>Symbol</th><th>Price</th><th>P/L</th><th>Reason</th></tr></thead>
      <tbody id="trades-body"><tr><td colspan="6" class="empty">Loading...</td></tr></tbody>
    </table>
  </section>

  <section>
    <h2>Live Logs</h2>
    <div id="logs"><div class="empty">Loading...</div></div>
  </section>
</main>
<footer>Auto-refreshes every 10s &middot; Dip-Buy Bot v3</footer>

<script>
function fmt(n, d=4) { return (typeof n === 'number') ? n.toFixed(d) : n; }

async function refresh() {
  try {
    const [statusRes, posRes, tradesRes, logsRes] = await Promise.all([
      fetch('/api/status'), fetch('/api/positions'), fetch('/api/trades'), fetch('/api/logs')
    ]);
    const status = await statusRes.json();
    const positions = await posRes.json();
    const trades = await tradesRes.json();
    const logs = await logsRes.json();

    const badge = document.getElementById('mode-badge');
    badge.textContent = status.testnet ? 'TESTNET' : 'LIVE';
    badge.className = 'badge ' + (status.testnet ? 'testnet' : 'live');

    const plEl = document.getElementById('pl');
    plEl.textContent = '$' + fmt(status.total_profit);
    plEl.className = 'value ' + (status.total_profit >= 0 ? 'pos' : 'neg');

    document.getElementById('winrate').textContent = fmt(status.win_rate, 1) + '%';
    document.getElementById('closed').textContent = status.closed_trades;
    document.getElementById('open').textContent = status.open_positions + ' / ' + status.max_positions;
    document.getElementById('loops').textContent = status.loops_completed;
    document.getElementById('since').textContent = new Date(status.started_at).toLocaleString();

    const posBody = document.getElementById('positions-body');
    posBody.innerHTML = '';
    const posEntries = Object.entries(positions);
    if (posEntries.length === 0) {
      posBody.innerHTML = '<tr><td colspan="5" class="empty">No open positions</td></tr>';
    } else {
      for (const [symbol, pos] of posEntries) {
        posBody.innerHTML += `<tr>
          <td>${symbol}</td>
          <td>$${fmt(pos.buy_price)}</td>
          <td>$${fmt(pos.peak_price)}</td>
          <td>${new Date(pos.timestamp).toLocaleString()}</td>
          <td>${pos.trailing_active ? '✅ active' : '—'}</td>
        </tr>`;
      }
    }

    const tradesBody = document.getElementById('trades-body');
    tradesBody.innerHTML = '';
    if (trades.length === 0) {
      tradesBody.innerHTML = '<tr><td colspan="6" class="empty">No trades yet</td></tr>';
    } else {
      for (const t of trades.slice().reverse()) {
        const isSell = t.action === 'SELL';
        const profit = isSell ? t.profit : null;
        const tagClass = t.action === 'BUY' ? 'buy' : (profit >= 0 ? 'sell-win' : 'sell-loss');
        tradesBody.innerHTML += `<tr>
          <td>${new Date(t.time).toLocaleString()}</td>
          <td><span class="tag ${tagClass}">${t.action}</span></td>
          <td>${t.symbol}</td>
          <td>$${fmt(t.price)}</td>
          <td>${profit !== null ? '$' + fmt(profit) : '—'}</td>
          <td>${t.reason || '—'}</td>
        </tr>`;
      }
    }

    const logsEl = document.getElementById('logs');
    if (logs.length === 0) {
      logsEl.innerHTML = '<div class="empty">No logs yet</div>';
    } else {
      logsEl.innerHTML = logs.slice().reverse().map(l =>
        `<div class="log-line log-${l.level}">[${new Date(l.time).toLocaleTimeString()}] ${l.message}</div>`
      ).join('');
    }
  } catch (e) {
    console.error('Dashboard refresh failed', e);
  }
}
refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>"""

class DashboardHandler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    def _html(self, html, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/" or path == "/dashboard":
            self._html(DASHBOARD_HTML)
            return

        if path == "/api/status":
            sells = [t for t in _shared_state["trade_log"] if t["action"] == "SELL"]
            total_profit = sum(t.get("profit", 0) for t in sells)
            wins = sum(1 for t in sells if t.get("profit", 0) > 0)
            win_rate = (wins / len(sells) * 100) if sells else 0
            self._json({
                "status": "alive",
                "testnet": USE_TESTNET,
                "started_at": _status["started_at"],
                "last_check": _status["last_check"],
                "loops_completed": _status["loops"],
                "open_positions": len(_shared_state["positions"]),
                "max_positions": MAX_CONCURRENT_POSITIONS,
                "closed_trades": len(sells),
                "win_rate": win_rate,
                "total_profit": total_profit,
            })
            return

        if path == "/api/positions":
            self._json(_shared_state["positions"])
            return

        if path == "/api/trades":
            self._json(_shared_state["trade_log"])
            return

        if path == "/api/logs":
            self._json(list(_log_buffer))
            return

        self._json({"error": "not found"}, code=404)

    def log_message(self, format, *args):
        pass  # silence default request logging, bot.py logging covers it

def start_dashboard_server():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    log.info(f"Dashboard + health server listening on port {PORT} — visit / for the UI, point UptimeRobot at / too")
    server.serve_forever()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"positions": {}, "price_history": {}, "trade_log": []}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def get_client():
    if not API_KEY or not API_SECRET:
        log.error("Missing BINANCE_API_KEY / BINANCE_API_SECRET env vars.")
        raise SystemExit(1)
    client = Client(API_KEY, API_SECRET, testnet=USE_TESTNET)
    log.info(f"Connected to Binance {'TESTNET' if USE_TESTNET else 'LIVE'}.")
    return client

def get_klines(client, symbol, interval, limit):
    try:
        raw = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        return raw
    except BinanceAPIException as e:
        log.error(f"Kline fetch failed for {symbol}: {e}")
        return []

def closes_from_klines(raw):
    return [float(k[4]) for k in raw]

def volumes_from_klines(raw):
    return [float(k[5]) for k in raw]

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def is_uptrend_or_neutral(client, symbol):
    if not TREND_FILTER_ENABLED:
        return True
    raw = get_klines(client, symbol, KLINE_INTERVAL, TREND_MA_PERIOD + 5)
    closes = closes_from_klines(raw)
    ma = calc_sma(closes, TREND_MA_PERIOD)
    if ma is None or not closes:
        return True
    return closes[-1] >= ma

def get_rsi(client, symbol):
    raw = get_klines(client, symbol, KLINE_INTERVAL, RSI_PERIOD + 10)
    closes = closes_from_klines(raw)
    return calc_rsi(closes, RSI_PERIOD)

def has_volume_confirmation(client, symbol):
    """Only confirm a dip if the most recent candle's volume is meaningfully above its recent average."""
    if not VOLUME_FILTER_ENABLED:
        return True
    raw = get_klines(client, symbol, KLINE_INTERVAL, VOLUME_MA_PERIOD + 2)
    volumes = volumes_from_klines(raw)
    if len(volumes) < VOLUME_MA_PERIOD + 1:
        return True  # not enough data yet, don't block on it
    avg_vol = calc_sma(volumes[:-1], VOLUME_MA_PERIOD)
    latest_vol = volumes[-1]
    if avg_vol is None or avg_vol == 0:
        return True
    return latest_vol >= avg_vol * VOLUME_MULTIPLIER

def update_price_history(state, symbol, price):
    hist = state["price_history"].setdefault(symbol, [])
    hist.append(price)
    if len(hist) > ROLLING_WINDOW:
        hist.pop(0)

def rolling_high(state, symbol):
    hist = state["price_history"].get(symbol, [])
    return max(hist) if hist else None

def get_min_notional(client, symbol):
    info = client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"] in ("NOTIONAL", "MIN_NOTIONAL"):
            return float(f.get("minNotional", 5))
    return 5.0

def place_buy(client, state, symbol, price, reason):
    qty_usdt = POSITION_SIZE_USDT
    min_notional = get_min_notional(client, symbol)
    if qty_usdt < min_notional:
        log.warning(f"{symbol}: position size ${qty_usdt} below exchange minimum ${min_notional}. Skipping buy.")
        return
    quantity = round(qty_usdt / price, 6)
    try:
        client.order_market_buy(symbol=symbol, quoteOrderQty=qty_usdt)
        state["positions"][symbol] = {
            "buy_price": price,
            "qty": quantity,
            "timestamp": datetime.utcnow().isoformat(),
            "peak_price": price,
            "trailing_active": False,
        }
        log.info(f"BUY {symbol} @ {price} (${qty_usdt}) | reason: {reason} | open positions now: {len(state['positions'])}")
        state["trade_log"].append({"action": "BUY", "symbol": symbol, "price": price, "reason": reason, "time": datetime.utcnow().isoformat()})
    except BinanceAPIException as e:
        log.error(f"Buy failed for {symbol}: {e}")

def place_sell(client, state, symbol, price, reason):
    pos = state["positions"].get(symbol)
    if not pos:
        return
    try:
        client.order_market_sell(symbol=symbol, quantity=pos["qty"])
        profit = (price - pos["buy_price"]) * pos["qty"]
        log.info(f"SELL {symbol} @ {price} | profit: ${profit:.4f} | reason: {reason}")
        state["trade_log"].append({"action": "SELL", "symbol": symbol, "price": price, "profit": profit, "reason": reason, "time": datetime.utcnow().isoformat()})
        del state["positions"][symbol]
    except BinanceAPIException as e:
        log.error(f"Sell failed for {symbol}: {e}")

def manage_open_position(client, state, symbol, price):
    pos = state["positions"][symbol]
    buy_price = pos["buy_price"]
    stop_price = buy_price * (1 - STOP_LOSS_PCT / 100)
    held_since = datetime.fromisoformat(pos["timestamp"])
    held_hours = (datetime.utcnow() - held_since).total_seconds() / 3600

    # Track peak price for trailing stop
    if price > pos.get("peak_price", buy_price):
        pos["peak_price"] = price

    gain_pct = (price - buy_price) / buy_price * 100

    # Trailing stop logic: once gain exceeds activation threshold, trail behind the peak
    if TRAILING_STOP_ENABLED:
        if not pos.get("trailing_active") and gain_pct >= TRAILING_ACTIVATION_PCT:
            pos["trailing_active"] = True
            log.info(f"{symbol}: trailing stop activated at {gain_pct:.2f}% gain (peak {pos['peak_price']})")

        if pos.get("trailing_active"):
            trail_stop_price = pos["peak_price"] * (1 - TRAILING_STOP_PCT / 100)
            if price <= trail_stop_price:
                place_sell(client, state, symbol, price, f"trailing stop (peak {pos['peak_price']:.4f})")
                return
    else:
        # No trailing stop: use the fixed sell target instead
        sell_target = buy_price * (1 + SELL_TARGET_PCT / 100)
        if price >= sell_target:
            place_sell(client, state, symbol, price, "target hit")
            return

    if price <= stop_price:
        place_sell(client, state, symbol, price, "stop-loss")
        return
    if held_hours >= MAX_HOLD_HOURS:
        place_sell(client, state, symbol, price, "max hold time exceeded")
        return

def look_for_entry(client, state, symbol, price):
    if len(state["positions"]) >= MAX_CONCURRENT_POSITIONS:
        return  # capacity full, skip scanning for new entries

    high = rolling_high(state, symbol)
    if not high:
        return
    drop_pct = (high - price) / high * 100
    if drop_pct < DIP_THRESHOLD_PCT:
        return

    rsi = get_rsi(client, symbol)
    if rsi is None:
        log.info(f"{symbol}: dip detected ({drop_pct:.2f}%) but not enough kline data for RSI yet. Skipping.")
        return
    if rsi > RSI_OVERSOLD:
        log.info(f"{symbol}: dip detected ({drop_pct:.2f}%) but RSI {rsi:.1f} not oversold enough (< {RSI_OVERSOLD}). Skipping.")
        return
    if not is_uptrend_or_neutral(client, symbol):
        log.info(f"{symbol}: dip detected but price below trend MA ({TREND_MA_PERIOD}-period) — likely downtrend. Skipping.")
        return
    if not has_volume_confirmation(client, symbol):
        log.info(f"{symbol}: dip detected but volume not confirming (below {VOLUME_MULTIPLIER}x average). Skipping.")
        return

    place_buy(client, state, symbol, price, f"dip {drop_pct:.2f}% + RSI {rsi:.1f} + volume confirmed")

def check_symbol(client, state, symbol):
    try:
        ticker = client.get_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
    except BinanceAPIException as e:
        log.error(f"Price fetch failed for {symbol}: {e}")
        return

    update_price_history(state, symbol, price)

    if symbol in state["positions"]:
        manage_open_position(client, state, symbol, price)
    else:
        look_for_entry(client, state, symbol, price)

def summary(state):
    sells = [t for t in state["trade_log"] if t["action"] == "SELL"]
    total_profit = sum(t.get("profit", 0) for t in sells)
    wins = sum(1 for t in sells if t.get("profit", 0) > 0)
    win_rate = (wins / len(sells) * 100) if sells else 0
    log.info(
        f"--- Summary: {len(sells)} closed trades | win rate: {win_rate:.1f}% | "
        f"open positions: {len(state['positions'])}/{MAX_CONCURRENT_POSITIONS} | realized P/L: ${total_profit:.4f} ---"
    )

def main():
    client = get_client()
    state = load_state()

    # Backfill new position fields for any positions saved by an older bot version
    for sym, pos in state["positions"].items():
        pos.setdefault("peak_price", pos["buy_price"])
        pos.setdefault("trailing_active", False)

    _shared_state["positions"] = state["positions"]
    _shared_state["trade_log"] = state["trade_log"]

    # Start dashboard server in background thread so Render sees this as a live web service
    threading.Thread(target=start_dashboard_server, daemon=True).start()

    log.info(f"Watching: {WATCHLIST}")
    log.info(
        f"Dip: {DIP_THRESHOLD_PCT}% | Stop-loss: {STOP_LOSS_PCT}% | Max hold: {MAX_HOLD_HOURS}h | "
        f"RSI oversold: <{RSI_OVERSOLD} | Trend filter: {TREND_FILTER_ENABLED} | Volume filter: {VOLUME_FILTER_ENABLED} "
        f"({VOLUME_MULTIPLIER}x) | Trailing stop: {TRAILING_STOP_ENABLED} (activate {TRAILING_ACTIVATION_PCT}%, "
        f"trail {TRAILING_STOP_PCT}%) | Max concurrent positions: {MAX_CONCURRENT_POSITIONS} | Position size: ${POSITION_SIZE_USDT}"
    )

    loop_count = 0
    while True:
        for symbol in WATCHLIST:
            check_symbol(client, state, symbol)
        save_state(state)

        # Keep the dashboard's view of state current for the HTTP handler thread
        _shared_state["positions"] = state["positions"]
        _shared_state["trade_log"] = state["trade_log"]

        loop_count += 1
        _status["last_check"] = datetime.utcnow().isoformat()
        _status["loops"] = loop_count
        if loop_count % 10 == 0:
            summary(state)
        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    main()
