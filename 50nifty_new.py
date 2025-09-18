# updated_trading_bot_nifty50_full_fixed_debug.py
# Revised: added detailed debug tracing, more robust instrument token lookup,
# optional dry-run/test flags, and informative skip-logging so you can see why
# symbols are being skipped. Keep DRY_RUN=True while debugging.

import time
import datetime
import numpy as np
import pandas as pd
import threading
import math
from collections import defaultdict
from kiteconnect import KiteConnect
from openpyxl import load_workbook
import access_token
from zoneinfo import ZoneInfo
import talib
import warnings
warnings.filterwarnings("ignore")

# --- Config ---
EXCHANGE = "NSE"

# --- NIFTY50 symbols (common tickers) ---
SYMBOLS = [
    "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","KOTAKBANK","SBIN","LT",
    "AXISBANK","ITC","BHARTIARTL","HINDUNILVR","BAJAJFINSV","BAJFINANCE","MARUTI",
    "M&M","POWERGRID","NTPC","ONGC","ULTRACEMCO","TATASTEEL","JSWSTEEL","SUNPHARMA",
    "DRREDDY","COALINDIA","BPCL","IOC","GRASIM","TECHM","WIPRO","ADANIENT","ADANIPORTS",
    "DIVISLAB","CIPLA","EICHERMOT","BRITANNIA","NESTLEIND","HCLTECH","HINDALCO","SBILIFE",
    "TITAN","HDFCLIFE","INDUSINDBK","UPL","APOLLOHOSP","ASIANPAINT","TATAMOTORS","HEROMOTOCO"
]

INTERVAL = "5minute"

# --- Money & risk tuning (set for ~₹13,000 capital) ---
TOTAL_CAPITAL = 13000.0
BUFFER_AMOUNT = 500
RISK_PER_TRADE = 0.03
MAX_EXPOSURE_PCT = 0.25
MAX_PORTFOLIO_EXPOSURE_PCT = 0.5
MIN_QTY = 1        # set to 1 for liquid NIFTY50 MIS trades; change per broker contract
MAX_POSITIONS = 6
EXCEL_FILE = "strategy_trades.xlsx"
BROKERAGE_PERCENTAGE = 0.0003
CHARGES_PERCENTAGE = 0.0005
ATR_PERIOD = 14
SMA_PERIOD = 50
MIN_PROFIT_MARGIN = 1.5

DAILY_TARGET = 1000.0
DAILY_MAX_LOSS = -2000.0
SYMBOL_COOLDOWN_MIN = 20

# Execution / order handling
SLIPPAGE_PCT = 0.0006
TRAILING_STOP_TRIGGER = 0.5
TRAILING_STOP_STEP = 0.25
PARTIAL_PROFIT_PCT = 0.5
DISABLE_CONSECUTIVE_LOSSES = 7

# Time window for intraday trading (IST)
TRADING_START = (9, 30)   # hour, minute
TRADING_END = (15, 10)
MINUTES_AFTER_OPEN_TO_TRADE = 0

# Limits
MAX_DAILY_TRADES = 25
MAX_TRADES_PER_SYMBOL = 2

# Safety & testing
DRY_RUN = False   # IMPORTANT: set True for debugging / paper-trade. Set False only after testing.
DEBUG_TRACING = False  # prints diagnostic reasons for skipping

# --- Globals ---
positions = {}
last_squareoff_date = None
DAILY_PNL = 0.0
last_trade_time = defaultdict(lambda: None)

strategy_perf = defaultdict(lambda: {"pnl": 0.0, "trades": 0})
strategy_losses = defaultdict(int)
disabled_strategies = set()

# daily counters
trades_count_today = 0
trades_per_symbol_today = defaultdict(int)
trades_day = None

# concurrency lock for SL updates
sl_lock = threading.Lock()

API_KEY = 'bzr39uzdxj8keovr'
# ACCESS_TOKEN = access_token.get_access_token()

ACCESS_TOKEN='a9TlCoLETrQSGpdRJ0wseig3Ad9mV0sA'

# --- Kite Setup ---
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# --- Utility: Kite retry wrapper ---
def kite_retry(func, *args, retries=3, backoff=1, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(backoff * (2 ** i))

# --- Instrument token map & helper ---
_instrument_map = None


def build_instrument_map(exchange="NSE", force_refresh=False):
    global _instrument_map
    if _instrument_map is not None and not force_refresh:
        return _instrument_map
    print("[INFO] Building instrument map from Kite (this may take several seconds)...")
    try:
        instruments = kite_retry(kite.instruments)
    except Exception as e:
        print("[ERROR] Failed to fetch instruments list:", e)
        _instrument_map = {}
        return _instrument_map
    m = {}
    for inst in instruments:
        ts = inst.get("tradingsymbol")
        token = inst.get("instrument_token")
        if ts and token:
            m[ts.upper()] = token
    _instrument_map = m
    print(f"[INFO] Instrument map built: {len(_instrument_map)} entries")
    return _instrument_map


def get_instrument_token(symbol, exchange="NSE"):
    """
    More robust token lookup: tries exact, common suffixes, and fuzzy matching.
    Raises RuntimeError if not found.
    """
    symbol_up = symbol.upper()
    inst_map = build_instrument_map(exchange)
    # direct match
    if symbol_up in inst_map:
        return inst_map[symbol_up]
    # try common suffixes used by some feeds
    suffixes = ["-EQ", " EQ", "EQ", ".NS", "_EQ"]
    for sfx in suffixes:
        key = (symbol_up + sfx).upper()
        if key in inst_map:
            print(f"[INFO] Found token using suffix {sfx} -> {key}")
            return inst_map[key]
    # fuzzy contains (prefer full-word match)
    candidates = [k for k in inst_map.keys() if symbol_up == k or symbol_up in k or k.startswith(symbol_up)]
    if len(candidates) == 1:
        return inst_map[candidates[0]]
    elif len(candidates) > 1:
        # prefer exact equality disregard case
        for c in candidates:
            if c == symbol_up:
                return inst_map[c]
        # otherwise pick the shortest candidate (likely exact token)
        candidates = sorted(candidates, key=lambda x: len(x))
        print(f"[WARN] Multiple instrument candidates for {symbol}: {candidates}. Using {candidates[0]}")
        return inst_map[candidates[0]]
    raise RuntimeError(
        f"Couldn't find instrument token for {exchange}:{symbol}. Check symbol spelling vs Kite instruments."
    )

# --- Cleaned Technical Functions ---

def atr(df, timeperiod=ATR_PERIOD):
    return talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=timeperiod)


def calculate_atr(df, period=ATR_PERIOD):
    atr_series = atr(df, period)
    return float(atr_series[-1]) if not np.isnan(atr_series[-1]) else 0.0


def calculate_sma(df, period=SMA_PERIOD):
    sma_series = talib.SMA(df['close'].values, timeperiod=period)
    return float(sma_series[-1]) if not np.isnan(sma_series[-1]) else 0.0


def price_based_target_sl(entry, atr_val, action="BUY", target_atr=2.0, sl_atr=1.0):
    if action == "BUY":
        target = entry + atr_val * target_atr
        stoploss = entry - atr_val * sl_atr
    else:
        target = entry - atr_val * target_atr
        stoploss = entry + atr_val * sl_atr
    return float(target), float(stoploss)


def is_trending(df, period=50):
    if len(df) < period:
        return None
    sma = calculate_sma(df, period)
    entry = df['close'].iloc[-1]
    return "UP" if entry > sma else ("DOWN" if entry < sma else None)


def has_minimum_volume(df, min_volume=50000):
    # For NIFTY50 large caps, require higher volume to avoid illiquid picks
    try:
        return float(df['volume'].iloc[-1]) >= min_volume
    except Exception:
        return False


def smooth_series(series, period=3):
    s = pd.Series(series)
    return float(s.rolling(window=period).mean().iloc[-1])

# VWAP calculated intraday only

def intraday_vwap(df):
    # Expect df.index to be datetime; compute VWAP for the current date only
    try:
        today = df.index[-1].date()
    except Exception:
        return pd.Series(np.nan, index=df.index)
    todays = df[df.index.date == today]
    if todays.empty:
        return pd.Series(np.nan, index=df.index)
    vol = todays['volume'].fillna(0)
    typical = (todays['high'] + todays['low'] + todays['close']) / 3.0
    cum_tp_vol = (typical * vol).cumsum()
    cum_vol = vol.cumsum().replace(0, np.nan)
    vwap_today = (cum_tp_vol / cum_vol).reindex(df.index)
    # fill forward/backward to have VWAP for all rows
    vwap_today = vwap_today.ffill().bfill()
    return vwap_today

# --- Strategy implementations tuned for NIFTY50 ---
# (unchanged)


# 1) EMA Momentum (fast=8, slow=21) + RSI confirmation (trend following)

def strat_ema_momentum(df):
    if len(df) < 30:
        return None
    close = df['close'].values
    ema_fast = talib.EMA(close, timeperiod=8)
    ema_slow = talib.EMA(close, timeperiod=21)
    rsi = talib.RSI(close, timeperiod=14)
    if np.isnan(ema_fast[-1]) or np.isnan(ema_slow[-1]) or np.isnan(rsi[-1]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = calculate_atr(df)
    trend = is_trending(df)
    if trend is None:
        return None
    # require volume
    if not has_minimum_volume(df):
        return None
    # buy signal
    if ema_fast[-1] > ema_slow[-1] and ema_fast[-2] <= ema_slow[-2] and trend == "UP" and rsi[-1] > 50:
        score = (ema_fast[-1] - ema_slow[-1]) / (entry + 1e-6)
        if score < 0.002:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY", target_atr=2.5, sl_atr=1.2)
        return dict(action="BUY", target=target, stoploss=stoploss, score=float(score), strategy='ema_mom')
    # sell signal
    if ema_fast[-1] < ema_slow[-1] and ema_fast[-2] >= ema_slow[-2] and trend == "DOWN" and rsi[-1] < 50:
        score = (ema_slow[-1] - ema_fast[-1]) / (entry + 1e-6)
        if score < 0.002:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL", target_atr=2.5, sl_atr=1.2)
        return dict(action="SELL", target=target, stoploss=stoploss, score=float(score), strategy='ema_mom')
    return None

# 2) VWAP Opening Range Breakout (first 30-45 min) - often works on NIFTY50 stocks

def strat_vwap_opening_breakout(df):
    # works only after opening range formed
    if len(df) < 40:
        return None
    vwap_series = intraday_vwap(df)
    entry = float(df['close'].iloc[-1])
    atr_val = calculate_atr(df)
    # opening range: first N minutes (use 30 minutes -> 6 bars of 5min)
    today = df[df.index.date == df.index[-1].date()]
    if len(today) < 6:
        return None
    opening = today.iloc[:6]
    or_high = opening['high'].max()
    or_low = opening['low'].min()
    # breakout above OR high with VWAP confirmation
    if entry > or_high and entry > vwap_series.iloc[-1] and is_trending(df) == 'UP' and has_minimum_volume(df):
        score = (entry - max(or_high, vwap_series.iloc[-1])) / (entry + 1e-6)
        if score < 0.01:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action='BUY', target_atr=3.0, sl_atr=1.2)
        return dict(action='BUY', target=target, stoploss=stoploss, score=float(score), strategy='vwap_or')
    if entry < or_low and entry < vwap_series.iloc[-1] and is_trending(df) == 'DOWN' and has_minimum_volume(df):
        score = (min(or_low, vwap_series.iloc[-1]) - entry) / (entry + 1e-6)
        if score < 0.01:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action='SELL', target_atr=3.0, sl_atr=1.2)
        return dict(action='SELL', target=target, stoploss=stoploss, score=float(score), strategy='vwap_or')
    return None

# 3) Bollinger Mean-Reversion but only when market volatility is low (avoid trending markets)

def strat_bollinger_meanrev(df):
    if len(df) < 30:
        return None
    close = df['close'].values
    upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    if np.isnan(upper[-1]) or np.isnan(lower[-1]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = calculate_atr(df)
    # condition: ATR relative to price low (low volatility)
    if atr_val / entry > 0.02:
        # skip if volatility high
        return None
    if entry <= lower[-1] and is_trending(df) == 'UP' and has_minimum_volume(df):
        score = (lower[-1] - entry) / (entry + 1e-6)
        if score < 0.01:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action='BUY', target_atr=2.0, sl_atr=1.0)
        return dict(action='BUY', target=target, stoploss=stoploss, score=float(abs(score)), strategy='boll_mr')
    if entry >= upper[-1] and is_trending(df) == 'DOWN' and has_minimum_volume(df):
        score = (entry - upper[-1]) / (entry + 1e-6)
        if score < 0.01:
            return None
        target, stoploss = price_based_target_sl(entry, atr_val, action='SELL', target_atr=2.0, sl_atr=1.0)
        return dict(action='SELL', target=target, stoploss=stoploss, score=float(abs(score)), strategy='boll_mr')
    return None

# 4) ATR Volatility Breakout (trend + rising ATR)

def strat_atr_breakout(df):
    if len(df) < ATR_PERIOD + 10:
        return None
    atr_series = atr(df)
    atr_now = float(atr_series[-1])
    atr_mean = float(pd.Series(atr_series[-10:]).mean())
    if np.isnan(atr_now) or np.isnan(atr_mean):
        return None
    entry = float(df['close'].iloc[-1])
    sma50 = calculate_sma(df, period=50)
    if not has_minimum_volume(df):
        return None
    if atr_now > atr_mean * 1.1:
        # breakout in direction of trend
        if entry > sma50 and is_trending(df) == 'UP':
            target, stoploss = price_based_target_sl(entry, atr_now, action='BUY', target_atr=2.5, sl_atr=1.5)
            score = (atr_now - atr_mean) / (atr_mean + 1e-6)
            if score < 0.01:
                return None
            return dict(action='BUY', target=target, stoploss=stoploss, score=float(score), strategy='atr_bo')
        if entry < sma50 and is_trending(df) == 'DOWN':
            target, stoploss = price_based_target_sl(entry, atr_now, action='SELL', target_atr=2.5, sl_atr=1.5)
            score = (atr_now - atr_mean) / (atr_mean + 1e-6)
            if score < 0.01:
                return None
            return dict(action='SELL', target=target, stoploss=stoploss, score=float(score), strategy='atr_bo')
    return None

# register strategies (prioritize fewer, more robust ones for NIFTY50)
strategies = {
    'ema_mom': strat_ema_momentum,
    'vwap_or': strat_vwap_opening_breakout,
    'boll_mr': strat_bollinger_meanrev,
    'atr_bo': strat_atr_breakout,
}


# --- Order helpers (unchanged but careful) ---

def apply_slippage(price, side):
    if price is None:
        return price
    return price * (1 + SLIPPAGE_PCT) if side == 'BUY' else price * (1 - SLIPPAGE_PCT)


def estimate_charges(entry_price, qty):
    base = entry_price * qty * (BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)
    slippage_cost = entry_price * qty * SLIPPAGE_PCT
    return base + slippage_cost


def get_available_balance():
    try:
        margins = kite_retry(kite.margins, "equity")
        # kite margins structures can vary; try multiple keys
        available = margins.get('available', {}) if isinstance(margins, dict) else {}
        live = 0
        if isinstance(available, dict):
            live = float(available.get('live_balance', TOTAL_CAPITAL))
        else:
            live = float(margins.get('net', TOTAL_CAPITAL))
        return max(0.0, live - BUFFER_AMOUNT)
    except Exception as e:
        print("[ERROR] Fetching balance:", e)
        return max(0.0, TOTAL_CAPITAL - BUFFER_AMOUNT)


def calculate_qty_risk_based(entry_price, stoploss_price, available_balance, risk_per_trade=RISK_PER_TRADE):
    per_share_risk = abs(entry_price - stoploss_price)
    risk_amount = available_balance * risk_per_trade
    if per_share_risk <= 0 or entry_price <= 0:
        return 0
    qty_by_risk = int(risk_amount / per_share_risk)
    max_by_balance = int(available_balance / entry_price)
    max_by_exposure = int((available_balance * MAX_EXPOSURE_PCT) / entry_price)
    qty = min(qty_by_risk, max_by_balance, max_by_exposure)
    if qty < MIN_QTY:
        return 0
    return qty

# --- Excel Logging (unchanged) ---

def init_excel():
    try:
        wb = load_workbook(EXCEL_FILE)
        wb.close()
    except Exception:
        df = pd.DataFrame(columns=["Date", "Symbol", "Side", "Qty", "Entry", "Exit",
                                   "Target", "Stoploss", "Strategy", "PnL", "Status", "OrderID"])
        df.to_excel(EXCEL_FILE, index=False)


def log_trade(row):
    try:
        df = pd.read_excel(EXCEL_FILE)
    except FileNotFoundError:
        df = pd.DataFrame(columns=["Date", "Symbol", "Side", "Qty", "Entry", "Exit",
                                   "Target", "Stoploss", "Strategy", "PnL", "Status", "OrderID"])
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_excel(EXCEL_FILE, index=False)


def record_strategy_result(strategy, pnl):
    if pnl < 0:
        strategy_losses[strategy] += 1
    else:
        strategy_losses[strategy] = 0
    if strategy_losses[strategy] >= DISABLE_CONSECUTIVE_LOSSES:
        disabled_strategies.add(strategy)
        print(f"[ACTION] Disabling strategy {strategy} after {DISABLE_CONSECUTIVE_LOSSES} consecutive losses")


def update_trade(symbol, exit_price, pnl, order_id=None):
    global DAILY_PNL
    try:
        df = pd.read_excel(EXCEL_FILE)
    except FileNotFoundError:
        print("[WARN] Excel file not found when updating trade.")
        return
    open_trades = df[(df['Symbol'] == symbol) & (df['Status'] == "OPEN")]
    if open_trades.empty:
        print(f"[WARN] No open trade found for {symbol}")
        return
    idx = open_trades.index[-1]
    df.loc[idx, "Exit"] = exit_price
    df.loc[idx, "PnL"] = pnl
    df.loc[idx, "Status"] = "CLOSED"
    if order_id:
        df.loc[idx, "OrderID"] = order_id
    strategy_name = df.loc[idx, "Strategy"]
    df.to_excel(EXCEL_FILE, index=False)
    DAILY_PNL += float(pnl)
    try:
        strategy_perf[strategy_name]["pnl"] += float(pnl)
        strategy_perf[strategy_name]["trades"] += 1
    except Exception:
        pass
    print(f"[DAILY_PNL] Updated: {DAILY_PNL}")
    print_strategy_performance()
    try:
        record_strategy_result(strategy_name, float(pnl))
    except Exception:
        pass


def print_strategy_performance():
    print("[STRATEGY_PERF] PnL by strategy:")
    for s, v in strategy_perf.items():
        avg = v["pnl"] / v["trades"] if v["trades"] > 0 else 0.0
        print(f"  {s}: trades={v['trades']}, total_pnl={v['pnl']:.2f}, avg={avg:.2f}")

# --- Historical Data ---

def fetch_historical(symbol, interval=INTERVAL, days=7):
    instrument_key = f"{EXCHANGE}:{symbol}"
    try:
        token = get_instrument_token(symbol, exchange=EXCHANGE)
    except Exception as e:
        raise RuntimeError(f"Couldn't fetch instrument token for {instrument_key}: {e}")
    to_date = datetime.datetime.now(ZoneInfo('Asia/Kolkata'))
    from_date = to_date - datetime.timedelta(days=days)
    data = kite_retry(kite.historical_data, token, from_date, to_date, interval)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
    return df

# --- Stop-loss management helpers (unchanged) ---
# (omitted for brevity in this debug file - include your full implementations)

# --- Evaluate and Execute (with extra debug tracing) ---

def within_trading_hours(now):
    start = now.replace(hour=TRADING_START[0], minute=TRADING_START[1], second=0, microsecond=0)
    end = now.replace(hour=TRADING_END[0], minute=TRADING_END[1], second=0, microsecond=0)
    return start <= now <= end


def reset_daily_counters_if_needed(now):
    global trades_day, trades_count_today, trades_per_symbol_today
    if trades_day != now.date():
        trades_day = now.date()
        trades_count_today = 0
        trades_per_symbol_today = defaultdict(int)


# Diagnostic helper
def diagnose_symbol(symbol, df=None):
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    print(f"\n[DIAG] {symbol} @ {now_ist.isoformat()}")
    # instrument token
    try:
        token = None
        try:
            token = get_instrument_token(symbol, exchange=EXCHANGE)
        except Exception as e:
            print(f"[DIAG] instrument token error: {e}")
        print(f"[DIAG] instrument_token: {token}")
    except Exception as e:
        print(f"[DIAG] token lookup failed: {e}")

    # historical data summary
    if df is None:
        try:
            df = fetch_historical(symbol, interval=INTERVAL, days=7)
        except Exception as e:
            print(f"[DIAG] fetch_historical error: {e}")
            df = pd.DataFrame()
    if df.empty:
        print("[DIAG] historical rows: 0 or fetch failed")
        return
    print(f"[DIAG] historical rows: {len(df)}")
    try:
        print(f"[DIAG] last close: {df['close'].iloc[-1]}, last vol: {df['volume'].iloc[-1]}")
        avg_vol = df['volume'].tail(20).mean() if len(df) >= 20 else df['volume'].mean()
        print(f"[DIAG] avg vol (tail20 or mean): {avg_vol:.1f}")
    except Exception as e:
        print(f"[DIAG] historical summary error: {e}")

    # time checks
    now = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    print(f"[DIAG] within_trading_hours: {within_trading_hours(now)}")
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    secs_after_open = (now - market_open).total_seconds()
    print(f"[DIAG] secs_after_open: {secs_after_open} (need >= {MINUTES_AFTER_OPEN_TO_TRADE*60})")

    # balances and qty estimate
    try:
        avail = get_available_balance()
        print(f"[DIAG] available_balance: {avail:.2f}")
    except Exception as e:
        print(f"[DIAG] get_available_balance error: {e}")
        avail = 0

    # run strategies and show outputs
    for name, strategy_func in strategies.items():
        if name in disabled_strategies:
            print(f"[DIAG] strategy {name} is disabled")
            continue
        try:
            res = strategy_func(df)
            print(f"[DIAG] strategy {name}: {res}")
        except Exception as e:
            print(f"[DIAG] strategy {name} ERROR: {e}")


# Main evaluate_and_execute with verbose skip reasons when DEBUG_TRACING

def evaluate_and_execute(symbol):
    global trades_count_today
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    reset_daily_counters_if_needed(now_ist)

    if DEBUG_TRACING:
        # quick diagnostic up front
        try:
            df_quick = None
            try:
                df_quick = fetch_historical(symbol, interval=INTERVAL, days=7)
            except Exception:
                df_quick = pd.DataFrame()
            diagnose_symbol(symbol, df_quick)
        except Exception as e:
            print(f"[DIAG] diagnose failed for {symbol}: {e}")

    # time filters
    if not within_trading_hours(now_ist):
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: outside trading hours ({now_ist.time()})")
        return
    market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    if (now_ist - market_open).total_seconds() < MINUTES_AFTER_OPEN_TO_TRADE * 60:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: waiting for {MINUTES_AFTER_OPEN_TO_TRADE} minutes after open")
        return
    if trades_count_today >= MAX_DAILY_TRADES:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: daily trades limit reached ({trades_count_today})")
        return
    if DAILY_PNL >= DAILY_TARGET or DAILY_PNL <= DAILY_MAX_LOSS:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: daily PnL limit hit: {DAILY_PNL}")
        return
    last = last_trade_time.get(symbol)
    if last and (now_ist - last).total_seconds() < SYMBOL_COOLDOWN_MIN * 60:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: cooling down since last trade at {last}")
        return
    if symbol not in SYMBOLS:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: not in SYMBOLS list")
        return

    try:
        df = fetch_historical(symbol, interval=INTERVAL, days=7)
    except Exception as e:
        print(f"[ERROR] Fetching historical data for {symbol}: {e}")
        return

    if df.empty or len(df) < 50:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: insufficient historical bars ({len(df) if hasattr(df,'shape') else '0'})")
        return

    # basic liquidity filter: avg volume last 20 bars
    avg_vol = df['volume'].tail(20).mean()
    if avg_vol < 20000:  # tuned for NIFTY50
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: avg_vol {avg_vol:.1f} < 20000")
        return

    signals = []
    for name, strategy_func in strategies.items():
        if name in disabled_strategies:
            if DEBUG_TRACING:
                print(f"[SKIP_STRAT] {symbol}: strategy {name} disabled")
            continue
        try:
            result = strategy_func(df)
            if result and all(k in result for k in ['action','target','stoploss','score']):
                result['strategy'] = result.get('strategy', name)
                signals.append(result)
            else:
                if DEBUG_TRACING:
                    print(f"[STRAT_NONE] {symbol}: strategy {name} returned None or incomplete")
        except Exception as e:
            print(f"[ERROR] Strategy {name} failed for {symbol}: {e}")
    if not signals:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: no strategy signals (all returned None or filtered)")
        return

    weighted_scores = {"BUY": 0.0, "SELL": 0.0}
    for sig in signals:
        weighted_scores[sig['action']] += sig['score']
    best_action = max(weighted_scores, key=weighted_scores.get)
    if weighted_scores[best_action] <= 0 or weighted_scores[best_action] < 0.02:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: weighted score too low: {weighted_scores}")
        return
    chosen = max((sig for sig in signals if sig['action'] == best_action), key=lambda x: x['score'])

    action = chosen['action']
    proposed_entry = float(df['close'].iloc[-1])
    stoploss = chosen['stoploss']
    target = chosen['target']
    strategy_name = chosen['strategy']
    reward = abs(target - proposed_entry)
    risk = abs(stoploss - proposed_entry)

    # ensure direction matches SMA
    sma = calculate_sma(df)
    if action == "BUY" and proposed_entry < sma:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: BUY but price below SMA ({proposed_entry} < {sma})")
        return
    if action == "SELL" and proposed_entry > sma:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: SELL but price above SMA ({proposed_entry} > {sma})")
        return
    if risk == 0 or reward / risk < 2:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: reward/risk too low ({reward}/{risk})")
        return

    available_balance = get_available_balance()
    qty = calculate_qty_risk_based(proposed_entry, stoploss, available_balance)
    if qty == 0:
        print(f"[SKIP] Qty below minimum for {symbol}")
        return
    if symbol in positions:
        print(f"[SKIP] Already trading {symbol}")
        return
    if len(positions) >= MAX_POSITIONS:
        print(f"[SKIP] Max positions reached")
        return

    total_profit = reward * qty
    total_charges = estimate_charges(proposed_entry, qty)
    if total_profit <= total_charges * MIN_PROFIT_MARGIN:
        print(f"[SKIP] Trade not worth it for {symbol}")
        return

    exposure = proposed_entry * qty
    current_exposure = sum(p['entry'] * p['qty'] for p in positions.values())
    if (current_exposure + exposure) > (available_balance * MAX_PORTFOLIO_EXPOSURE_PCT):
        print(f"[SKIP] Would exceed portfolio exposure for {symbol}")
        return
    if exposure > available_balance:
        print(f"[SKIP] Insufficient funds for {symbol}")
        return

    # per-symbol daily trade cap
    if trades_per_symbol_today[symbol] >= MAX_TRADES_PER_SYMBOL:
        if DEBUG_TRACING:
            print(f"[SKIP] {symbol}: per-symbol daily trades reached")
        return

    # Place order (DRY_RUN will simulate)
    try:
        if DRY_RUN:
            order_id = f"DRY-{symbol}-{int(time.time())}"
            print(f"[DRY_RUN] Simulated order {order_id}: {action} {qty} {symbol}")
        else:
            order_id = kite_retry(kite.place_order,
                                  variety=kite.VARIETY_REGULAR,
                                  exchange=EXCHANGE,
                                  tradingsymbol=symbol,
                                  transaction_type=kite.TRANSACTION_TYPE_BUY if action == "BUY" else kite.TRANSACTION_TYPE_SELL,
                                  quantity=qty,
                                  product=kite.PRODUCT_MIS,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  validity=kite.VALIDITY_DAY)
            print(f"[ORDER_PLACED] {action} {qty} {symbol}, order_id={order_id}")
    except Exception as e:
        print(f"[ERROR] Order failed for {symbol}: {e}")
        return

    executed_price = None
    try:
        # try LTP both by symbol and by instrument token if necessary
        try:
            ltp_resp = kite_retry(kite.ltp, f"{EXCHANGE}:{symbol}")
            executed_price = float(ltp_resp[f"{EXCHANGE}:{symbol}"]["last_price"])
        except Exception:
            tok = get_instrument_token(symbol)
            ltp_resp = kite_retry(kite.ltp, tok)
            # when requesting by token the key may differ; try best effort
            if isinstance(ltp_resp, dict):
                # pick first numeric value
                for v in ltp_resp.values():
                    if isinstance(v, dict) and 'last_price' in v:
                        executed_price = float(v['last_price'])
                        break
    except Exception as e:
        print(f"[WARN] Couldn't fetch executed price; using proposed entry. {e}")
        executed_price = proposed_entry
    if executed_price is None:
        executed_price = proposed_entry

    executed_price = apply_slippage(executed_price, action)
    # record position and place exchange stop
    positions[symbol] = dict(side=action, qty=qty, entry=executed_price, target=target, stoploss=stoploss, strategy=strategy_name, order_id=order_id)
    # place SL (simulated if DRY_RUN)
    # ensure place_stop_order_on_kite exists in your full code; for brevity omitted here
    try:
        sl_order_id = None
        if 'place_stop_order_on_kite' in globals():
            sl_order_id = place_stop_order_on_kite(symbol, action, qty, stoploss)
        positions[symbol]['sl_order_id'] = sl_order_id
    except Exception as e:
        print(f"[WARN] SL placement failed for {symbol}: {e}")

    last_trade_time[symbol] = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    trades_count_today += 1
    trades_per_symbol_today[symbol] += 1
    log_trade({
        "Date": last_trade_time[symbol].replace(tzinfo=None),
        "Symbol": symbol,
        "Side": action,
        "Qty": qty,
        "Entry": executed_price,
        "Exit": None,
        "Target": target,
        "Stoploss": stoploss,
        "Strategy": strategy_name,
        "PnL": None,
        "Status": "OPEN",
        "OrderID": order_id
    })


def modify_stop_order_on_kite(sl_order_id, new_trigger):
    if sl_order_id is None:
        return False
    if DRY_RUN or str(sl_order_id).startswith("DRY-"):
        print(f"[DRY_RUN] Simulated modify SL {sl_order_id} -> trigger={new_trigger}")
        return True
    try:
        kite_retry(
            kite.modify_order,
            order_id=sl_order_id,
            variety=kite.VARIETY_REGULAR,
            trigger_price=float(new_trigger)
        )
        print(f"[SL_MODIFIED] order={sl_order_id} new_trigger={new_trigger}")
        return True
    except Exception as e:
        print(f"[WARN] modify SL failed for order {sl_order_id}: {e}")
        return False


def cancel_order_on_kite(sl_order_id):
    if sl_order_id is None:
        return False
    if DRY_RUN or str(sl_order_id).startswith("DRY-"):
        print(f"[DRY_RUN] Simulated cancel order {sl_order_id}")
        return True
    try:
        kite_retry(kite.cancel_order, variety=kite.VARIETY_REGULAR, order_id=sl_order_id)
        print(f"[SL_CANCELLED] {sl_order_id}")
        return True
    except Exception as e:
        print(f"[WARN] cancel SL failed for {sl_order_id}: {e}")
        return False


def update_stoploss_on_kite(symbol, new_stop):
    with sl_lock:
        pos = positions.get(symbol)
        if not pos:
            print(f"[WARN] No live position for {symbol} to update SL")
            return None
        side = pos['side']
        qty = pos['qty']
        current_sl_id = pos.get('sl_order_id')
        ok = False
        try:
            ok = modify_stop_order_on_kite(current_sl_id, new_stop)
        except Exception:
            ok = False
        if not ok:
            try:
                cancel_order_on_kite(current_sl_id)
            except Exception:
                pass
            new_id = place_stop_order_on_kite(symbol, side, qty, new_stop)
            if new_id:
                pos['sl_order_id'] = new_id
                pos['stoploss'] = new_stop
                print(f"[SL_UPDATED] New SL order {new_id} placed for {symbol} -> {new_stop}")
                return new_id
            else:
                print(f"[ERROR] Could not place new SL order for {symbol}; keeping internal SL = {pos.get('stoploss')}")
                return None
        else:
            pos['stoploss'] = new_stop
            return current_sl_id


# --- Monitoring and exit logic ---
def place_exit_order(symbol, side, qty):
    try:
        opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY
        if DRY_RUN:
            order_id = f"DRY-EXIT-{symbol}-{int(time.time())}"
            print(f"[DRY_RUN] Simulated exit {order_id}: {opposite} {qty} {symbol}")
            return order_id
        return kite_retry(kite.place_order,
                          variety=kite.VARIETY_REGULAR,
                          exchange=EXCHANGE,
                          tradingsymbol=symbol,
                          transaction_type=opposite,
                          quantity=qty,
                          product=kite.PRODUCT_MIS,
                          order_type=kite.ORDER_TYPE_MARKET,
                          validity=kite.VALIDITY_DAY)
    except Exception as e:
        print(f"[ERROR] Exit order for {symbol} failed:", e)
        return None


def fetch_live_net_positions():
    try:
        pos = kite_retry(kite.positions)
        return pos.get('net', [])
    except Exception as e:
        print("[ERROR] Fetching live positions failed:", e)
        return []


def squareoff_live_positions():
    live = fetch_live_net_positions()
    for pos in live:
        try:
            tradingsymbol = pos.get('tradingsymbol') or ''
            quantity = int(pos.get('quantity', 0))
            if quantity == 0:
                continue
            side = "SELL" if quantity > 0 else "BUY"
            qty = abs(quantity)
            print(f"[SQUAREOFF] {side} {qty} {tradingsymbol}")
            place_exit_order(tradingsymbol, side, qty)
        except Exception as e:
            print("[ERROR] During squareoff:", e)


def check_and_squareoff():
    global last_squareoff_date
    now = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    if last_squareoff_date != now.date() and now.hour >= 15:
        print("[INFO] Performing daily squareoff...")
        squareoff_live_positions()
        last_squareoff_date = now.date()


def monitor_positions():
    global DAILY_PNL, positions
    while True:
        now = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
        check_and_squareoff()
        net_positions = fetch_live_net_positions()
        for pos in net_positions:
            symbol = pos.get('tradingsymbol') or ''
            quantity = abs(int(pos.get('quantity', 0)))
            if symbol in positions and quantity > 0:
                side = positions[symbol]['side']
                entry = positions[symbol]['entry']
                target = positions[symbol]['target']
                stoploss = positions[symbol]['stoploss']
                strategy_name = positions[symbol].get('strategy')
                try:
                    ltp_resp = kite_retry(kite.ltp, f"{EXCHANGE}:{symbol}")
                    ltp = float(ltp_resp[f"{EXCHANGE}:{symbol}"]["last_price"])
                except Exception as e:
                    print(f"[ERROR] LTP fetch failed for {symbol}: {e}")
                    continue
                exit_trade = False
                reason = ""
                # trailing and partial logic
                if side == "BUY":
                    if ltp >= target:
                        exit_trade = True
                        reason = "TARGET"
                    elif ltp <= stoploss:
                        exit_trade = True
                        reason = "STOPLOSS"
                    else:
                        if ltp >= entry + TRAILING_STOP_TRIGGER * (target - entry):
                            new_stop = entry + TRAILING_STOP_STEP * (ltp - entry)
                            if new_stop > positions[symbol]['stoploss']:
                                update_stoploss_on_kite(symbol, new_stop)
                else:
                    if ltp <= target:
                        exit_trade = True
                        reason = "TARGET"
                    elif ltp >= stoploss:
                        exit_trade = True
                        reason = "STOPLOSS"
                    else:
                        if ltp <= entry - TRAILING_STOP_TRIGGER * (entry - target):
                            new_stop = entry - TRAILING_STOP_STEP * (entry - ltp)
                            if new_stop < positions[symbol]['stoploss']:
                                update_stoploss_on_kite(symbol, new_stop)
                if exit_trade:
                    print(f"[EXIT] {symbol} hitting {reason}: LTP={ltp}, Entry={entry}, Target={target}, SL={stoploss}")
                    # partial exit handling
                    if PARTIAL_PROFIT_PCT < 1.0 and reason == "TARGET":
                        total_qty = positions[symbol]['qty']
                        partial_qty = int(total_qty * PARTIAL_PROFIT_PCT)
                        remaining_qty = total_qty - partial_qty
                        if partial_qty > 0:
                            order_id_partial = place_exit_order(symbol, side, partial_qty)
                            pnl_partial = round((ltp - entry) * partial_qty * (1 if side == "BUY" else -1), 2)
                            print(f"[PARTIAL_EXIT] {symbol} qty={partial_qty} pnl={pnl_partial}")
                            log_trade({
                                "Date": now.replace(tzinfo=None),
                                "Symbol": symbol,
                                "Side": side,
                                "Qty": partial_qty,
                                "Entry": entry,
                                "Exit": ltp,
                                "Target": target,
                                "Stoploss": stoploss,
                                "Strategy": strategy_name,
                                "PnL": pnl_partial,
                                "Status": "CLOSED_PARTIAL",
                                "OrderID": order_id_partial
                            })
                            strategy_perf[strategy_name]["pnl"] += pnl_partial
                            strategy_perf[strategy_name]["trades"] += 1
                            DAILY_PNL += pnl_partial
                            record_strategy_result(strategy_name, pnl_partial)
                        if remaining_qty <= 0:
                            if positions[symbol].get('sl_order_id'):
                                cancel_order_on_kite(positions[symbol]['sl_order_id'])
                            positions.pop(symbol, None)
                            continue
                        else:
                            positions[symbol]['qty'] = remaining_qty
                            continue
                    # full exit
                    if positions[symbol].get('sl_order_id'):
                        cancel_order_on_kite(positions[symbol]['sl_order_id'])
                    order_id = place_exit_order(symbol, side, quantity)
                    pnl = round((ltp - entry) * quantity * (1 if side == "BUY" else -1), 2)
                    update_trade(symbol, ltp, pnl, order_id)
                    positions.pop(symbol, None)
        time.sleep(2)


def place_stop_order_on_kite(symbol, side, qty, stop_price):
    opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY
    if DRY_RUN:
        oid = f"DRY-SL-{symbol}-{int(time.time())}"
        print(f"[DRY_RUN] Simulated SL order {oid}: {opposite} {qty} {symbol} trigger={stop_price}")
        return oid
    try:
        order_id = kite_retry(
            kite.place_order,
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=float(stop_price),
            validity=kite.VALIDITY_DAY
        )
        print(f"[SL_PLACED] {symbol} sl_order_id={order_id} trigger={stop_price}")
        return order_id
    except Exception as e:
        print(f"[ERROR] placing SL order for {symbol}: {e}")
        return None


# --- Run bot (keeps same) ---

def run_bot():
    try:
        build_instrument_map('NSE')
    except Exception as e:
        print("[WARN] instrument map build failed at startup:", e)
    init_excel()
    monitor_thread = threading.Thread(target=monitor_positions, daemon=True)
    monitor_thread.start()
    while True:
        for symbol in SYMBOLS:
            try:
                evaluate_and_execute(symbol)
            except Exception as e:
                print(f"[ERROR] Exception during evaluation for {symbol}: {e}")
            time.sleep(0.5)
        time.sleep(2)

if __name__ == "__main__":
    run_bot()