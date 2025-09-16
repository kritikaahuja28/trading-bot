import time
import datetime
import numpy as np
import pandas as pd
import threading
from kiteconnect import KiteConnect
from openpyxl import load_workbook
import access_token
from zoneinfo import ZoneInfo
import talib
import warnings
warnings.filterwarnings("ignore")

# --- Config ---
EXCHANGE = "NSE"
SYMBOLS = ["ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
           "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BHARTIARTL", "BRITANNIA",
           "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
           "GRASIM", "ICICIBANK", "RELIANCE", "SHRIRAMFIN", "JSWSTEEL", "TECHM"]
INTERVAL = "5minute"
RISK_PER_TRADE = 0.01
MAX_POSITIONS = 10
EXCEL_FILE = "strategy_trades.xlsx"
MIN_QTY = 5
BUFFER_AMOUNT = 500
BROKERAGE_PERCENTAGE = 0.0003
CHARGES_PERCENTAGE = 0.0005
ATR_PERIOD = 14
SMA_PERIOD = 20
MIN_PROFIT_MARGIN = 1.5

# --- Globals ---
positions = {}
last_squareoff_date = None

API_KEY='bzr39uzdxj8keovr'
ACCESS_TOKEN=access_token.get_access_token()
# --- Kite Connect Setup ---
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# --- Technical Functions ---
def atr(df, timeperiod=14):
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    atr_series = talib.ATR(high, low, close, timeperiod=timeperiod)
    return atr_series

def price_based_target_sl(entry, atr_value, target_atr=2.0, sl_atr=1.0):
    target = entry + atr_value * target_atr
    stoploss = entry - atr_value * sl_atr
    return float(target), float(stoploss)

def calculate_atr(df, period=ATR_PERIOD):
    high = df['high']
    low = df['low']
    close = df['close']
    high_low = high - low
    high_close = abs(high - close.shift())
    low_close = abs(low - close.shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr_val = true_range.rolling(window=period).mean().iloc[-1]
    return atr_val

def calculate_sma(df, period=SMA_PERIOD):
    return df['close'].rolling(window=period).mean().iloc[-1]

def get_available_balance():
    try:
        margins = kite.margins("equity")
        available = margins['available']['live_balance']
        return max(0, available - BUFFER_AMOUNT)
    except Exception as e:
        print("[ERROR] Fetching balance:", e)
        return 0

def calculate_qty(entry_price, stoploss_price, available_balance):
    per_share_cost = entry_price * (1 + BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)
    max_qty = int(available_balance / per_share_cost)
    return max_qty

def estimate_charges(entry_price, qty):
    return entry_price * qty * (BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)

# --- Excel Logging ---
def init_excel():
    try:
        wb = load_workbook(EXCEL_FILE)
        wb.close()
    except:
        df = pd.DataFrame(columns=["Date", "Symbol", "Side", "Qty", "Entry", "Exit", 
                                   "Target", "Stoploss", "Strategy", "PnL", "Status"])
        df.to_excel(EXCEL_FILE, index=False)

def log_trade(row):
    df = pd.read_excel(EXCEL_FILE)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_excel(EXCEL_FILE, index=False)

def update_trade(symbol, exit_price, pnl):
    df = pd.read_excel(EXCEL_FILE)
    open_trades = df[(df['Symbol'] == symbol) & (df['Status'] == "OPEN")]
    if open_trades.empty:
        print(f"[WARN] No open trade found for {symbol}")
        return
    idx = open_trades.index[-1]
    df.loc[idx, "Exit"] = exit_price
    df.loc[idx, "PnL"] = pnl
    df.loc[idx, "Status"] = "CLOSED"
    df.to_excel(EXCEL_FILE, index=False)

# --- Historical Data ---
def fetch_historical(symbol, interval=INTERVAL, days=3):
    instrument = f"{EXCHANGE}:{symbol}"
    token_entry = kite.ltp(instrument).get(instrument)
    if token_entry is None:
        raise RuntimeError(f"Couldn't fetch instrument token for {instrument}")
    token = token_entry["instrument_token"]
    to_date = datetime.datetime.now()
    from_date = to_date - datetime.timedelta(days=days)
    data = kite.historical_data(token, from_date, to_date, interval)
    df = pd.DataFrame(data)
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    df.set_index('date', inplace=True)
    return df

# --- Strategies ---

def strat_aroon_crossover(df, timeperiod=14):
    if len(df) < timeperiod + 3:
        return None
    aroon_down, aroon_up = talib.AROON(df['high'].values, df['low'].values, timeperiod=timeperiod)
    if np.isnan(aroon_up[-1]) or np.isnan(aroon_down[-1]) or np.isnan(aroon_up[-2]) or np.isnan(aroon_down[-2]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    long_ema = talib.EMA(df['close'].values, timeperiod=50)[-1]
    if aroon_up[-1] > aroon_down[-1] and aroon_up[-2] <= aroon_down[-2] and entry > long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=2.0, sl_atr=1.0)
        score = float((aroon_up[-1] - aroon_down[-1]) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if aroon_down[-1] > aroon_up[-1] and aroon_down[-2] <= aroon_up[-2] and entry < long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=2.0, sl_atr=1.0)
        score = float((aroon_down[-1] - aroon_up[-1]) / 100.0)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

def strat_ema_crossover(df, short_period=9, long_period=21):
    if len(df) < long_period + 3:
        return None
    close = df['close'].values
    ema_short = talib.EMA(close, timeperiod=short_period)
    ema_long = talib.EMA(close, timeperiod=long_period)
    if np.isnan(ema_short[-1]) or np.isnan(ema_long[-1]) or np.isnan(ema_short[-2]) or np.isnan(ema_long[-2]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    long_ema = talib.EMA(close, timeperiod=50)[-1]
    if ema_short[-1] > ema_long[-1] and ema_short[-2] <= ema_long[-2] and entry > long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.8, sl_atr=0.8)
        score = float((ema_short[-1] - ema_long[-1]) / entry)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if ema_long[-1] > ema_short[-1] and ema_long[-2] <= ema_short[-2] and entry < long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.8, sl_atr=0.8)
        score = float((ema_long[-1] - ema_short[-1]) / entry)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

# Add other strategies as needed...
# For brevity, only two are included here, but you can add others like MACD, VWAP, RSI, etc.

strategies = {
    "aroon": strat_aroon_crossover,
    "ema": strat_ema_crossover,
    # add others similarly
}

# --- Evaluate and Execute ---
def evaluate_and_execute(symbol):
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 15):
        print(f"[INFO] Skipping {symbol}, after 3:15 PM")
        return
    try:
        df = fetch_historical(symbol, interval=INTERVAL, days=7)
    except Exception as e:
        print(f"[ERROR] Fetching historical data for {symbol}: {e}")
        return
    atr_val = calculate_atr(df)
    sma = calculate_sma(df)
    signals = []
    for name, strategy_func in strategies.items():
        try:
            result = strategy_func(df)
            if result:
                result['strategy'] = name
                signals.append(result)
        except Exception as e:
            print(f"[ERROR] Strategy {name} failed for {symbol}: {e}")
    if not signals:
        return
    weighted_scores = {"BUY": 0.0, "SELL": 0.0}
    for sig in signals:
        weighted_scores[sig['action']] += sig['score']
    best_action = max(weighted_scores, key=weighted_scores.get)
    chosen = max((sig for sig in signals if sig['action'] == best_action), key=lambda x: x['score'])
    action = chosen['action']
    entry_price = float(df['close'].iloc[-1])
    stoploss = chosen['stoploss']
    target = chosen['target']
    strategy_name = chosen['strategy']
    reward = abs(target - entry_price)
    risk = abs(stoploss - entry_price)
    if action == "BUY" and entry_price < sma:
        print(f"[SKIP] BUY not in uptrend for {symbol}")
        return
    if action == "SELL" and entry_price > sma:
        print(f"[SKIP] SELL not in downtrend for {symbol}")
        return
    if risk == 0 or reward / risk < 2:
        print(f"[SKIP] Low reward/risk for {symbol}")
        return
    available_balance = get_available_balance()
    qty = calculate_qty(entry_price, stoploss, available_balance)
    if qty < MIN_QTY:
        print(f"[SKIP] Qty {qty} below minimum for {symbol}")
        return
    if symbol in positions:
        print(f"[SKIP] Already trading {symbol}")
        return
    if len(positions) >= MAX_POSITIONS:
        print(f"[SKIP] Max positions reached")
        return
    total_profit = reward * qty
    total_charges = estimate_charges(entry_price, qty)
    if total_profit <= total_charges * MIN_PROFIT_MARGIN:
        print(f"[SKIP] Trade not worth it for {symbol}")
        return
    exposure = entry_price * qty
    if exposure > available_balance:
        print(f"[SKIP] Insufficient funds for {symbol}")
        return
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY if action == "BUY" else kite.TRANSACTION_TYPE_SELL,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity=kite.VALIDITY_DAY
        )
        print(f"[ORDER] {action} {qty} {symbol} at {entry_price}, SL={stoploss}, Target={target}, Strategy={strategy_name}")
    except Exception as e:
        print(f"[ERROR] Order failed for {symbol}: {e}")
        return
    positions[symbol] = dict(side=action, qty=qty, entry=entry_price, target=target, stoploss=stoploss, strategy=strategy_name)
    log_trade({
        "Date": now_ist.replace(tzinfo=None),
        "Symbol": symbol,
        "Side": action,
        "Qty": qty,
        "Entry": entry_price,
        "Exit": None,
        "Target": target,
        "Stoploss": stoploss,
        "Strategy": strategy_name,
        "PnL": None,
        "Status": "OPEN"
    })

# --- Monitoring ---
def place_exit_order(symbol, side, qty):
    try:
        opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY
        return kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity=kite.VALIDITY_DAY
        )
    except Exception as e:
        print(f"[ERROR] Exit order for {symbol} failed:", e)
        return None

def fetch_live_net_positions():
    try:
        return kite.positions().get('net', [])
    except Exception as e:
        print("[ERROR] Fetching live positions failed:", e)
        return []

def squareoff_live_positions():
    live = fetch_live_net_positions()
    for pos in live:
        try:
            tradingsymbol = pos.get('tradingsymbol') or ''
            exchange_code = pos.get('exchange', EXCHANGE)
            product = pos.get('product', '')
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
                try:
                    ltp = float(kite.ltp(f"{EXCHANGE}:{symbol}")[f"{EXCHANGE}:{symbol}"]["last_price"])
                except Exception as e:
                    print(f"[ERROR] LTP fetch failed for {symbol}: {e}")
                    continue
                exit_trade = False
                reason = ""
                if side == "BUY":
                    if ltp >= target:
                        exit_trade = True
                        reason = "TARGET"
                    elif ltp <= stoploss:
                        exit_trade = True
                        reason = "STOPLOSS"
                else:
                    if ltp <= target:
                        exit_trade = True
                        reason = "TARGET"
                    elif ltp >= stoploss:
                        exit_trade = True
                        reason = "STOPLOSS"
                if exit_trade:
                    print(f"[EXIT] {symbol} hitting {reason}: LTP={ltp}, Entry={entry}, Target={target}, SL={stoploss}")
                    place_exit_order(symbol, side, quantity)
                    pnl = round((ltp - entry) * quantity * (1 if side == "BUY" else -1), 2)
                    update_trade(symbol, ltp, pnl)
                    positions.pop(symbol, None)
        time.sleep(2)

def run_bot():
    init_excel()
    monitor_thread = threading.Thread(target=monitor_positions, daemon=True)
    monitor_thread.start()
    while True:
        for symbol in SYMBOLS:
            try:
                evaluate_and_execute(symbol)
            except Exception as e:
                print(f"[ERROR] Exception during evaluation for {symbol}: {e}")
            time.sleep(1)
        time.sleep(5)

if __name__ == "__main__":
    run_bot()
