# updated_trading_bot_nifty50.py
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
# Edit this list if you want different or updated constituents.
SYMBOLS = [
    "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","HDFC","KOTAKBANK","SBIN","LT",
    "AXISBANK","ITC","BHARTIARTL","HINDUNILVR","BAJFINSV","BAJFINANCE","MARUTI",
    "M&M","POWERGRID","NTPC","ONGC","ULTRACEMCO","TATASTEEL","JSWSTEEL","SUNPHARMA",
    "DRREDDY","COALINDIA","BPCL","IOC","GRASIM","TECHM","WIPRO","ADANIENT","ADANIPORTS",
    "DIVISLAB","CIPLA","EICHERMOT","BRITANNIA","NESTLEIND","HCLTECH","HINDALCO","SBILIFE",
    "TITAN","HDFCLIFE","INDUSINDBK","UPL","APOLLOHOSP","ASIANPAINT","TATAMOTORS","HEROMOTOCO",
    "ONGC"  # duplicate is okay but you may remove duplicates
]

INTERVAL = "5minute"
RISK_PER_TRADE = 0.03            # Changed default to 3% (you can tune this)
MAX_POSITIONS = 10
EXCEL_FILE = "strategy_trades.xlsx"
MIN_QTY = 1                      # minimum quantity to place (set 5 if broker requires)
BUFFER_AMOUNT = 500
BROKERAGE_PERCENTAGE = 0.0003
CHARGES_PERCENTAGE = 0.0005
ATR_PERIOD = 14
SMA_PERIOD = 20
MIN_PROFIT_MARGIN = 1.5

DAILY_TARGET = 1000.0
DAILY_MAX_LOSS = -2000.0
SYMBOL_COOLDOWN_MIN = 15

# --- Globals ---
positions = {}
last_squareoff_date = None
DAILY_PNL = 0.0
last_trade_time = defaultdict(lambda: None)

# track per-strategy performance (PnL, trades)
strategy_perf = defaultdict(lambda: {"pnl": 0.0, "trades": 0})

API_KEY = 'bzr39uzdxj8keovr'
ACCESS_TOKEN = access_token.get_access_token()

# --- Kite Setup ---
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# --- Technical Functions ---
def atr(df, timeperiod=14):
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    atr_series = talib.ATR(high, low, close, timeperiod=timeperiod)
    return atr_series

def price_based_target_sl(entry, atr_value, action, target_atr=3.0, sl_atr=1.5):
    if action == "BUY":
        target = entry + atr_value * target_atr
        stoploss = entry - atr_value * sl_atr
    else:  # SELL
        target = entry - atr_value * target_atr
        stoploss = entry + atr_value * sl_atr
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

def estimate_charges(entry_price, qty):
    return entry_price * qty * (BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)

def calculate_qty_risk_based(entry_price, stoploss_price, available_balance, risk_per_trade=RISK_PER_TRADE):
    per_share_risk = abs(entry_price - stoploss_price)
    risk_amount = available_balance * risk_per_trade
    if per_share_risk <= 0:
        return 0
    qty = int(risk_amount / per_share_risk)
    # cap qty by available balance and minimum qty
    max_qty = int(available_balance / entry_price) if entry_price > 0 else 0
    qty = min(qty, max_qty)
    if qty < MIN_QTY:
        return 0
    return qty

# --- Excel Logging ---
def init_excel():
    try:
        wb = load_workbook(EXCEL_FILE)
        wb.close()
    except:
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
    # update strategy performance
    try:
        strategy_perf[strategy_name]["pnl"] += float(pnl)
        strategy_perf[strategy_name]["trades"] += 1
    except Exception:
        pass
    print(f"[DAILY_PNL] Updated: {DAILY_PNL}")
    print_strategy_performance()

def print_strategy_performance():
    print("[STRATEGY_PERF] PnL by strategy:")
    for s, v in strategy_perf.items():
        avg = v["pnl"] / v["trades"] if v["trades"] > 0 else 0.0
        print(f"  {s}: trades={v['trades']}, total_pnl={v['pnl']:.2f}, avg={avg:.2f}")

# --- Historical Data ---
def fetch_historical(symbol, interval=INTERVAL, days=7):
    # instrument = f"{EXCHANGE}:{symbol}"
    instrument = symbol
    token_entry = kite.ltp(f"{EXCHANGE}:{symbol}")[f"{EXCHANGE}:{symbol}"]["last_price"]
    if token_entry is None:
        raise RuntimeError(f"Couldn't fetch instrument token for {instrument}")
    token = token_entry["instrument_token"]
    to_date = datetime.datetime.now()
    from_date = to_date - datetime.timedelta(days=days)
    data = kite.historical_data(token, from_date, to_date, interval)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    df.set_index('date', inplace=True)
    # ensure numeric types
    df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
    return df

# --- Strategy Implementations ---
def strat_macd_signal(df):
    """
    MACD crossover: buy when MACD crosses above signal, sell on cross below.
    """
    if len(df) < 35:
        return None
    close = df['close'].values
    macd, macdsignal, macdhist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    if np.isnan(macd[-1]) or np.isnan(macdsignal[-1]) or np.isnan(macd[-2]) or np.isnan(macdsignal[-2]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    # crossover logic
    if macd[-1] > macdsignal[-1] and macd[-2] <= macdsignal[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY")
        score = float(abs(macdhist[-1])) / (abs(entry) + 1e-6)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if macd[-1] < macdsignal[-1] and macd[-2] >= macdsignal[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL")
        score = float(abs(macdhist[-1])) / (abs(entry) + 1e-6)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

def strat_vwap_breakout(df):
    """
    VWAP breakout: buy when price crosses above VWAP and VWAP is rising; sell on break below.
    """
    if len(df) < 30:
        return None
    high = df['high']
    low = df['low']
    close = df['close']
    vol = df['volume'].fillna(0)
    typical = (high + low + close) / 3.0
    cum_tp_vol = (typical * vol).cumsum()
    cum_vol = vol.cumsum().replace(0, np.nan)
    vwap = cum_tp_vol / cum_vol
    # last and previous
    if np.isnan(vwap.iloc[-1]) or np.isnan(vwap.iloc[-2]):
        return None
    entry = float(close.iloc[-1])
    atr_val = calculate_atr(df)
    # breakout
    if entry > vwap.iloc[-1] and close.iloc[-2] <= vwap.iloc[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY")
        score = float((entry - vwap.iloc[-1]) / (entry + 1e-6))
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if entry < vwap.iloc[-1] and close.iloc[-2] >= vwap.iloc[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL")
        score = float((vwap.iloc[-1] - entry) / (entry + 1e-6))
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

def strat_bollinger_reversion(df):
    """
    Bollinger bands mean reversion: sell at upper band, buy at lower band (with confirmation).
    """
    if len(df) < 30:
        return None
    close = df['close'].values
    upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    if np.isnan(upper[-1]) or np.isnan(lower[-1]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    # buy when price touches or goes below lower band
    if entry <= lower[-1]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY", target_atr=2.0, sl_atr=1.0)
        score = float((lower[-1] - entry) / (abs(entry) + 1e-6))
        return dict(action="BUY", target=target, stoploss=stoploss, score=abs(score))
    # sell when price touches or goes above upper band
    if entry >= upper[-1]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL", target_atr=2.0, sl_atr=1.0)
        score = float((entry - upper[-1]) / (abs(entry) + 1e-6))
        return dict(action="SELL", target=target, stoploss=stoploss, score=abs(score))
    return None

def strat_rsi_momentum(df, period=14):
    """
    RSI-based momentum: buy on RSI crossing up from oversold or crossing above 50; sell on reverse.
    """
    if len(df) < period + 5:
        return None
    close = df['close'].values
    rsi = talib.RSI(close, timeperiod=period)
    if np.isnan(rsi[-1]) or np.isnan(rsi[-2]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    # cross from oversold
    if rsi[-2] < 30 and rsi[-1] >= 30:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY")
        score = float((rsi[-1] - 30) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    # cross above neutral 50
    if rsi[-2] < 50 and rsi[-1] >= 50:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY")
        score = float((rsi[-1] - 50) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    # sell signals
    if rsi[-2] > 70 and rsi[-1] <= 70:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL")
        score = float((70 - rsi[-1]) / 100.0)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    if rsi[-2] > 50 and rsi[-1] <= 50:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL")
        score = float((50 - rsi[-1]) / 100.0)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

def strat_stochastic(df, fastk=14, slowk=3, slowd=3):
    """
    Stochastic oscillator crossovers (slowk crosses slowd).
    """
    if len(df) < fastk + slowk + slowd:
        return None
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    slowk_arr, slowd_arr = talib.STOCH(high, low, close, fastk_period=fastk,
                                       slowk_period=slowk, slowk_matype=0,
                                       slowd_period=slowd, slowd_matype=0)
    if np.isnan(slowk_arr[-1]) or np.isnan(slowd_arr[-1]) or np.isnan(slowk_arr[-2]) or np.isnan(slowd_arr[-2]):
        return None
    entry = float(df['close'].iloc[-1])
    atr_val = atr(df, timeperiod=ATR_PERIOD)[-1]
    # buy when K crosses above D (and both below 50)
    if slowk_arr[-1] > slowd_arr[-1] and slowk_arr[-2] <= slowd_arr[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="BUY")
        score = float((slowd_arr[-1] - slowk_arr[-1]) / (entry + 1e-6))
        return dict(action="BUY", target=target, stoploss=stoploss, score=abs(score) + (50 - min(slowk_arr[-1], slowd_arr[-1]))/100.0)
    # sell when K crosses below D (and both above 50)
    if slowk_arr[-1] < slowd_arr[-1] and slowk_arr[-2] >= slowd_arr[-2]:
        target, stoploss = price_based_target_sl(entry, atr_val, action="SELL")
        score = float((slowk_arr[-1] - slowd_arr[-1]) / (entry + 1e-6))
        return dict(action="SELL", target=target, stoploss=stoploss, score=abs(score) + (max(slowk_arr[-1], slowd_arr[-1]) - 50)/100.0)
    return None

def strat_volatility_trend_atr(df):
    """
    Simple volatility trend using ATR vs moving average of ATR + price trend:
    - Buy if ATR increasing and price above SMA
    - Sell if ATR increasing and price below SMA (trend with volatility)
    """
    if len(df) < ATR_PERIOD + SMA_PERIOD + 5:
        return None
    close = df['close']
    atr_series = pd.Series(atr(df, timeperiod=ATR_PERIOD))
    if np.isnan(atr_series.iloc[-1]):
        return None
    atr_now = atr_series.iloc[-1]
    atr_mean = atr_series.tail(10).mean()
    sma_now = calculate_sma(df, period=SMA_PERIOD)
    entry = float(close.iloc[-1])
    # trending higher volatility
    atr_increasing = atr_now > atr_mean
    if atr_increasing and entry > sma_now:
        target, stoploss = price_based_target_sl(entry, atr_now, action="BUY", target_atr=2.5, sl_atr=1.5)
        score = float((atr_now - atr_mean) / (atr_mean + 1e-6))
        return dict(action="BUY", target=target, stoploss=stoploss, score=max(0.01, score))
    if atr_increasing and entry < sma_now:
        target, stoploss = price_based_target_sl(entry, atr_now, action="SELL", target_atr=2.5, sl_atr=1.5)
        score = float((atr_now - atr_mean) / (atr_mean + 1e-6))
        return dict(action="SELL", target=target, stoploss=stoploss, score=max(0.01, score))
    return None

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
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=3.0, sl_atr=1.5,action="BUY")
        score = float((aroon_up[-1] - aroon_down[-1]) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if aroon_down[-1] > aroon_up[-1] and aroon_down[-2] <= aroon_up[-2] and entry < long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=3.0, sl_atr=1.5,action="SELL")
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
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=3.0, sl_atr=1.5,action="BUY")
        score = float((ema_short[-1] - ema_long[-1]) / entry)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)
    if ema_long[-1] > ema_short[-1] and ema_long[-2] <= ema_short[-2] and entry < long_ema:
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=3.0, sl_atr=1.5,action="SELL")
        score = float((ema_long[-1] - ema_short[-1]) / entry)
        return dict(action="SELL", target=target, stoploss=stoploss, score=score)
    return None

# register strategies
strategies = {
    "aroon": strat_aroon_crossover,
    "ema": strat_ema_crossover,
    "macd": strat_macd_signal,
    "vwap": strat_vwap_breakout,
    "bollinger": strat_bollinger_reversion,
    "rsi": strat_rsi_momentum,
    "stochastic": strat_stochastic,
    "volatility_atr": strat_volatility_trend_atr,
}

# If you already have implementations of aroon and ema earlier in file, they will be used.
# If not present, ensure you include those implementations (from your original code).

# --- Evaluate and Execute ---
def evaluate_and_execute(symbol):
    global DAILY_PNL
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    # end-of-day guard
    if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 15):
        return
    if DAILY_PNL >= DAILY_TARGET:
        print(f"[INFO] Daily target reached ({DAILY_PNL}). Skipping trades.")
        return
    if DAILY_PNL <= DAILY_MAX_LOSS:
        print(f"[INFO] Daily max loss reached ({DAILY_PNL}). Skipping trades.")
        return
    last = last_trade_time.get(symbol)
    if last and (now_ist - last).total_seconds() < SYMBOL_COOLDOWN_MIN * 60:
        return
    # only trade if symbol is in the configured NIFTY50 list
    if symbol not in SYMBOLS:
        return
    try:
        df = fetch_historical(symbol, interval=INTERVAL, days=7)
    except Exception as e:
        print(f"[ERROR] Fetching historical data for {symbol}: {e}")
        return
    if df.empty or len(df) < 50:
        return
    sma = calculate_sma(df)
    signals = []
    for name, strategy_func in strategies.items():
        try:
            result = strategy_func(df)
            if result:
                # sanity: check keys
                if all(k in result for k in ['action','target','stoploss','score']):
                    result['strategy'] = name
                    signals.append(result)
        except Exception as e:
            print(f"[ERROR] Strategy {name} failed for {symbol}: {e}")
    if not signals:
        return
    # weight signals by score
    weighted_scores = {"BUY": 0.0, "SELL": 0.0}
    for sig in signals:
        weighted_scores[sig['action']] += sig['score']
    best_action = max(weighted_scores, key=weighted_scores.get)
    if weighted_scores[best_action] <= 0 or weighted_scores[best_action] < 0.02:
        return
    # choose best single signal in direction
    chosen = max((sig for sig in signals if sig['action'] == best_action), key=lambda x: x['score'])
    action = chosen['action']
    proposed_entry = float(df['close'].iloc[-1])
    stoploss = chosen['stoploss']
    target = chosen['target']
    strategy_name = chosen['strategy']
    reward = abs(target - proposed_entry)
    risk = abs(stoploss - proposed_entry)
    if action == "BUY" and proposed_entry < sma:
        return
    if action == "SELL" and proposed_entry > sma:
        return
    if risk == 0 or reward / risk < 2:
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
    if exposure > available_balance:
        print(f"[SKIP] Insufficient funds for {symbol}")
        return
    # Place order
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
        print(f"[ORDER_PLACED] {action} {qty} {symbol}, order_id={order_id}")
    except Exception as e:
        print(f"[ERROR] Order failed for {symbol}: {e}")
        return
    executed_price = None
    try:
        executed_price = float(kite.ltp(f"{EXCHANGE}:{symbol}")[f"{EXCHANGE}:{symbol}"]["last_price"])
    except Exception as e:
        print(f"[WARN] Couldn't fetch executed price; using proposed entry. {e}")
        executed_price = proposed_entry
    positions[symbol] = dict(side=action, qty=qty, entry=executed_price, target=target, stoploss=stoploss, strategy=strategy_name, order_id=order_id)
    last_trade_time[symbol] = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    log_trade({
        "Date": now_ist.replace(tzinfo=None),
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
                strategy_name = positions[symbol].get('strategy')
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
                    order_id = place_exit_order(symbol, side, quantity)
                    pnl = round((ltp - entry) * quantity * (1 if side == "BUY" else -1), 2)
                    update_trade(symbol, ltp, pnl, order_id)
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
