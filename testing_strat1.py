"""
Zerodha Aroon + Multi-strategy selector skeleton
- Adds simple Aroon crossover strategy port
- Framework to add more strategies (EMA, VWAP, RSI, etc.)
- Places market orders and manages target/stoploss
"""

import time
import datetime
import threading
import pandas as pd
import numpy as np
import talib
from kiteconnect import KiteConnect, KiteTicker
from kite_client import kite

# ---------- CONFIG ----------
API_KEY = "bzr39uzdxj8keovr"
# ACCESS_TOKEN = "lORvf6uLCKSzqiJ5dM4WKtnHgSUK6PpV"   # get via Kite Connect flow
EXCHANGE = "NSE"                    # or "NSE" / "BSE"
SYMBOLS = ["ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BHARTIARTL", "BRITANNIA",
    "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM",'ICICIBANK','RELIANCE','SHRIRAMFIN','BAJFINANCE','EICHERMOT','JSWSTEEL','ASIANPAINT','TECHM','AXISBANK']      # list of tradingsymbols you want to watch
INTERVAL = "5minute"                # kite interval string
TIMEPERIOD = 14                     # typical Aroon period
LOT_SIZE = 1                        # replace with actual lot size or quantity logic
MAX_POSITIONS = 3                   # max concurrent positions
RISK_PER_TRADE = 0.01               # 1% of equity
EQUITY = 100000                     # placeholder for position sizing
# -----------------------------

# kite = KiteConnect(API_KEY)
# kite.set_access_token(ACCESS_TOKEN)

# ---------- Utilities ----------
def fetch_historical(symbol, interval=INTERVAL, days=3):
    """Fetch historical candles as pandas DataFrame (most recent first)."""
    instrument = f"{EXCHANGE}:{symbol}"
    token_entry = kite.ltp(instrument).get(instrument)
    if token_entry is None:
        raise RuntimeError(f"Couldn't fetch instrument token for {instrument}")
    token = token_entry["instrument_token"]
    to_date = datetime.datetime.now()
    from_date = to_date - datetime.timedelta(days=days)
    data = kite.historical_data(token, from_date, to_date, interval)
    df = pd.DataFrame(data)
    # ensure columns: date, open, high, low, close, volume
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    df.set_index('date', inplace=True)
    return df

# ---------- Example strategy implementations ----------
"""
multi_strategy_zk.py

Ported strategy functions (Aroon, EMA crossover, MACD, VWAP, Bollinger, RSI,
Stochastic, Volatility Trend ATR) returning a unified signal dict:
    { action: "BUY"/"SELL", target: float, stoploss: float, score: float }
or None if no signal.

Dependencies: pandas, numpy, talib
"""

import numpy as np
import pandas as pd
import talib

# ---------- Helper utils ----------
def atr(df, timeperiod=14):
    """Return ATR series (uses talib)."""
    try:
        return talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=timeperiod)
    except Exception:
        return np.full(len(df), np.nan)

def last_safe(series, n=1):
    """Return last n entries safely (series can be numpy or pd.Series)."""
    if len(series) < n:
        return None
    return series[-n:]

def price_based_target_sl(entry_price, atr_val, target_atr=1.5, sl_atr=1.0, min_pct_target=0.002):
    """
    Build target & stoploss from ATR. Ensures a minimum percent target to avoid tiny legs.
    """
    if atr_val is None or np.isnan(atr_val) or atr_val <= 0:
        # fallback to percent-based
        target = entry_price * (1 + max(min_pct_target, 0.01))
        stoploss = entry_price * (1 - max(min_pct_target/2, 0.005))
        return target, stoploss

    target = entry_price + target_atr * atr_val
    stoploss = entry_price - sl_atr * atr_val
    # ensure min percentage
    if (target - entry_price) / entry_price < min_pct_target:
        target = entry_price * (1 + min_pct_target)
    if (entry_price - stoploss) / entry_price < min_pct_target/2:
        stoploss = entry_price * (1 - min_pct_target/2)
    return float(target), float(stoploss)

# ---------- Strategy implementations ----------

def strat_aroon_crossover(df, timeperiod=14):
    """
    Aroon crossover:
    - BUY when Aroon Up crosses above Aroon Down
    - SELL when Aroon Down crosses above Aroon Up
    Score: difference between Aroon lines (higher -> stronger)
    """
    if len(df) < timeperiod + 3:
        return None
    aroon_down, aroon_up = talib.AROON(df['high'].values, df['low'].values, timeperiod=timeperiod)
    if np.isnan(aroon_up[-1]) or np.isnan(aroon_down[-1]) or np.isnan(aroon_up[-2]) or np.isnan(aroon_down[-2]):
        return None

    # upward crossover
    if aroon_up[-1] > aroon_down[-1] and aroon_up[-2] <= aroon_down[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.5, sl_atr=1.0)
        score = float((aroon_up[-1] - aroon_down[-1]) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # downward crossover
    if aroon_down[-1] > aroon_up[-1] and aroon_down[-2] <= aroon_up[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.5, sl_atr=1.0)
        # invert for short
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((aroon_down[-1] - aroon_up[-1]) / 100.0))

    return None


def strat_ema_crossover(df, short_period=9, long_period=21):
    """
    EMA crossover:
    - BUY when EMA(short) crosses above EMA(long)
    - SELL when EMA(long) crosses above EMA(short)
    Score: distance between EMAs normalized by price
    """
    if len(df) < long_period + 3:
        return None
    close = df['close'].values
    ema_short = talib.EMA(close, timeperiod=short_period)
    ema_long = talib.EMA(close, timeperiod=long_period)
    if np.isnan(ema_short[-1]) or np.isnan(ema_long[-1]) or np.isnan(ema_short[-2]) or np.isnan(ema_long[-2]):
        return None

    # bullish cross
    if ema_short[-1] > ema_long[-1] and ema_short[-2] <= ema_long[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.8)
        score = float((ema_short[-1] - ema_long[-1]) / entry)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # bearish cross
    if ema_long[-1] > ema_short[-1] and ema_long[-2] <= ema_short[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.8)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((ema_long[-1] - ema_short[-1]) / entry))

    return None


def strat_macd_signal(df, fastperiod=12, slowperiod=26, signalperiod=9):
    """
    MACD signal:
    - BUY when MACD crosses above signal
    - SELL when MACD crosses below signal
    Score: MACD - signal last value
    """
    if len(df) < slowperiod + signalperiod + 3:
        return None
    close = df['close'].values
    macd, macdsignal, macdhist = talib.MACD(close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    if np.isnan(macd[-1]) or np.isnan(macdsignal[-1]) or np.isnan(macd[-2]) or np.isnan(macdsignal[-2]):
        return None

    # bullish cross
    if macd[-1] > macdsignal[-1] and macd[-2] <= macdsignal[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.2, sl_atr=1.0)
        score = float(macd[-1] - macdsignal[-1])
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # bearish cross
    if macd[-1] < macdsignal[-1] and macd[-2] >= macdsignal[-2]:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.2, sl_atr=1.0)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float(macdsignal[-1] - macd[-1]))

    return None


def strat_vwap_breakout(df):
    """
    VWAP breakout (intraday):
    - BUY if price closes above VWAP from below (momentum)
    - SELL if price closes below VWAP from above
    Uses cumulative typical price * volume / cumulative volume (intraday)
    Note: data MUST be intraday (same day) for correct VWAP.
    """
    # need intraday same-day series; if df covers multiple days, compute per-day VWAP last day
    if len(df) < 10:
        return None

    # compute VWAP over entire df (works for short intraday windows)
    tp = (df['high'] + df['low'] + df['close']) / 3.0
    cum_tp_v = (tp * df['volume']).cumsum()
    cum_vol = df['volume'].cumsum()
    vwap = cum_tp_v / cum_vol
    if len(vwap) < 2 or np.isnan(vwap.iloc[-1]) or np.isnan(vwap.iloc[-2]):
        return None

    last_close = float(df['close'].iloc[-1])
    prev_close = float(df['close'].iloc[-2])
    last_vwap = float(vwap.iloc[-1])
    prev_vwap = float(vwap.iloc[-2])

    # bullish breakout: close crossed above vwap
    if prev_close <= prev_vwap and last_close > last_vwap:
        entry = last_close
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.2, sl_atr=0.8)
        score = float((last_close - last_vwap) / last_vwap)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # bearish breakout
    if prev_close >= prev_vwap and last_close < last_vwap:
        entry = last_close
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.2, sl_atr=0.8)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((last_vwap - last_close) / last_vwap))

    return None


def strat_bollinger_reversion(df, timeperiod=20, nbdev=2):
    """
    Bollinger Band mean reversion:
    - BUY when price closes below lower band (expect reversion)
    - SELL when price closes above upper band
    Score: normalized distance from band
    """
    if len(df) < timeperiod + 3:
        return None
    close = df['close'].values
    upper, middle, lower = talib.BBANDS(close, timeperiod=timeperiod, nbdevup=nbdev, nbdevdn=nbdev)
    if np.isnan(upper[-1]) or np.isnan(lower[-1]):
        return None

    last_close = float(df['close'].iloc[-1])
    # mean reversion - buy at lower band
    if last_close < lower[-1]:
        entry = last_close
        atr_val = atr(df, timeperiod=14)[-1]
        # for reversion use closer target to middle band
        target = float(middle[-1])
        stoploss = float(entry - max(0.5 * (middle[-1] - lower[-1]), atr_val if not np.isnan(atr_val) else 0.01))
        score = float((lower[-1] - last_close) / last_close)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    if last_close > upper[-1]:
        entry = last_close
        atr_val = atr(df, timeperiod=14)[-1]
        target = float(middle[-1])
        stoploss = float(entry + max(0.5 * (upper[-1] - middle[-1]), atr_val if not np.isnan(atr_val) else 0.01))
        score = float((last_close - upper[-1]) / last_close)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (stoploss - entry)), score=score)

    return None


def strat_rsi_momentum(df, rsi_period=14, overbought=70, oversold=30):
    """
    RSI momentum:
    - BUY when RSI crosses above oversold (e.g., crosses up 30)
    - SELL when RSI crosses below overbought (e.g., crosses down 70)
    Score: distance from threshold
    """
    if len(df) < rsi_period + 3:
        return None
    close = df['close'].values
    rsi = talib.RSI(close, timeperiod=rsi_period)
    if np.isnan(rsi[-1]) or np.isnan(rsi[-2]):
        return None

    # bullish - RSI crossing up from oversold
    if rsi[-1] > oversold and rsi[-2] <= oversold:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.8)
        score = float((rsi[-1] - oversold) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # bearish - RSI crossing down from overbought
    if rsi[-1] < overbought and rsi[-2] >= overbought:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.8)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((overbought - rsi[-1]) / 100.0))

    return None


def strat_stochastic(df, k_period=14, d_period=3, smooth_k=3):
    """
    Slow Stochastic crossover:
    - BUY when %K crosses above %D below the oversold threshold
    - SELL when %K crosses below %D above the overbought threshold
    Score: absolute %K-%D difference
    """
    if len(df) < k_period + smooth_k + d_period:
        return None
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    slowk, slowd = talib.STOCH(high, low, close, fastk_period=k_period, slowk_period=smooth_k, slowk_matype=0, slowd_period=d_period, slowd_matype=0)
    if np.isnan(slowk[-1]) or np.isnan(slowd[-1]) or np.isnan(slowk[-2]) or np.isnan(slowd[-2]):
        return None

    oversold = 20
    overbought = 80

    # bullish crossover below oversold
    if slowk[-1] > slowd[-1] and slowk[-2] <= slowd[-2] and slowk[-1] < oversold:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.75)
        score = float((slowk[-1] - slowd[-1]) / 100.0)
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # bearish crossover above overbought
    if slowk[-1] < slowd[-1] and slowk[-2] >= slowd[-2] and slowk[-1] > overbought:
        entry = float(df['close'].iloc[-1])
        atr_val = atr(df, timeperiod=14)[-1]
        target, stoploss = price_based_target_sl(entry, atr_val, target_atr=1.0, sl_atr=0.75)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((slowd[-1] - slowk[-1]) / 100.0))

    return None


def strat_volatility_trend_atr(df, atr_period=14, atr_mult=1.5):
    """
    Volatility Trend using ATR:
    - BUY when price moves above a recent smoothed close and ATR indicates rising volatility
    - SELL on opposite
    It's a simple trend + volatility filter.
    Score: ATR normalized
    """
    if len(df) < atr_period + 5:
        return None
    close = df['close'].values
    atr_series = atr(df, timeperiod=atr_period)
    sma_close = talib.SMA(close, timeperiod=atr_period)
    if np.isnan(atr_series[-1]) or np.isnan(atr_series[-2]) or np.isnan(sma_close[-1]):
        return None

    last_close = float(df['close'].iloc[-1])
    prev_close = float(df['close'].iloc[-2])
    atr_now = float(atr_series[-1])
    atr_prev = float(atr_series[-2])

    # rising volatility and price above sma -> continue long
    if (atr_now > atr_prev * 1.0) and (last_close > sma_close[-1]) and (prev_close <= sma_close[-2] if len(sma_close) >= 2 else True):
        entry = last_close
        target, stoploss = price_based_target_sl(entry, atr_now, target_atr=atr_mult, sl_atr=1.0)
        score = float((atr_now - atr_prev) / (atr_prev + 1e-8))
        return dict(action="BUY", target=target, stoploss=stoploss, score=score)

    # falling below sma with rising volatility -> short/sell
    if (atr_now > atr_prev * 1.0) and (last_close < sma_close[-1]) and (prev_close >= sma_close[-2] if len(sma_close) >= 2 else True):
        entry = last_close
        target, stoploss = price_based_target_sl(entry, atr_now, target_atr=atr_mult, sl_atr=1.0)
        return dict(action="SELL", target=float(entry - (target - entry)), stoploss=float(entry + (entry - stoploss)), score=float((atr_now - atr_prev) / (atr_prev + 1e-8)))

    return None


# ---------- Strategy registry ----------
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

# # If you want to test locally:
# if __name__ == "__main__":
#     # small demonstration stub (requires df to be prepared)
#     print("Module loaded. Import strategies and call with a pandas DataFrame (index date, cols: open, high, low, close, volume).")


# Placeholder for other strategy functions (EMA crossover, VWAP, RSI, etc.)
def strat_dummy_hold(df):
    return None

# # Register strategies here (order indicates priority if you want)
# strategies = {
#     "aroon": strat_aroon_crossover,
#     # "ema": strat_ema_crossover,
#     # "vwap": strat_vwap,
#     # add more...
# }

# ---------- Position manager ----------
positions = {}  # key: symbol -> dict with order_id, side, qty, entry_price, target, stoploss, strategy

# def calculate_qty(entry_price, stoploss):
#     """Simple position sizing: risk per trade fixed percent of EQUITY"""
#     risk_amount = EQUITY * RISK_PER_TRADE
#     per_share_risk = abs(entry_price - stoploss)
#     if per_share_risk <= 0:
#         return 0
#     qty = int(risk_amount / per_share_risk)
#     return max(1, qty)

def calculate_qty(entry, stoploss, available_balance=None):
    risk_amount = available_balance * RISK_PER_TRADE
    if stoploss == 0:
        return 0
    qty = int(risk_amount / abs(entry - stoploss))
    return qty


def place_market_order(symbol, action, qty):
    """Place a market order via Kite."""
    # tx_type = kite.TRANSACTION_TYPE_BUY if action == "BUY" else kite.TRANSACTION_TYPE_SELL
    # try:
    #     order_id = kite.place_order(
    #         variety=kite.VARIETY_REGULAR,
    #         exchange=EXCHANGE,
    #         tradingsymbol=symbol,
    #         transaction_type=tx_type,
    #         quantity=qty,
    #         order_type=kite.ORDER_TYPE_MARKET,
    #         product=kite.PRODUCT_MIS
    #     )
    #     return order_id
    # except Exception as e:
    #     print("Order placement failed:", e)
    #     return None
    print(f"Simulated order: {action} {qty} of {symbol}")

def exit_position(symbol):
    pos = positions.get(symbol)
    if not pos:
        return
    # place opposite market order to square off
    side = "SELL" if pos['side'] == "BUY" else "BUY"
    print(f"Exiting {symbol} side {pos['side']} qty {pos['qty']}")
    place_market_order(symbol, side, pos['qty'])
    positions.pop(symbol, None)

# ---------- Strategy selector & executor ----------
def evaluate_and_execute(symbol):
    # 1. fetch data
    df = fetch_historical(symbol, interval=INTERVAL, days=3)

    # 2. run all strategies, collect signals
    signals = []
    for name, func in strategies.items():
        try:
            s = func(df)
            if s:
                s['strategy'] = name
                signals.append(s)
        except Exception as e:
            print(f"Strategy {name} failed for {symbol}: {e}")

    if not signals:
        # nothing to do
        return

    # 3. choose best signal (highest score)
    signals = sorted(signals, key=lambda x: x['score'] if x.get('score') else 0, reverse=True)
    chosen = signals[0]
    action = chosen['action']
    target = chosen['target']
    stoploss = chosen['stoploss']
    strategy_name = chosen['strategy']

    # 4. check if already in position on symbol
    if symbol in positions:
        # If in position and signal reversed, exit
        if positions[symbol]['side'] != action:
            print(f"Signal reversed on {symbol} ({positions[symbol]['side']} -> {action}) - exiting.")
            exit_position(symbol)
        else:
            # same side - maybe update target/SL or do nothing
            print(f"Already long/short on {symbol} from {positions[symbol]['strategy']}.")
        return

    # 5. check max positions
    if len(positions) >= MAX_POSITIONS:
        print("Max positions reached. Skipping new entries.")
        return

    # 6. determine qty
    entry_price = df['close'].iloc[-1]
    qty = calculate_qty(entry_price, stoploss)
    if qty <= 0:
        print("Calculated qty 0, skipping")
        return

    print(f"Placing {action} for {symbol} qty {qty} via {strategy_name} (entry {entry_price:.2f}, target {target:.2f}, sl {stoploss:.2f})")
    order_id = place_market_order(symbol, action, qty)
    if order_id:
        positions[symbol] = dict(order_id=order_id, side=action, qty=qty, entry_price=entry_price,
                                 target=target, stoploss=stoploss, strategy=strategy_name, timestamp=datetime.datetime.now())

# ---------- Monitor loop ----------
def monitor_positions():
    """Poll LTP for SL/Target hit and exit accordingly. Run in background thread."""
    while True:
        if not positions:
            time.sleep(5)
            continue
        try:
            for symbol, pos in list(positions.items()):
                instrument = f"{EXCHANGE}:{symbol}"
                ltp = kite.ltp(instrument)[instrument]['last_price']
                # long
                if pos['side'] == "BUY":
                    if ltp >= pos['target'] or ltp <= pos['stoploss']:
                        print(f"{symbol} target/SL hit. LTP={ltp}, target={pos['target']}, sl={pos['stoploss']}")
                        exit_position(symbol)
                else:  # short
                    if ltp <= pos['target'] or ltp >= pos['stoploss']:
                        print(f"{symbol} target/SL hit (short). LTP={ltp}, target={pos['target']}, sl={pos['stoploss']}")
                        exit_position(symbol)
        except Exception as e:
            print("monitor_positions error:", e)
        time.sleep(10)

# Start monitor thread
monitor_thread = threading.Thread(target=monitor_positions, daemon=True)
monitor_thread.start()

# Main scheduling loop (runs once per interval; naive sleep-based)
def run_once():
    for symbol in SYMBOLS:
        try:
            print(f"Evaluating {symbol}...")
            evaluate_and_execute(symbol)
        except Exception as e:
            print(f"Error evaluating {symbol}: {e}")

if __name__ == "__main__":
    # One-shot run; convert to scheduler if you want run every 5-minute
    run_once()
