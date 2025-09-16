import time
import datetime
import pandas as pd
import threading
from kiteconnect import KiteConnect
from openpyxl import load_workbook
import access_token
from testing_strat1 import strategies, fetch_historical, calculate_qty
from kite_client import kite
import warnings
warnings.filterwarnings("ignore")
from zoneinfo import ZoneInfo

# --- Config ---
EXCHANGE = "NSE"
SYMBOLS = ["ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BHARTIARTL", "BRITANNIA",
    "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", 'ICICIBANK', 'RELIANCE', 'SHRIRAMFIN', 'BAJFINANCE', 'EICHERMOT',
    'JSWSTEEL', 'ASIANPAINT', 'TECHM', 'AXISBANK']
INTERVAL = "5minute"
RISK_PER_TRADE = 0.01
MAX_POSITIONS = 10
EXCEL_FILE = "strategy_trades_10sept.xlsx"
MIN_QTY = 5  # Do not trade if computed quantity is below this threshold

# --- Globals ---
positions = {}
last_squareoff_date = None  # Track last date we executed 3:15 PM square-off

# --- Excel Setup ---
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
    idx = df[(df['Symbol'] == symbol) & (df['Status'] == "OPEN")].index[-1]
    df.loc[idx, "Exit"] = exit_price
    df.loc[idx, "PnL"] = pnl
    df.loc[idx, "Status"] = "CLOSED"
    df.to_excel(EXCEL_FILE, index=False)

# --- Get available balance with buffer ---
def get_available_balance():
    try:
        margins = kite.margins("equity")
        available = margins['available']['live_balance']
        
        # Define buffer to avoid margin errors – e.g., ₹500
        BUFFER_AMOUNT = 500
        
        adjusted_available = max(0, available - BUFFER_AMOUNT)
        
        return adjusted_available
    except Exception as e:
        print("[ERROR] Fetching balance failed:", e)
        return 0

# --- Calculate quantity considering charges ---
def calculate_qty(entry_price, stoploss_price, available_balance):
    # Approximate brokerage and charges as a percentage of trade value
    BROKERAGE_PERCENTAGE = 0.0003  # 0.03%
    CHARGES_PERCENTAGE = 0.0005    # 0.05%
    
    per_share_cost = entry_price * (1 + BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)
    max_possible_qty = int(available_balance / per_share_cost)
    
    return max_possible_qty

# --- Estimate total charges for the trade ---
def estimate_charges(entry_price, qty):
    BROKERAGE_PERCENTAGE = 0.0003  # 0.03%
    CHARGES_PERCENTAGE = 0.0005    # 0.05%
    
    total_cost_per_share = entry_price * (BROKERAGE_PERCENTAGE + CHARGES_PERCENTAGE)
    total_charges = total_cost_per_share * qty
    return total_charges

# --- Strategy evaluation and execution ---
def evaluate_and_execute(symbol):
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 15):
        print(f"[INFO] Market closed for new trades after 3:15 PM. Skipping {symbol}.")
        return
    df = fetch_historical(symbol, interval=INTERVAL, days=3)

    signals = []
    for name, strategy_func in strategies.items():
        try:
            result = strategy_func(df)
            if result:
                result['strategy'] = name
                signals.append(result)
        except Exception as e:
            print(f"[ERROR] Strategy '{name}' failed for {symbol}: {e}")

    if not signals:
        return

    chosen = max(signals, key=lambda x: x['score'])
    action = chosen['action']
    target = chosen['target']
    stoploss = chosen['stoploss']
    strategy_name = chosen['strategy']
    entry_price = float(df['close'].iloc[-1])

    available_balance = get_available_balance()
    qty = calculate_qty(entry_price, stoploss, available_balance)

    if qty <= 0:
        print(f"[SKIP] Not enough balance to trade {symbol}. Available: {available_balance}")
        return

    if qty < MIN_QTY:
        print(f"[SKIP] Qty {qty} below minimum {MIN_QTY} for {symbol} (avoid fees)")
        return

    if symbol in positions:
        print(f"[SKIP] Already in trade for {symbol}")
        return

    if len(positions) >= MAX_POSITIONS:
        print(f"[SKIP] Max positions reached ({MAX_POSITIONS})")
        return

    reward = abs(target - entry_price)
    risk = abs(stoploss - entry_price)
    if risk == 0 or reward / risk < 1.5:
        print(f"[SKIP] Low reward/risk ratio for {symbol}")
        return


    expected_profit_per_share = abs(target - entry_price)
    total_expected_profit = expected_profit_per_share * qty
    total_charges = estimate_charges(entry_price, qty)

    if total_expected_profit <= total_charges:
        print(f"[SKIP] Trade not profitable after charges for {symbol}. Expected Profit: {total_expected_profit}, Charges: {total_charges}")
        return

    expected_exposure = entry_price * qty
    if expected_exposure > available_balance:
        print(f"[SKIP] Insufficient funds for {symbol}. Exposure: {expected_exposure}, Available: {available_balance}")
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
        print(f"[ORDER] Placed {action} order for {qty} {symbol} at market price {entry_price} "
              f"(Intended SL={stoploss}, Target={target}, Strategy={strategy_name})")
    except Exception as e:
        print(f"[ERROR] Order placement failed for {symbol}: {e}")
        return

    positions[symbol] = dict(side=action, qty=qty, entry=entry_price,
                             target=target, stoploss=stoploss, strategy=strategy_name)

    log_trade({
        "Date": datetime.datetime.now(),
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

# --- Exit helper: place opposite market order ---
def place_exit_order(symbol, side, qty):
    try:
        opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY
        exit_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity=kite.VALIDITY_DAY
        )
        return exit_order_id
    except Exception as e:
        print(f"[ERROR] Exit order failed for {symbol}: {e}")
        return None

# --- Live positions helpers (from Zerodha) ---
def fetch_live_net_positions():
    """Return list of dicts with net live positions from Zerodha."""
    try:
        pos = kite.positions()
        return pos.get('net', [])
    except Exception as e:
        print("[ERROR] Fetching live positions failed:", e)
        return []

def squareoff_live_positions():
    """Square-off all live MIS positions based on actual net quantities from Zerodha."""
    live = fetch_live_net_positions()
    for p in live:
        try:
            tradingsymbol = p.get('tradingsymbol') or p.get('tradingsymbol'.upper(), '')
            exchange_code = p.get('exchange', EXCHANGE)
            product = p.get('product', '')
            quantity = int(p.get('quantity', 0))
            if quantity == 0:
                continue
            # Only square-off intraday MIS positions
            if product != kite.PRODUCT_MIS:
                continue
            tx_type = kite.TRANSACTION_TYPE_SELL if quantity > 0 else kite.TRANSACTION_TYPE_BUY
            qty = abs(quantity)
            try:
                exit_order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=exchange_code,
                    tradingsymbol=tradingsymbol,
                    transaction_type=tx_type,
                    quantity=qty,
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                    validity=kite.VALIDITY_DAY
                )
                print(f"[SQUARE-OFF LIVE] {tradingsymbol} qty={qty} exit_order_id={exit_order_id}")
            except Exception as ex:
                print(f"[ERROR] Square-off failed for {tradingsymbol}: {ex}")
        except Exception as inner:
            print("[ERROR] Processing live position failed:", inner)

# --- Monitor open positions ---
def monitor_positions():
    while True:
        if not positions:
            time.sleep(5)
            continue
        try:
            # Daily square-off at 15:15 IST to avoid carryover/charges
            global last_squareoff_date
            now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
            today_ist = now_ist.date()
            if (now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 15)) and last_squareoff_date != today_ist:
                # Prefer squaring off based on actual live positions from broker
                squareoff_live_positions()
                # Best-effort: update any locally tracked positions for logging
                for symbol, pos in list(positions.items()):
                    try:
                        ltp = kite.ltp(f"{EXCHANGE}:{symbol}")[f"{EXCHANGE}:{symbol}"]["last_price"]
                        side, qty, entry = pos['side'], pos['qty'], pos['entry']
                        pnl = (ltp - entry) * qty if side == "BUY" else (entry - ltp) * qty
                        update_trade(symbol, ltp, pnl)
                        positions.pop(symbol)
                        print(f"[SQUARE-OFF LOG] {symbol} closed locally @ {ltp}, PnL={pnl}")
                    except Exception as log_ex:
                        print("[WARN] Local square-off log failed:", log_ex)
                last_squareoff_date = today_ist

            for symbol, pos in list(positions.items()):
                ltp = kite.ltp(f"{EXCHANGE}:{symbol}")[f"{EXCHANGE}:{symbol}"]["last_price"]
                side, qty, entry, target, stoploss = pos['side'], pos['qty'], pos['entry'], pos['target'], pos['stoploss']

                if side == "BUY":
                    if ltp >= target or ltp <= stoploss:
                        pnl = (ltp - entry) * qty
                        exit_id = place_exit_order(symbol, side, qty)
                        update_trade(symbol, ltp, pnl)
                        positions.pop(symbol)
                        print(f"[EXIT] {symbol} BUY exit @ {ltp}, PnL={pnl}, exit_order_id={exit_id}")
                else:
                    if ltp <= target or ltp >= stoploss:
                        pnl = (entry - ltp) * qty
                        exit_id = place_exit_order(symbol, side, qty)
                        update_trade(symbol, ltp, pnl)
                        positions.pop(symbol)
                        print(f"[EXIT] {symbol} SELL exit @ {ltp}, PnL={pnl}, exit_order_id={exit_id}")
        except Exception as e:
            print("[ERROR] Monitor error:", e)
        time.sleep(10)

# --- Run evaluation once per interval ---
def run_once():
    for s in SYMBOLS:
        try:
            evaluate_and_execute(s)
        except Exception as e:
            print(f"[ERROR] Running evaluation for {s}: {e}")

# --- Main ---
if __name__ == "__main__":
    init_excel()
    threading.Thread(target=monitor_positions, daemon=True).start()
    while True:
        run_once()
        time.sleep(300)  # Run every 5 minutes
