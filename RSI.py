import numpy as np
import pandas as pd
import talib

# --- Parameters ---
ATR_PERIOD = 14
RSI_PERIOD = 14
SL_ATR_MULTIPLIER = 1.0   # Stop-loss set at 1 ATR below entry
TP_ATR_MULTIPLIER = 3.0   # Target set at 3 ATR above entry
MAX_HOLD_DAYS = 7         # Exit if holding more than 7 days
MIN_RSI = 30              # RSI oversold threshold
MAX_RSI_EXIT = 70         # RSI overbought threshold where you may exit

# --- Function to calculate ATR ---
def calculate_atr(df, period=ATR_PERIOD):
    high = df['high']
    low = df['low']
    close = df['close']
    atr = talib.ATR(high, low, close, timeperiod=period)
    return atr

# --- Function to calculate RSI ---
def calculate_rsi(df, period=RSI_PERIOD):
    close = df['close']
    rsi = talib.RSI(close, timeperiod=period)
    return rsi

# --- Generate trade signals ---
def rsi_oversold_trade(df):
    df = df.copy()
    df['ATR'] = calculate_atr(df)
    df['RSI'] = calculate_rsi(df)
    trades = []
    
    for i in range(len(df)):
        if i < ATR_PERIOD or i < RSI_PERIOD:
            continue
        
        rsi_now = df['RSI'].iloc[i]
        entry_price = df['close'].iloc[i]
        atr_now = df['ATR'].iloc[i]
        
        # Check if RSI < 30 → Buy signal
        if rsi_now < MIN_RSI:
            stop_loss = entry_price - SL_ATR_MULTIPLIER * atr_now
            target = entry_price + TP_ATR_MULTIPLIER * atr_now
            entry_date = df.index[i]
            
            # Find exit within MAX_HOLD_DAYS
            exit_price = None
            exit_date = None
            for j in range(i+1, min(i + MAX_HOLD_DAYS + 1, len(df))):
                price = df['close'].iloc[j]
                date = df.index[j]
                rsi_val = df['RSI'].iloc[j]
                
                # Check stop-loss
                if price <= stop_loss:
                    exit_price = price
                    exit_date = date
                    outcome = "SL hit"
                    break
                
                # Check target
                if price >= target:
                    exit_price = price
                    exit_date = date
                    outcome = "Target hit"
                    break
                
                # Optional: exit if RSI overbought
                if rsi_val >= MAX_RSI_EXIT:
                    exit_price = price
                    exit_date = date
                    outcome = "RSI exit"
                    break
            
            # If no exit hit, force exit at MAX_HOLD_DAYS
            if exit_price is None:
                exit_price = df['close'].iloc[min(i + MAX_HOLD_DAYS, len(df) - 1)]
                exit_date = df.index[min(i + MAX_HOLD_DAYS, len(df) - 1)]
                outcome = "Max hold time"
            
            pnl = exit_price - entry_price
            
            trades.append({
                'Entry Date': entry_date,
                'Exit Date': exit_date,
                'Entry Price': entry_price,
                'Exit Price': exit_price,
                'Stop Loss': stop_loss,
                'Target': target,
                'Outcome': outcome,
                'P&L': pnl
            })
    
    return pd.DataFrame(trades)

# --- Example usage ---
if __name__ == "__main__":
    # Load your data into df: it must have columns ['high', 'low', 'close']
    # Example loading CSV for NSE stock
    df = pd.read_csv("nse_stock.csv", index_col="date", parse_dates=True)
    
    # Run RSI oversold strategy
    trades_df = rsi_oversold_trade(df)
    
    # Show results
    print(trades_df)
