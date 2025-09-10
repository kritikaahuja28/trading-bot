from kiteconnect import KiteConnect

import access_token
import datetime
import pandas as pd

def _load_api_key_from_file() -> str:
    with open("api_key.txt", 'r') as file_handle:
        parts = file_handle.read().strip().split()
    # Format: api_key api_secret user_id password totp_secret
    return parts[0]

API_KEY = _load_api_key_from_file()
ACCESS_TOKEN = access_token.get_access_token()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

INTERVAL = "5minute"
EXCHANGE = "NSE"

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