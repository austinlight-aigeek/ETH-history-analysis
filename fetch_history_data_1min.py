import requests
import pandas as pd
import time
from datetime import datetime, timedelta

symbol = 'ETH-USD'
granularity = 60  # 1 minute in seconds
base_url = f'https://api.exchange.coinbase.com/products/{symbol}/candles'

# Date range for data collection
start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 2, 18)


# Coinbase API returns data in reverse chronological order
def fetch_data(start, end, granularity):
    params = {'start': start.isoformat() + 'Z', 'end': end.isoformat() + 'Z', 'granularity': granularity}
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return []


# Collecting data in chunks
all_data = []
current_start = start_date

while current_start < end_date:
    current_end = min(current_start + timedelta(minutes=300), end_date)
    data = fetch_data(current_start, current_end, granularity)

    if not data:
        print("No data returned. Stopping the fetch.")
        break

    all_data.extend(data)
    print(f"Fetched {len(all_data)} records so far...")

    # Update the start date for the next batch
    last_timestamp = data[-1][0]
    current_start = datetime.utcfromtimestamp(last_timestamp) + timedelta(seconds=granularity)

    time.sleep(1)  # Avoid hitting rate limits

# Convert to a DataFrame
if all_data:
    columns = ['timestamp', 'low', 'high', 'open', 'close', 'volume']
    df = pd.DataFrame(all_data, columns=columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    # Sort by timestamp ascending
    df = df.sort_values('timestamp')

    # Save to CSV
    df.to_csv('eth_usd_1min_coinbase.csv', index=False)
    print("Data downloaded and saved as eth_usd_1min_coinbase.csv")
else:
    print("No data fetched. CSV file will not be created.")
