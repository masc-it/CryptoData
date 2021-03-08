# original repo: https://github.com/gosuto-ai/candlestick_retriever

import json
import os
import random
import subprocess
import time
from datetime import date, datetime, timedelta
import pyarrow.parquet as pq
import requests
import pandas as pd
from threading import Thread
import preprocessing as pp

API_BASE = 'https://api.binance.com/api/v3/'

LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'ignore'
]


def get_batch(symbol, interval='1m', start_time=0, limit=4000):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON into a pandas
    dataframe and return it. If not successful, return an empty dataframe.
    """

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'limit': limit
    }
    try:
        # timeout should also be given as a parameter to the function
        response = requests.get(f'{API_BASE}klines', params, timeout=30)
    except requests.exceptions.ConnectionError:
        print('Connection error, Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.Timeout:
        print('Timeout, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.ConnectionResetError:
        print('Connection reset by peer, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    if response.status_code == 200:
        return pd.DataFrame(response.json(), columns=LABELS)
    print(f'Got erroneous response back: {response}')
    return pd.DataFrame([])



def get_parquet_info(filepath):
    """
    Reads and returns the last timestamp and number of candles in a parquet file.
    """
    last_timestamp = 0
    old_lines = 0
    try:
        existing_data = pq.read_pandas(filepath).to_pandas()
        if not existing_data.empty:
            last_timestamp = int(existing_data.index.max().timestamp()*1000)
            old_lines = len(existing_data.index)
    except OSError:
        pass
    return last_timestamp, old_lines


def write_to_parquet(file, batches, base, quote, append=False):
    """
    Writes a batch of candles data to a parquet file.
    """
    df = pd.concat(batches, ignore_index=True)
    df = pp.quick_clean(df)
    if append:
        pp.append_raw_to_parquet(df, file, False)
    else:
        pp.write_raw_to_parquet(df, file, False)
    
    return len(df.index)


def gather_new_candles(base, quote, last_timestamp, interval='1m'):
    """
    Gather all candlesticks available, starting from the last timestamp loaded from disk or from beginning of time.
    Stop if the timestamp that comes back from the api is the same as the last one.
    """
    previous_timestamp = None
    batches = [pd.DataFrame([], columns=LABELS)]

    first_read = True
    start_datetime = None
    bar = None
    print(f"Last timestamp of pair {base}-{quote} was {datetime.fromtimestamp(last_timestamp / 1000)}.")
    while previous_timestamp != last_timestamp:
        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base+quote,
            interval=interval,
            start_time=last_timestamp+1,
            limit=3000
        )
        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch['open_time'].max()
        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, interval, str(last_datetime)+covering_spaces, end='\r', flush=True)
        
    return batches

    
def all_candles_to_parquet(base, quote, interval='1m'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to parquet.
    """
    filepath = f'compressed/{base}-{quote}.parquet'

    last_timestamp, old_lines = get_parquet_info(filepath)
    new_candle_batches = gather_new_candles(base, quote, last_timestamp, interval)
    return write_to_parquet(filepath, new_candle_batches, base, quote, append=True)
    
    
def all_candles_to_csv(base, quote, interval='1m'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """

    # see if there is any data saved on disk already
    try:
        batches = [pd.read_csv(f'data/{base}-{quote}.csv')]
        last_timestamp = batches[-1]['open_time'].max()
    except FileNotFoundError:
        batches = [pd.DataFrame([], columns=LABELS)]
        last_timestamp = 0
    old_lines = len(batches[-1].index)

    # gather all candlesticks available, starting from the last timestamp loaded from disk or 0
    # stop if the timestamp that comes back from the api is the same as the last one
    previous_timestamp = None

    while previous_timestamp != last_timestamp:
        # stop if we reached data from today
        #if date.fromtimestamp(last_timestamp / 1000) >= int(datetime.now().timestamp()):#date.today():
        if last_timestamp / 1000 >= int(datetime.now().timestamp()):
            break

        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base+quote,
            interval=interval,
            start_time=last_timestamp+1
        )

        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch['open_time'].max()

        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, interval, str(last_datetime)+covering_spaces, end='\r', flush=True)

    # write clean version of csv to parquet
    parquet_name = f'{base}-{quote}.parquet'
    full_path = f'compressed/{parquet_name}'
    df = pd.concat(batches, ignore_index=True)
    df = pp.quick_clean(df)
    pp.write_raw_to_parquet(df, full_path)
    
    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        df.to_csv(f'data/{base}-{quote}.csv', index=False)
        return len(df.index) - old_lines
    return 0


def get_data(base, quote, n, n_count):
    #new_lines = all_candles_to_csv(base=base, quote=quote)
    new_lines = all_candles_to_parquet(base=base, quote=quote)
    if new_lines > 0:
        print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {base}-{quote}')
    else:
        print(f'{datetime.now()} {n}/{n_count} Already up to date with {base}-{quote}')

def main():
   
    # get all pairs currently available
    all_symbols = pd.DataFrame(requests.get(f'{API_BASE}exchangeInfo').json()['symbols'])
    
    coins = ["ADA", "BTC", "COTI", "ETH", "XRP"]
    all_pairs = [tuple(x) for x in all_symbols[['baseAsset', 'quoteAsset']].to_records(index=False) if x[1] == "USDT" and x[0] in coins]
    
    print(all_pairs)
    # make sure data folders exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('compressed', exist_ok=True)

    # do a full update on all pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair
        thread = Thread(target = get_data, args = (base, quote, n, n_count))
        thread.start()
    
if __name__ == '__main__':
    main()
