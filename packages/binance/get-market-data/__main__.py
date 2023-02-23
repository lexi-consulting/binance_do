import urllib.request
import json
from sqlalchemy import (MetaData, Table, Column, Integer, String, Sequence, create_engine, insert)
import pandas as pd
import datetime

PAIRS = ['BTCBUSD','ETHBUSD','DOGEBUSD']

CONN = create_engine(
    'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={wh}'.format(
        user='DBT',
        password='g96EaK7GfdKwC6P',
        account='nkvppyr-ud10682',
        db='BINANCE_MARKET_DATA',
        schema='PUBLIC',
        wh='COMPUTE_WH'
    )
)

def read_symbol(symbol):
    URL = 'https://api.binance.com/api/v3/klines?symbol={}&interval=1m&limit=1'
    request = urllib.request.urlopen(URL.format(symbol))
    if request.getcode() != 200:
        return None
    else:
        df = pd.DataFrame(
            json.loads(request.read()),
            columns = ['OPEN_TIME',
            'OPEN',
            'HIGH',
            'LOW',
            'CLOSE',
            'VOLUME',
            'CLOSE_TIME',
            'QUOTE_ASSET_VOLUME',
            'TRADES',
            'TAKER_BUY_BASE_ASSET_VOLUME',
            'TAKER_BUY_QUOTE_ASSET_VOLUME',
            'SYMBOL'
            ]       
        )
        df['SYMBOL'] = symbol
        return df

def create_df():
    result = []
    for PAIR in PAIRS:
        result.append(read_symbol(PAIR))
    return pd.concat(result)

market_data = create_df()

market_data['OPEN_TIME'] = pd.to_datetime(market_data['OPEN_TIME'], unit = 'ms')
market_data['CLOSE_TIME'] = pd.to_datetime(market_data['CLOSE_TIME'], unit = 'ms' )

market_data.to_sql(name="RAW_MARKET_DATA", con=CONN, if_exists='append',index=False)
