import urllib.request
import pandas as pd
import json

PAIRS = ['BTCBUSD','ETHBUSD','DOGEBUSD']

def read_symbol(symbol):
    URL = 'https://api.binance.com/api/v3/klines?symbol={}&interval=1m&limit=1'
    request = urllib.request.urlopen(URL.format(symbol))
    if request.getcode() != 200:
        return None
    else:
        df = json.loads(request.read())
        return df

def main(args):
    result = []
    for PAIR in PAIRS:
        result.append(read_symbol(PAIR))
    return result
  