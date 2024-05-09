# Developed by Dennis - 2024-05-09 - denbe52@yahoo.com
# Retrieve prices for CANADIAN DEPOSITARY RECEIPTS (CDR) from Yahoo
# https://www.cboe.ca/en/services/raising-assets/canadian-depositary-receipts
# Save prices in a csv file that can be imported into Quicken (Canadian)

# This module uses yfinance to retrieve the prices from Yahoo
# https://github.com/ranaroussi/yfinance
# pip install yfinance[nospam]
# pip install yfinance[nospam] --upgrade

from datetime import datetime as dt
from datetime import timedelta
import time
from pathlib import Path
import pandas as pd
import numpy as np

import yfinance as yf
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


class CdrPrices:
    def __init__(self):
        # create default data set
        self.quicken_csv_file = 'Q.csv'         # default name of csv file
        self.symbols = ['AMZN.NE', 'MSFT.NE']   # default list of symbols
        self.days = 5                           # default number of calendar days of prices to download

    def load_yahoo_prices(self):
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND * 5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        session.headers['User-agent'] = 'my-program/1.01'

        file_csv = Path(__file__).parent.absolute() / Path(self.quicken_csv_file)
        print(f'Clearing data from : {file_csv}')
        with open(file_csv, 'w+') as f:
            f.close()

        begin = f'{(dt.now() - timedelta(days=self.days)):%Y-%m-%d}'  # e.g. '2020-01-31'

        dfp = pd.DataFrame()
        print(f'Loading Prices at  : {dt.now():%Y-%m-%d %H:%M:%S} into {file_csv}')
        print(f'Loading Prices from: {begin}')
        for symbol in self.symbols:
            try:
                df = yf.download(symbol, start=begin, end=None, session=session)
                df['Symbol'] = symbol
                df.drop(['Open', 'High', 'Low', 'Adj Close', 'Volume'], axis=1, inplace=True)
                df.reset_index(inplace=True)
                df.dropna(axis=0)
                dfp = pd.concat([dfp, df])
                print('Loaded Prices for: ' + symbol)
            except Exception as err:
                print(err.__class__)
                print('Error Retrieving Data for: ' + symbol)
            time.sleep(2)
        print('Loaded Prices from Yahoo for ' + str(len(self.symbols)) + ' symbols')
        dfp = dfp[['Symbol', 'Close', 'Date']]  # Re-order the columns
        dfp['Date'] = dfp['Date'].dt.strftime("%d/%m/%Y")
        dfp['Close'] = np.round(dfp['Close'], decimals=3)
        dfp.to_csv(self.quicken_csv_file, mode='w', lineterminator='', index=False, header=False)  # save to csv
        print(f'Finished Saving CDR_prices to: {self.quicken_csv_file}')


if __name__ == '__main__':
    cdr = CdrPrices()
    cdr.quicken_csv_file = 'Q.csv'
    cdr.symbols = ['AMD.NE', 'AMZN.NE', 'AVGO.NE', 'CATR.NE', 'CITI.NE', 'COST.NE',
                   'IBM.NE', 'JPM.NE', 'LLY.NE', 'NVDA.NE', 'UBER.NE']
    cdr.days = 5  # Number of calendar days of data to download (e.g. 730 for the past two years)
    print(f'symbols = {cdr.symbols}')
    cdr.load_yahoo_prices()
    print(f'Finished at {dt.now():%Y-%m-%d %H:%M:%S}')
    exit()
