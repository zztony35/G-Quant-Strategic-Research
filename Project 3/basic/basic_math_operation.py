# Functionality : Basic Math Operation
# Version       : V1.0 (09/25/2017)
# Author        : wSun


import pandas as pd
import time
import sys

from com.wj.db.security import Security

def Main():
    # step 1: db connection
    db = Security(user='root', password='zztony35', host='127.0.0.1', port=3306, db='gmbp')

    # step 2: lookup dictionary and ticker list
    tickers = {'AAPL':16,'SPY':7189}

    # step 3: statistic
    for key in tickers:
        id = tickers[key]
        ticker = key
        df = db.get_security_day_price_with_id(id)

        # basic math operation
        mean = df['ADJ_CLOSE'].mean()       #average
        var = df['ADJ_CLOSE'].values.var()  # variance
        std = df['ADJ_CLOSE'].values.std()    # standard deviation

        print(ticker,mean,var,std)

        del df

    print('whole done...')

if __name__ == '__main__':
    Main()
