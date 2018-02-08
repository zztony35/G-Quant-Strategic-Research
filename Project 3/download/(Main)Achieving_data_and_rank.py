import pandas as pd
import numpy as np

from db.security import Security
from log.my_logger import MyLogger

def get_ticker_and_index(db_):
    sql = 'SELECT DISTINCT(STOCK_TICKER) AS TICKER,INDEX_TICKER FROM INDEX_COMPONENT GROUP BY STOCK_TICKER;'
    df = db_.get_data_with_sql(sql)
    return df

def get_id_with_ticker(db_):
    sql = 'SELECT ID,TICKER,SECTOR FROM SECURITY_LOOKUP ' \
          'WHERE TICKER IN (SELECT DISTINCT(STOCK_TICKER) FROM INDEX_COMPONENT);'
    df = db_.get_data_with_sql(sql)
    return df

def get_bgn_and_end_price_with_id(db_, bgn, end): # Time format: yyyy-mm-dd (string)
    sql_id = '(SELECT ID FROM SECURITY_LOOKUP ' \
             'WHERE TICKER IN (SELECT DISTINCT(STOCK_TICKER) FROM INDEX_COMPONENT))'
    sql1 = 'SELECT SECURITY_LOOKUP_ID,TICKER,ADJ_CLOSE AS \"BGN_PRICE\" FROM SECURITY_DAY_PRICE ' \
            'WHERE SECURITY_LOOKUP_ID IN' + sql_id +'AND TIME_X =\' '+ bgn +'\' ORDER BY TICKER;'
    # Dataframe containing id and bgn price
    df1 = db_.get_data_with_sql(sql1)

    sql2 = 'SELECT SECURITY_LOOKUP_ID,TICKER,ADJ_CLOSE AS \"END_PRICE\" FROM SECURITY_DAY_PRICE ' \
            'WHERE SECURITY_LOOKUP_ID IN' + sql_id +'AND TIME_X =\' '+ end +'\' ORDER BY TICKER;'
    # Dataframe containing id and end price
    df2 = db_.get_data_with_sql(sql2)
    # Merge these two tables
    df = pd.merge(df1, df2, on=['TICKER','SECURITY_LOOKUP_ID'])
    return df

def get_spy_bgn_and_end_price(db_, bgn, end):
    sql1 = 'SELECT  SECURITY_LOOKUP_ID,TICKER, ADJ_CLOSE AS \"BGN_PRICE\" FROM SECURITY_DAY_PRICE ' \
          'WHERE SECURITY_LOOKUP_ID = 7189 AND TIME_X = \' '+ bgn +'\';'
    df1 = db_.get_data_with_sql(sql1)
    sql2 = 'SELECT  SECURITY_LOOKUP_ID,TICKER, ADJ_CLOSE AS \"END_PRICE\" FROM SECURITY_DAY_PRICE ' \
           'WHERE SECURITY_LOOKUP_ID = 7189 AND TIME_X = \' ' + end + '\';'
    df2 = db_.get_data_with_sql(sql2)
    df = pd.merge(df1, df2, on=['TICKER','SECURITY_LOOKUP_ID'])
    return df

def get_mkt_cap_with_id(db_):
    sql = 'SELECT SECURITY_LOOKUP_ID,TICKER,MKT_CAP FROM SECURITY_REALTIME_PRICE ' \
            'WHERE SECURITY_LOOKUP_ID IN (SELECT ID FROM SECURITY_LOOKUP WHERE TICKER IN (SELECT DISTINCT(STOCK_TICKER) ' \
            'FROM INDEX_COMPONENT));'
    df = db_.get_data_with_sql(sql)
    return df

def add_pctchange_to_df(df):
    df['PCT_CHANGE'] = np.exp(np.log(df['END_PRICE']) - np.log(df['BGN_PRICE'])) - 1
    return df

def add_rank_of_equity_return(df):
    df['EQUITY_CHANGE_RANK'] = df['PCT_CHANGE'].rank(ascending=0).apply(int)
    return df

def add_rank_of_sector_average_return(df):
    sec_series = df.groupby(['SECTOR'])['PCT_CHANGE'].mean()
    df2 = sec_series.to_frame().reset_index()
    df2 = df2.rename(columns={'PCT_CHANGE': 'AVG_SECTOR_CHANGE'})
    df2['SECTOR_CHANGE_RANK'] = df2['AVG_SECTOR_CHANGE'].rank(ascending=0).apply(int)
    return df2

def add_rank_of_sector_weighted_return(df):
    df['WEIGHTED_PCT_CHANGE'] = df['PCT_CHANGE'] * df['MKT_CAP']
    w_sec_series = df.groupby(['SECTOR'])['WEIGHTED_PCT_CHANGE'].mean()
    df3 = w_sec_series.to_frame().reset_index()
    df3['WEIGHTED_SECTOR_CHANGE_RANK'] = df3['WEIGHTED_PCT_CHANGE'].rank(ascending=0).apply(int)
    return df3

def Main():
    pd.options.mode.chained_assignment = None  # default='warn'
    # step 1: database connection
    db = Security(user='readonly', password='123456', host='160.79.239.235', port=3306, db='gmbp')

    # step 2: get full ticker list from database(index_component)
    # 不知道为什么，这里只能用index_ticker，不能用ticker
    #　(^DJI - Dow Jones) (^GSPC - SP 500) (^IXIC - Nasdap)
    full_ticker_df = get_ticker_and_index(db)
    full_ticker_list = list(full_ticker_df['TICKER'])
    # print('Number of distinct ticker is: {}'.format(len(full_ticker_list))) # 524


    # step 3: get (id,ticker) pair from database(security_lookup)
    id_ticker_sec_df = get_id_with_ticker(db)
    # print('Shape of id_ticker_sec_df is: {}'.format(id_ticker_sec_df.shape)) # (524,3)
    id_list = list(id_ticker_sec_df['ID'])


    # step 4: get price data (bgn to end) by id from database(security_day_price) 搜索2017-01-02和2017-09-21这两天的price
    bgn = '2017-01-03' # 时间如果选01-02只有4条数据, 01-03是524条
    end = '2017-09-21' # 09-21 有518条数据
    tmp1 = get_bgn_and_end_price_with_id(db, bgn, end)

    # step 5: Search for SPY's price data (SPY_ID = 7189)
    tmp2 = get_spy_bgn_and_end_price(db, bgn, end)
    equity_price_df = pd.concat([tmp1, tmp2], ignore_index=True)

    # step 6: add a column of the change of price (Percentage) to the table
    tmp1_with_change = add_pctchange_to_df(tmp1)
    equity_price_df_with_change = add_pctchange_to_df(equity_price_df)

    # step 7: add a column of the rank of equity price change (Column's name: Equity_Change_Rank)
    table1 = add_rank_of_equity_return(equity_price_df_with_change)
    table1.to_csv('1_equity_rank.csv') # ID / TICKER / BGN_PRICE / END_PRICE / PCT_CHANGE / RANK
    print('Table 1 (Equity\'s rank) generated')

    # step 8: add the sector column to this built table to get a structure like (ticker / price 1 / price 2 / id / sector)
    id_bgn_end_sector_table = pd.merge(tmp1_with_change, id_ticker_sec_df, on='TICKER')

    # step 9: add a column of the rank of average sector price change
    table2 = add_rank_of_sector_average_return(id_bgn_end_sector_table)
    table2.to_csv('2_sector_average_rank.csv')
    print('Table 2 (Sector\'s average rank) generated')

    # step 9: add a column of the rank of weighted-average (marketCap) sector price change
    id_cap_df = get_mkt_cap_with_id(db)
    id_cap_sector_price_df = pd.merge(id_bgn_end_sector_table, id_cap_df, on='TICKER')
    table3 = add_rank_of_sector_weighted_return(id_cap_sector_price_df)
    table3.to_csv('3_sector_weighted_rank.csv')
    print('Table 3 (Sector\'s weighted rank) generated')

if __name__ == '__main__':
    Main()
