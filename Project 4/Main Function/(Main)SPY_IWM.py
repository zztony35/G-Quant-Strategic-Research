from db.security import Security
import pandas as pd
import numpy as np
import datetime as dt # Class: dt.date(year, month, day)
from datetime import datetime # 这两个module是不一样的！
from datetime import timedelta
# from pandas.tseries.offsets import BDay # 此函数BDay()只除去周六周日

def get_first_date_in_the_month(bgnstring, endstring):
    # Given a date range, return a list of all dates that are the first date of a month
    # 给出时间期限，返回这一段时间之内，每一个月1号的日期，格式为list
    bgn = datetime.strptime(bgnstring,'%Y-%m-%d')
    end = datetime.strptime(endstring,'%Y-%m-%d')
    startyear, startmonth = bgn.year, bgn.month
    endyear, endmonth = end.year, end.month
    first_date_list = [str(dt.date(m//12, m%12+1, 1)) for m in range(startyear*12+startmonth-1, endyear*12+endmonth)]
    return first_date_list

def get_historical_data(Security_, tic1_, tic2_, bgn_date, end_date):
    # 此函数可以获取两个tickers在一段时间内的价格数据
    # df的格式 TIME_X | TICKER1 | TICKER1_PRICE | TICKER2 | TICKER2_PRICE
    df1 = Security_.get_security_day_price_with_ticker(ticker=tic1_, bgn=bgn_date, end=end_date)
    df2 = Security_.get_security_day_price_with_ticker(ticker=tic2_, bgn=bgn_date, end=end_date)
    # Rename columns
    df1 = df1.rename(columns={'ADJ_CLOSE': 'TICKER1_PRICE', 'TICKER': 'TICKER1'})[
        ['time_x', 'TICKER1', 'TICKER1_PRICE']]
    df2 = df2.rename(columns={'ADJ_CLOSE': 'TICKER2_PRICE', 'TICKER': 'TICKER2'})[
        ['time_x', 'TICKER2', 'TICKER2_PRICE']]
    df = pd.merge(df1, df2, on='time_x')
    df['time_x'] = df['time_x'].astype(str)
    return df

def matrix_row_index_list():
    # 返回一个长度为100的list,清楚地表示出窗口类型的所用可能性
    # NxPy的格式来表示不同的window，代表在月底的倒数第x个工作日建仓，在月初的第y个工作日平仓。x, y的取值范围均是[1, 10]之间的整数
    row_index_list = []
    for i in range(1,11):
        for j in range(1,11):
            row_index = 'N'+str(i)+'P'+str(j)
            row_index_list.append(row_index)
    return row_index_list

def generate_return_matrix(price_df, bgnstring, endstring):
    # 此函数给定一个包含price数据的df,以及一段时间期限
    # 返回一个储存每个月每种window收益的df

    # 为之后方便，我们直接把日期设为index

    price_df.set_index('time_x', inplace=True)
    price_df_date_list = price_df.index #历史价格df中有数据的所有日期，可在之后循环中用来判断某日是不是工作日
    first_date_list = get_first_date_in_the_month(bgnstring, endstring) # 所有月初第一天的日期的list (一个时间段内)
    return_matrix = pd.DataFrame(columns=first_date_list, index=matrix_row_index_list()) # 收益矩阵，初始化一个df，定好行和列的index

    for t_str in first_date_list:
        t = datetime.strptime(t_str, "%Y-%m-%d") # 当月第一天的日期
        revenue_list = []
        open_index = 1
        prior_days = 1
        while (open_index <= 10):
            window_open = t - timedelta(days=prior_days)
            window_open_str = window_open.strftime("%Y-%m-%d")
            if window_open_str in price_df_date_list:
                close_index = 1
                after_days = 0
                while (close_index <= 10):
                    window_close = t + timedelta(days=after_days)
                    window_close_str = window_close.strftime("%Y-%m-%d")
                    if window_close_str in price_df_date_list:
                        tic1_revenue = np.exp(np.log(df.loc[window_close_str]['TICKER1_PRICE'])
                                              - np.log(df.loc[window_open_str]['TICKER1_PRICE'])) - 1
                        tic2_revenue = np.exp(- np.log(df.loc[window_close_str]['TICKER2_PRICE'])
                                              + np.log(df.loc[window_open_str]['TICKER2_PRICE'])) - 1
                        revenue = tic1_revenue + tic2_revenue
                        revenue_list.append(revenue)
                        close_index += 1
                        after_days += 1
                    else:
                        after_days += 1
                open_index += 1
                prior_days += 1
            else:
                prior_days += 1
        return_matrix[t_str] = revenue_list
        print(t_str + ' completed!')
    return return_matrix

if __name__ == '__main__':
    s = Security(user='readonly', password='123456', host='160.79.239.235', port=3306, db='gmbp')
    bgn_date = '2000-06-10'
    end_date = '2017-10-14'
    ticker1 = 'SPY'
    ticker2 = 'IWM' # df2.shape = (4371, 9) IWM 最早从2000-05-26开始才有数据

    # SPY 与IWM 的历史数据query到DataFrame，并按照datetime merge 在一起
    df = get_historical_data(s, ticker1, ticker2, bgn_date, end_date)

    # 每个月有100种window的选取方法，所以要获取100个结果, 用first_date_list中的元素作为column index, 100种配对组合作为row index, 构建一个matrix
    # Default Setting: Long SPY + Short IWM
    # 给row index起名字的方式：此项目中用NxPy的格式来表示不同的window，代表在月底的倒数第x个工作日建仓，在月初的第y个工作日平仓。x,y的取值范围均是[1,10]之间的整数
    hundred_windows_df = generate_return_matrix(df, bgn_date, end_date)

    # Rank them by mean of the row
    hundred_windows_df['MEAN'] = hundred_windows_df.mean(axis=1, skipna=False)
    hundred_windows_df['RANK'] = hundred_windows_df['MEAN'].rank(ascending=0).apply(int)
    # Transfer to csv file
    hundred_windows_df.to_csv("hundred_windows_with_rank.csv")
