from db.security import Security
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import datetime as dt

def get_sp500_ticker_list(Security_):
    sql = 'SELECT * FROM INDEX_COMPONENT WHERE INDEX_TICKER = \'^GSPC\';'
    df = pd.read_sql(sql, con=Security_.conn)
    return list(df['STOCK_TICKER'])

def get_first_workday_list(Security_, bgnstring, endstring):
    historical_data = Security_.get_security_day_price_with_ticker(ticker='SPY', bgn=bgnstring, end=endstring)
    workday_list = list(historical_data['time_x'].astype(str))

    bgn = datetime.strptime(bgnstring,'%Y-%m-%d')
    end = datetime.strptime(endstring,'%Y-%m-%d')
    startyear, startmonth = bgn.year, bgn.month
    endyear, endmonth = end.year, end.month
    first_workday_list = []
    for m in range(startyear*12+startmonth-1, endyear*12+endmonth):
        first_workday_date = dt.date(m//12, m%12+1, 1)
        while(str(first_workday_date) not in workday_list):
            first_workday_date +=  timedelta(days=1)
        first_workday_list.append(str(first_workday_date))
    return first_workday_list

def get_price_list_with_date(Security_, date):
    sql_id = '(SELECT STOCK_TICKER FROM INDEX_COMPONENT WHERE INDEX_TICKER = \'^GSPC\')'
    sql = 'SELECT TICKER, TIME_X, ADJ_CLOSE FROM SECURITY_DAY_PRICE ' \
          'WHERE TICKER IN ' + sql_id +' AND TIME_X =\' ' + date +'\' ORDER BY TICKER'
    df = Security_.get_data_with_sql(sql)
    return df[['TICKER','ADJ_CLOSE']]

def get_spy_price_with_date(Security_, date):
    sql= 'SELECT SECURITY_LOOKUP_ID, ADJ_CLOSE FROM SECURITY_DAY_PRICE ' \
         'WHERE SECURITY_LOOKUP_ID = 7189 AND TIME_X = \' ' + date + '\';'
    df = Security_.get_data_with_sql(sql)
    return df.iloc[0]['ADJ_CLOSE']

if __name__ == '__main__':
    s = Security(user='readonly', password='123456', host='160.79.239.235', port=3306, db='gmbp')
    # Step 1
    sp500_list = get_sp500_ticker_list(s) # totally 496 stocks
    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    bgn_date = '2007-01-01'
    end_date = '2017-10-20'
    # Step 2
    first_workday_list = get_first_workday_list(s, bgnstring=bgn_date, endstring=end_date)

    # Step 3 生成一个表，记录下每只股票上面这个date_list中每一天的价格
    # 需要较长时间

    """
    tmp = pd.DataFrame({'TICKER':sp500_list})
    for t in first_workday_list:
        p = get_price_list_with_date(s,t)
        tmp = tmp.join(p.set_index('TICKER'), on='TICKER')
        tmp = tmp.rename(columns={'ADJ_CLOSE':t})
        print("Date "+ t +" completed")
    tmp.to_csv('price.csv',index=False)
    """
    # step 4 把SPY的结果加进去
    spy_price_list = [get_spy_price_with_date(s, m) for m in first_workday_list]
    spy_price_list.insert(0,'SPY')

    df = pd.read_csv('price.csv')
    df = df.dropna(axis=0, how='any')
    df.loc[df.shape[0]] = spy_price_list
    df = df.set_index('TICKER') # df.shape = (428,130) 一共130个月

    # step 5 把月初的价格转化成每月的增长率
    change_df = (np.exp(np.log(df.shift(-1,axis=1)) - np.log(df)) - 1) # 重要，df.shift()
    change_df = change_df.iloc[:, :-1]
    # change_df.to_csv('change.csv')

    # step 6 计算同一月份的均值 (每个股票12个数据)
    N = int(change_df.shape[1] / 12)  # The number of full year
    M = change_df.shape[1] - 12*N     # The number of months left
    new_columns = month_list * N + month_list[:M]

    month_df = change_df
    month_df.columns = new_columns
    month_df = month_df.groupby(by=month_df.columns, axis=1).mean()
    month_df = month_df[month_list]
    # month_df.to_csv('month.csv')

    # step 7 每个月所有available股票的平均涨跌幅 (总共输出12个月份数据)
    # 先暂时除去SPY的数据
    avg_month_df = month_df.iloc[:-1,:]
    avg_month_df = avg_month_df.mean(axis=0)
    # avg_month_df.to_csv('avg_month.csv')



