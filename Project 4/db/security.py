# security.py
# Version   : 0.04 (08/10/2017) not forget to edit version in line 15
# Functions : 1 database connection and disconnection
#             2 query database


import pymysql as mysql_module
import sys
import pandas as pd


class Security:
    def __init__(self, **kwargs):

        self.version = '0.04'

        user = kwargs.get('user', None)
        password = kwargs.get('password', None)
        host = kwargs.get('host', None)
        port = kwargs.get('port', None)
        db = kwargs.get('db', None)

        self.conn = mysql_module.connect(user=user, passwd=password, host=host, port=port, db=db,charset='utf8')    #utf8 support chinese characters
        print('Connected Successfully, Securtiy Version: ', self.version)


    def disconnect(self):
        self.conn.close()
        print('disconnect to database successful...')


    def get_security_lookup(self):
        sql = 'SELECT * FROM security_lookup;'
        df = pd.read_sql(sql, con=self.conn)
        return df

    def get_security_lookup_cn(self):
        sql = 'SELECT * FROM security_lookup_cn;'
        df = pd.read_sql(sql, con=self.conn)
        return df

    def get_security_lookup_ticker_id(self):
        sql = 'SELECT ID,TICKER FROM security_lookup;'
        df = pd.read_sql(sql, con=self.conn)
        security_dict = dict()
        for idx in range(len(df)):
            security_dict[df['TICKER'][idx]] = df['ID'][idx]
        return security_dict

    def get_security_lookup_cn_ticker_id(self):
        sql = 'SELECT ID,TICKER FROM security_lookup_cn;'
        df = pd.read_sql(sql, con=self.conn)
        security_dict = dict()
        for idx in range(len(df)):
            security_dict[str(df['TICKER'][idx])] = df['ID'][idx]
        return security_dict


    def get_security_day_price_with_ticker(self, ticker, bgn=None, end=None):
        if bgn is None and end is None:
            sql = 'SELECT * ' \
                'FROM security_day_price WHERE SECURITY_LOOKUP_ID = (SELECT ID FROM security_lookup WHERE TICKER=\'' + ticker + '\') ' \
                'ORDER BY TIME_X;'    #the speed of using '=' is much more quickly than using 'IN'
        elif bgn is None:
            sql = 'SELECT * ' \
                  'FROM security_day_price WHERE SECURITY_LOOKUP_ID = (SELECT ID FROM security_lookup WHERE TICKER=\'' + ticker + '\') AND TIME_X <=\'' \
                  + str(end) + '\' ORDER BY TIME_X;'
        elif end is None:
            sql = 'SELECT * ' \
                  'FROM security_day_price WHERE SECURITY_LOOKUP_ID = (SELECT ID FROM security_lookup WHERE TICKER=\'' + ticker + '\') AND TIME_X >=\'' \
                  + str(bgn) + '\' ORDER BY TIME_X;'
        else:
            sql = 'SELECT * ' \
                  'FROM security_day_price WHERE SECURITY_LOOKUP_ID = (SELECT ID FROM security_lookup WHERE TICKER=\'' + ticker + '\') AND TIME_X >=\'' \
                  + str(bgn) + '\' AND TIME_X <=\'' + str(end) + '\' ORDER BY TIME_X;'

        # tried inner join, the running time is below max of tolerancee

        df = pd.read_sql(sql, con=self.conn)
        return df


    def get_security_day_price_with_id(self, id, bgn=None, end=None):
        if bgn is None and end is None:
            sql = 'SELECT * FROM security_day_price WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' ORDER BY TIME_X;'
        elif bgn is None:
            sql = 'SELECT * FROM security_day_price WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X <=\'' \
                  + str(end) + '\' ORDER BY TIME_X;'
        elif end is None:
            sql = 'SELECT * FROM security_day_price WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' ORDER BY TIME_X;'
        else:
            sql = 'SELECT * FROM security_day_price WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' AND TIME_X <=\'' + str(end) + '\' ORDER BY TIME_X;'

        df = pd.read_sql(sql, con=self.conn)
        return df

    def get_security_day_price_cn_with_id(self, id, bgn=None, end=None):
        if bgn is None and end is None:
            sql = 'SELECT * FROM security_day_price_cn WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' ORDER BY TIME_X;'
        elif bgn is None:
            sql = 'SELECT * FROM security_day_price_cn WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X <=\'' \
                  + str(end) + '\' ORDER BY TIME_X;'
        elif end is None:
            sql = 'SELECT * FROM security_day_price_cn WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' ORDER BY TIME_X;'
        else:
            sql = 'SELECT * FROM security_day_price_cn WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' AND TIME_X <=\'' + str(end) + '\' ORDER BY TIME_X;'

        df = pd.read_sql(sql, con=self.conn)
        return df

    def get_security_min_price_with_id(self, id, bgn=None, end=None):
        if bgn is None and end is None:
            sql = 'SELECT * FROM security_min_price_combo WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' ORDER BY TIME_X;'
        elif bgn is None:
            sql = 'SELECT * FROM security_min_price_combo WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X <=\'' \
                  + str(end) + '\' ORDER BY TIME_X;'
        elif end is None:
            sql = 'SELECT * FROM security_min_price_combo WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' ORDER BY TIME_X;'
        else:
            sql = 'SELECT * FROM security_min_price_combo WHERE SECURITY_LOOKUP_ID = ' + str(id) + ' AND TIME_X >=\'' \
                  + str(bgn) + '\' AND TIME_X <=\'' + str(end) + '\' ORDER BY TIME_X;'

        df = pd.read_sql(sql, con=self.conn)
        return df

    def get_data_with_sql(self, sql):
        df = pd.read_sql(sql, con=self.conn)
        return df
