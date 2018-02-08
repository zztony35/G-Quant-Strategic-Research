# Functionality:  Upload Data Into Database with Max Rows For Each Time
# Version       : 1.1 (09/11/2017)
# Auther        : wSun


import pandas as pd
import sys


class Upload_Data():
    def __init__(self,conn,rows,sql,data):
        self.conn = conn
        self.rows_each_time = rows;
        self.sql = sql
        self.data = data

        self.limit_each_time = 500000
        if self.rows_each_time > self.limit_each_time:
            self.rows_each_time = self.limit_each_time

    def __del__(self):
        pass


    def upload_with_counter(self):
        cursor = self.conn.cursor()

        row_counter =  len(self.data)   #the number of all rows
        counter = 0
        tt = row_counter / self.rows_each_time
        times = int(tt)
        if times < tt:
            times += 1

        for i in range(times):
            if self.rows_each_time > self.limit_each_time:
                print("mysql server will gone away with limitation of upload once time!")
                sys.exit()
            elif counter + self.rows_each_time >= row_counter:
                #cursor.executemany(sql, data[counter:row_counter])
                data2 = []
                for idx in range(counter,row_counter):
                    # solution 1:
                    #data2.append(self.data.iloc[idx].tolist())  #would report KeyError: Numpy:float64

                    # solution 2:
                    row = []
                    list = self.data.iloc[idx].tolist()
                    for j in range(len(list)):
                        row.append(str(list[j]))
                    data2.append(row)
                    del row

                cursor.executemany(self.sql, data2)
                del data2   #release here is ok through testing
                #print('the rest is completed')
            else:
                #cursor.executemany(sql, data[counter:(counter + manyRows)])
                data2 = []
                for idx in range(counter, counter + self.rows_each_time):
                    # solution 1:
                    # data2.append(self.data.iloc[idx].tolist())  #would report KeyError: Numpy:float64

                    # solution 2:
                    row = []
                    list = self.data.iloc[idx].tolist()
                    for j in range(len(list)):
                        row.append(str(list[j]))
                    data2.append(row)
                    del row

                cursor.executemany(self.sql, data2)
                del data2   #release here is ok through testing
                #print('write in of rows from %d to %d is compeleted' % (counter, counter + self.rows_each_time - 1))
            counter += self.rows_each_time
        self.conn.commit()
        cursor.close()
