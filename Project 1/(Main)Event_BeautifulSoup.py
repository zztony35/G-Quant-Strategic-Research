from bs4 import BeautifulSoup
import urllib3
import lxml
import pandas as pd
import numpy as np


def pageload(url_):
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED')
    response = http.request('GET', url_)
    return response.data

# Get Navigable String of tag
def get_string(list_):
    return [i.string for i in list_]

# Transfer abbreviated month name to mm/yyyy
import calendar
def transfer_format(list_):
    abbr_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}
    output = []
    for i in range(len(list_)):
        if list_[i] in calendar.month_abbr:
            num = abbr_to_num[list_[i]]
            two_digit_num = str(num).zfill(2)
            new_month = two_digit_num + '/2017'
            output.append(new_month)
        else:
            output.append(list_[i])
    return output

# Extract values of the table from the soup and store them into a dataframe
def ExtractValue(soup_):
    columns = ['Event', 'Event Time',
               'For', 'Actual', 'Market Expectation',
               'Prior to This', 'Revised from']
    d = {}
    for i in range(len(columns)):
        class_name = "data-col" + str(i)
        object_list = soup_.find_all(attrs={ "class": class_name})
        data_list = get_string(object_list)
        d[columns[i]] = data_list
        del class_name, object_list, data_list

    df = pd.DataFrame(data=d, columns=columns)
    l0 = df['For']
    l1 = transfer_format(l0)
    df['For'] = l1
    df.index = np.arange(1, len(df) + 1)
    return df

def save_to_csv(df, filename):
    df.to_csv(filename)

from datetime import timedelta, date, datetime
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def parse(start_date_str, end_date_str):
    start_date, end_date = datetime.strptime(start_date_str, "%Y-%m-%d").date(), datetime.strptime(end_date_str, "%Y-%m-%d").date()
    parse_num = 0
    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime('%Y-%m-%d')
        url_head = 'https://finance.yahoo.com/calendar/economic?day='
        url = url_head + date_str
        try:
            page = pageload(url)
            soup = BeautifulSoup(page, "lxml")
            df = ExtractValue(soup)
            filename = date_str + '.csv'
            save_to_csv(df, filename)
            parse_num += 1
            print(date_str + ' completed. Total ' + str(parse_num))
            del date_str, url_head, url, page, soup, df, filename
        except:
            print("Error ! "+ date_str)

if __name__ == '__main__':
    start_date_str = '2017-01-01'
    end_date_str = '2017-09-21'
    parse(start_date_str, end_date_str)
