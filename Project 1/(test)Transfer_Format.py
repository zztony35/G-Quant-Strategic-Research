from datetime import timedelta, date, datetime
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

if __name__ == '__main__':
    start_date_str = '2017-09-21'
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    l0 = ['Jan', 'Apr', 'Feb', 'Q2']
    l1 = transfer_format(l0)
    print(l1)
    # print(start_date.strftime('%Y-%m-%d'))
    # print(type(start_date))

