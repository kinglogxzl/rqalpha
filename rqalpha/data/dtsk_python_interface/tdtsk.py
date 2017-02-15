import date
import os

def get_time_delta(kline_type = '1_day'):
    if kline_type.lower() == '1_day'.lower():
        return 0
    kline_array = kline_type.split("_")
    if len(kline_array) != 2:
        raise ValueError('KLine_type {0} not supported'.format(kline_type))
    if kline_array[1].lower() == 'min'.lower():
        time_interval = int(kline_array[0])
        if time_interval in [1,5,15,30,60]:
            return time_interval
        else:
            raise ValueError('KLine_type {0} not supported'.format(kline_type))
    else:
        raise ValueError('KLine_type {0} not supported'.format(kline_type))


def get_kline_type_labal(kline_type):
    ''' kline_type must be fit the format {int}_{time unit}
    Example: 1_min '''
    time_interval = get_time_delta(kline_type)
    if time_interval == 0:
        return 'KLINE_DAILY'
    else:
        return "KLINE_{0}_MIN".format(str(time_interval))


def get_kline_type_folder_name(kline_type = '1_day'):
    ''' kline_type must be fit the format {int}_{time unit}
    Example: 1_min '''
    time_interval = get_time_delta(kline_type)
    if time_interval == 0:
        return 'daily'
    else:
        return "{0}min".format(str(time_interval))

def get_kline_file_relative_root(kline_type = '1_day'):
    return os.path.join('KLine', get_kline_type_folder_name(kline_type))


def get_kline_time_list(kline_type = '1_day'):
    time_interval = get_time_delta(kline_type)
    if time_interval == 0:
        return ["0"]
    else:
        kline_time_list = []
        for time_delta in range(0, 120/time_interval):
            kline_time_list.append(date.create_kline_time_string(9,30,time_delta*time_interval))
        for time_delta in range(0, 120/time_interval):
            kline_time_list.append(date.create_kline_time_string(13,00,time_delta*time_interval))
        return kline_time_list
