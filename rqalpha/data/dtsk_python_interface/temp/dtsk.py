import os
import platform
import getpass
import bisect
import tsk
import tdtsk
import numpy as np
import xarray as xr
import utility.file
import utility.directory
import utility.local_symbol
import shutil
import json

def load(restoration_base_date = 'no_restoration',\
    start_date = 'very_beginning', end_date = 'Today',\
    kline_type = '1_day', symbol_list = [], key_list = ['Folder.Base'], \
    remote_root = 'Default', local_cache_root = '' ):


def copy_file_if_not_exist(copy_source, target):
    if not os.path.isfile(target):
        shutil.copyfile(copy_source, target)

def get_network_folder(kline_file_name = 'KLine'):
    # Select system
    platform_system = platform.system()
    if 'Windows' in platform_system:
        network_base_folder = '//192.168.222.199/AShareData/' + kline_file_name
    elif 'Linux' in platform_system:
        network_base_folder = os.path.join('/home', getpass.getuser(), 'AShareData', kline_file_name)
    elif 'Darwin' in platform_system:
        network_base_folder = os.path.join('/Users', getpass.getuser(), 'AShareData', kline_file_name)
    else:
        raise Exception('Not support ' + platform_system)
    return network_base_folder

def apply_restoration_factor(restoration_factor_list_txt, start_date, end_date,\
    date_list, dtsk_data):
    ''' this function is to be tested '''
    restoration_factor_file = open(restoration_factor_list_txt, 'r')

    for line in restoration_factor_file:
        line_words = line.split()
        symbol = line_words[0]
        re_end_date = line_words[1]
        re_end_date_pos = bisect.bisect_left(date_list, re_end_date)
        if re_end_date_pos == 0:
            return dtsk_data
        else:
            re_end_date = date_list[re_end_date_pos - 1]
            
        re_start_date = line_words[2]
        price = float(line_words[3])

        #print start_date, end_date
        #print re_start_date, re_end_date

        latest_start = max(start_date, re_start_date)
        earliest_end = min(end_date, re_end_date)

        #print latest_start, earliest_end

        if latest_start <= earliest_end:
            if earliest_end != re_end_date:
                start = latest_start
                end = pre_trading_data[earliest_end]
            else:
                start = latest_start
                end = earliest_end

            date_list = dtsk_data.coords['DATE'].values
            start = date_list[bisect.bisect_left(date_list, start)]
            end = date_list[bisect.bisect_right(date_list, end) - 1]

            if symbol in dtsk_data['SYMBOL'].values:
                dtsk_data.loc[start : end, :, symbol, 'Open': 'LowLimit'] *= price
                dtsk_data.loc[start : end, :, symbol, 'Volume'] /= price

    return dtsk_data

def load_single_key(kline_type, start_date, end_date, cache_folder = '', \
    use_restoration = 0, local_symbol_list = [], h5_folder = ''):

    network_base_folder = get_network_folder()
    #print network_base_folder
    if cache_folder != '':
        if use_restoration == 0:
            restoration_factor_list_txt = ''
        else:
            restoration_factor_list_txt = os.path.join(cache_folder, 'restoration_factor_list.txt')
            copy_file_if_not_exist(os.path.join(network_base_folder, 'restoration_factor_list.txt'), restoration_factor_list_txt)

        if len(local_symbol_list) == 0:
            stock_symbol_list_csv = os.path.join(cache_folder, 'stock_symbol_list.csv')
            symbol_list_full_extension_txt = os.path.join(cache_folder, 'symbol_list_full_extension.txt')

            copy_file_if_not_exist(os.path.join(network_base_folder, 'stock_symbol_list.csv'), stock_symbol_list_csv)
            copy_file_if_not_exist(os.path.join(network_base_folder, 'symbol_list_full_extension.txt'), symbol_list_full_extension_txt)
            local_symbol_list = utility.local_symbol.load(stock_symbol_list_csv, symbol_list_full_extension_txt)

        cache_folder = os.path.join(cache_folder, \
            tdtsk.get_kline_type_folder_name(kline_type))
    else:
        if use_restoration == 0:
            restoration_factor_list_txt = ''
        else:
            restoration_factor_list_txt = os.path.join(network_base_folder, 'restoration_factor_list.txt')
        if len(local_symbol_list) == 0:
            local_symbol_list = utility.local_symbol.load(\
                os.path.join(network_base_folder, 'stock_symbol_list.csv'),\
                os.path.join(network_base_folder, 'symbol_list_full_extension.txt'))

    return raw_load(h5_folder, kline_type, restoration_factor_list_txt,\
        start_date, end_date, local_symbol_list, cache_folder)

def raw_load(h5_folder, kline_type, restoration_factor_list_txt, \
    start_date, end_date, local_symbol_list, cache_folder):

    network_base_folder = get_network_folder()
    if h5_folder == '':
        h5_folder = os.path.join(network_base_folder, \
            tdtsk.get_kline_type_folder_name(kline_type))
    else:
        cache_folder = '';

    # Initialization
    dtsk_xarray = []
    date_list = []

    h5_files = utility.directory.get_h5_file_list(h5_folder)
    format_got = 0
    tsk_select_xarray = ''
    # Append all h5 files into dtsk_array
    for single_h5_file in h5_files:
        #print single_h5_file
        date = utility.file.get_name_without_extension(single_h5_file)
        #print date
        if date >= start_date and date <= end_date:
            print "Begin to load dtsk of " + kline_type + " on " + date
            date_list.append(date)
            cache_file = single_h5_file
            if cache_folder != '':
                if not os.path.isdir(cache_folder):
                    os.makedirs(cache_folder)
                cache_file = os.path.join(cache_folder, date)
                if not os.path.isfile(cache_file):
                    shutil.copyfile(single_h5_file, cache_file)
            tsk_select_xarray = tsk.load(cache_file, kline_type, local_symbol_list)
            dtsk_xarray.append(tsk_select_xarray.values)

    # Reshape the array
    dtsk_xarray = np.asarray(dtsk_xarray).reshape(len(date_list), \
        len(tsk_select_xarray.coords['TIME'].values), \
        len(local_symbol_list), \
        len(tsk_select_xarray.coords['KEY']))

    dtsk_xarray = xr.DataArray(dtsk_xarray, \
        coords=[date_list, \
            tsk_select_xarray.coords['TIME'].values, \
            local_symbol_list, tsk_select_xarray.coords['KEY'].values], \
        dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])

    # Apply restoration factor
    if restoration_factor_list_txt != '':
        dtsk_xarray = apply_restoration_factor(restoration_factor_list_txt, \
            start_date, end_date, date_list, dtsk_xarray)

    # return dtsk data in memory
    return dtsk_xarray

def save(dtsk, h5_folder, kline_type, start_date, end_date):
    ''' Please make sure you are saving no rights offering/issue '''
    if not os.path.isdir(h5_folder):
        os.makedirs(h5_folder)
    dtsk.dims
    for date in dtsk.coords['DATE'].values:
        if date >= start_date and date <= end_date:
            file_path = os.path.join(h5_folder, date + ".h5")
            tsk.save(file_path, dtsk.loc[date, :, :, :], kline_type)

def key_load(feature_groups):
    '''Load the feature'''
    json_name = get_network_folder()
    json_name = os.path.join(json_name, 'feature_groups_config.json')
    #print json_name
    json_name = 'feature_groups_config.json'
    config_file = file(json_name)
    info = json.load(config_file)
    return info[feature_groups]

def dtsk_concatenate(dtsk1, dtsk2):
    '''Merge the dtsk in condition that x1 x2 have the same characteristics'''
    result = np.concatenate((dtsk1, dtsk2), axis=3)
    return result

def load(kline_type, start_date, end_date, cache_folder='', \
                 use_restoration=0, local_symbol_list=[], h5_folder='', feature_groups=''):
    '''Load the h5 file to dtsk'''
    key_value = key_load(feature_groups)
    dtsk_xarray = []
    dtsk_tmp = []
    key_array = []

    h5_folder_tmp = get_network_folder(kline_file_name='KLineSeperate')
    h5_folder_tmp = os.path.join(h5_folder_tmp, \
            tdtsk.get_kline_type_folder_name(kline_type))
    #print h5_folder_tmp
    #h5_folder_tmp = '/Users/Kinglog/SAL/dtsk_cache/temp'
    #h5_folder_tmp = '/Volumes/Seagate Backup Plus Drive/meridian/data/daily'
    #TODO: wait for confirm of the storage format of file system, modify the h5_folder
    # e.g. h5_folder_tmp = '/Users/Kinglog/AShareData/KLine/daily' like this

    for i, k in enumerate(key_value):
        h5_folder = os.path.join(h5_folder_tmp, k)
        dtsk_tmp = load_single_key(kline_type=kline_type, \
                                   start_date=start_date, \
                                   end_date=end_date, \
                                   cache_folder=cache_folder, \
                                   use_restoration=use_restoration, \
                                   local_symbol_list=local_symbol_list, \
                                   h5_folder=h5_folder)

        dtsk_xarray = dtsk_tmp if i == 0 else dtsk_concatenate(dtsk_xarray, dtsk_tmp)
        key_array.append(k)

    dtsk_xarray = np.asarray(dtsk_xarray).reshape(len(dtsk_tmp.coords['DATE']), \
                                                  len(dtsk_tmp.coords['TIME']), \
                                                  len(dtsk_tmp.coords['SYMBOL']), \
                                                  len(key_value)
                                                 )
    dtsk_xarray = xr.DataArray(dtsk_xarray, \
                               coords=[dtsk_tmp.coords['DATE'].values, \
                                       dtsk_tmp.coords['TIME'].values, \
                                       dtsk_tmp.coords['SYMBOL'].values, \
                                       key_array], \
                               dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])
    return dtsk_xarray

#start_date = '2014-12-05'
#end_date = '2014-12-05'
#load('1_day', start_date = start_date, end_date = end_date, feature_groups = 'key')
