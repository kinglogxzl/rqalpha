import os
import platform
import getpass
import bisect
import shutil
import json
import numpy as np
import xarray as xr
from datetime import datetime
from utility import date
from utility import tdtsk
from utility import key_manager
from utility import dts
from utility import restoration
from utility import stock_symbol_list
from utility import directory

def load(restoration_base_date = 'no_restoration',\
        start_date = 'very_beginning',\
        end_date = 'today',\
        kline_type = '1_day',\
        stock_list = [],\
        key_group = 'Group.Basic',\
        remote_root = 'Default',\
        local_cache_root = ''):

    # Print first log
    print 'Start Loading DTSK at {0}...'.format(datetime.utcnow())
    print 'Remote Root Path = ', remote_root
    print 'Local Cache Folder = ', local_cache_root

    # Pre-init D
    if restoration_base_date.lower() != 'no_restoration'.lower():
        restoration_base_date = date.parse(restoration_base_date)
    start_date = date.parse(start_date) 
    end_date = date.parse(end_date)
    date_list = date.get_date_list_in_range(start_date = start_date,\
        end_date = end_date, all_date_list = date.get_trading_days_list(\
            remote_root = remote_root, local_cache_root = local_cache_root))

    print 'Restoration_base_date = ', restoration_base_date
    print 'Start_date = ', start_date
    print 'End_date = ', end_date
    print '{0} trading days will be solved'.format(len(date_list))

    # Pre-init T
    kline_relative_root = tdtsk.get_kline_file_relative_root(\
        kline_type = kline_type)
    kline_time_list = tdtsk.get_kline_time_list(kline_type = kline_type)

    print 'KLine_relative_root = ', kline_relative_root
    print 'KLine time list length = ', len(kline_time_list)

    # Pre-init S
    if len(stock_list) == 0:
        stock_list = stock_symbol_list.load(remote_root = remote_root, \
            local_cache_root = local_cache_root)

    print 'Stock_list length = ', len(stock_list)

    # Pre-init K
    key_list = key_manager.load(key_group = key_group, remote_root = remote_root, \
        local_cache_root = local_cache_root)

    print 'Key_list: ', key_list

    # Real Loading With Exciting...
    dtsk_numpy = np.empty(shape=(len(date_list),len(kline_time_list),\
        len(stock_list), 0))

    for key in key_list:
        dts_relative_path = os.path.join(kline_relative_root, key)
        print 'Loading DTS under {0}...'.format(dts_relative_path)
        dts_numpy_in_current_key = dts.load(kline_type = kline_type,\
            local_cache_root = local_cache_root, remote_root = remote_root, \
            stock_list = stock_list, dts_folder_relative_path = dts_relative_path,\
            kline_time_list = kline_time_list, date_list = date_list, key = key)

        dtsk_numpy = np.concatenate((dtsk_numpy, dts_numpy_in_current_key), axis=3)

    dtsk_xarray = xr.DataArray(dtsk_numpy, coords = [date_list, kline_time_list, \
        stock_list, key_list], dims = ['DATE', 'TIME', 'SYMBOL', 'KEY'])

    if restoration.contains_key(key_list):
        dtsk_xarray = restoration.apply(dtsk_data = dtsk_xarray)

    return dtsk_xarray

def save(dtsk_entity = '', root = '', kline_type = ''):

    if not root:
        raise ValueError('root should not be null.')
    if not dtsk_entity.any():
        raise ValueError('dtsk_entity should not be null.')
    if not kline_type:
        raise ValueError('kline_type should not be null.')

    absolute_root = os.path.join(root, \
        tdtsk.get_kline_file_relative_root(kline_type))
    
    for current_key in dtsk_entity.coords['KEY'].values:
        key_root = os.path.join(absolute_root, current_key)
        directory.enforce_create_directory(key_root)

        dts_entity = dtsk_entity.loc[:,:,:,current_key]
        dts.save(folder_absolute_path = key_root, dts_entity = dts_entity, \
            current_key = current_key, kline_type = kline_type)
