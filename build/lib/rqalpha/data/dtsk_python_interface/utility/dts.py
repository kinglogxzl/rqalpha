import os
import ascii
import tdtsk
import h5py
import numpy as np
import xarray as xr
import ts
import directory
import date

def load(kline_type = '1_day', kline_time_list = [], date_list = [],\
    local_cache_root = '', remote_root = 'Default', key = '',\
    stock_list = [], dts_folder_relative_path = ''):

    if not key:
        raise ValueError('key should not be null.')
    if not dts_folder_relative_path:
        raise ValueError('dts_folder_relative_path should not be null.')
    if not kline_time_list:
        raise ValueError('kline_time_list should not be null.')

    dts_numpy = np.empty(shape=(0,len(kline_time_list),len(stock_list),1))
    ts_nan_numpy = np.ndarray(shape=(1, len(kline_time_list), len(stock_list), 1)) * np.nan
    
    last_day_month = ''
    for current_date in date_list:
        file_relative_path = os.path.join(dts_folder_relative_path, current_date + '.h5')
        
        if directory.posite_file_with_priority_rule(file_relative_path = file_relative_path,\
            remote_root = remote_root, local_cache_root = local_cache_root):
            if last_day_month != date.get_month(current_date):
                print 'Loading: {0}'.format(date.get_month(current_date))
                last_day_month = date.get_month(current_date)
            try:
                ts_numpy_in_current_date = ts.load(local_cache_root = local_cache_root, \
                    remote_root = remote_root, file_relative_path = file_relative_path,\
                    kline_time_list = kline_time_list, stock_list = stock_list, \
                    current_date = current_date, kline_type = kline_type, key = key)

                dts_numpy = np.concatenate((dts_numpy, ts_numpy_in_current_date), axis = 0)
            except Exception,e:  
                print Exception,":",e
                dts_numpy = np.concatenate((dts_numpy, ts_nan_numpy), axis = 0)
        else:
            if last_day_month != date.get_month(current_date):
                print 'Loading Empty: {0}'.format(date.get_month(current_date))
                last_day_month = date.get_month(current_date)

            dts_numpy = np.concatenate((dts_numpy, ts_nan_numpy), axis = 0)

    dts_xarray = xr.DataArray(dts_numpy, \
        coords=[date_list, kline_time_list, stock_list, [key]], \
        dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])

    # return dts_xarray # For test
    return dts_numpy # For product

def save(folder_absolute_path = '', dts_entity = '', kline_type = '', \
    current_key = ''): 

    if not folder_absolute_path:
        raise ValueError('folder_absolute_path should not be null.')
    if not dts_entity.any():
        raise ValueError('dts_entity should not be null.')
    if not kline_type:
        raise ValueError('kline_type should not be null.')
    if not current_key:
        raise ValueError('current_key should not be null.')
    
    directory.enforce_create_directory(folder_absolute_path)
    print 'Saving under {0}...'.format(folder_absolute_path)
    last_day_month = ''

    for current_date in dts_entity.coords['DATE'].values:
        ts_entity = dts_entity.loc[current_date,:,:]
        h5_file_path = os.path.join(folder_absolute_path, current_date + '.h5')
        if ts_entity.size == np.sum(np.isnan(ts_entity)):
            if last_day_month != date.get_month(current_date):
                print 'Saving Empty: {0}'.format(date.get_month(current_date))
                last_day_month = date.get_month(current_date)
        else:
            ts.save(h5_file_path = h5_file_path, ts_entity = ts_entity, \
                kline_type = kline_type, current_key = current_key)
            if last_day_month != date.get_month(current_date):
                print 'Saving: {0}'.format(date.get_month(current_date))
                last_day_month = date.get_month(current_date)

        

