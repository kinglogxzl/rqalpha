'''
Created on 2017.1.11

@author: Kinglog
'''

import os
import dtsk
import numpy as np
import xarray as xr

def split_file(kline_type, start_date, end_date, save_folder):
    '''Split the file with nine features to the file with one feature
    '''
    key_value = ['Open', 'High', 'Low', 'Close', 'PreClose', \
                 'HighLimit', 'LowLimit', 'Volume', 'Turnover']
    dtsk_1day = dtsk.load_single_key(kline_type, start_date, end_date)
    for key in key_value:
        dtsk_single_key = dtsk_1day.loc[:, :, :, key]
        dtsk_single_key_ext = np.asarray(dtsk_single_key).reshape(
            len(dtsk_single_key.coords['DATE']), \
            len(dtsk_single_key.coords['TIME']), \
            len(dtsk_single_key.coords['SYMBOL']), \
            1
        )
        key_array = ['Open']
        key_array[0] = key
        dtsk_single_key_ext = xr.DataArray(
            dtsk_single_key_ext, \
            coords=[dtsk_single_key.coords['DATE'].values, \
                    dtsk_single_key.coords['TIME'].values, \
                    dtsk_single_key.coords['SYMBOL'].values, \
                    key_array], \
            dims=['DATE', 'TIME', 'SYMBOL', 'KEY']
        )

        save_path = os.path.join(save_folder, key)
        #save_path = '/Users/Kinglog/SAL/dtsk_cache/temp/' + key
        dtsk.save(dtsk_single_key_ext, save_path, kline_type, start_date, end_date)
