import utility.ascii
import tdtsk
import h5py
import numpy as np
import xarray as xr

def get_key_string_list(h5_file_content, dimension_name):
    ascii_arr = h5_file_content[dimension_name][()]
    string_list = []
    for current_ascii in ascii_arr.tolist():
        string_list.append(utility.ascii.ascii_to_string(current_ascii))
    return string_list

def get_key_ascii_array_list(tsk, dimension_name):
	ascii_solid_len = np.nan
	if dimension_name == "KEY":
		ascii_solid_len = 128
	if dimension_name == "SYMBOL":
		ascii_solid_len = 20
	if dimension_name == "TIME":
		ascii_solid_len = 8

	key_string_list = tsk.coords[dimension_name].values
	key_ascii_array_list = utility.ascii.parse_ascii_array_list_from_string_list(\
		key_string_list, ascii_solid_len)
	return key_ascii_array_list

def load(h5_file_path, kline_type, local_symbol_list):
    '''
    kline_type is formatted as {int}_{time_unit}
    local symbol list is read from 2 local files, 
    time/symbol/key list is read from h5 files.
    '''

    # Load h5 file
    h5_file_content = h5py.File(h5_file_path, 'r')

    # Load dimension names from h5_file content
    time_list = get_key_string_list(h5_file_content, \
    	tdtsk.get_kline_type_labal(kline_type))
    symbol_list = get_key_string_list(h5_file_content, "SYMBOL")
    key_list = get_key_string_list(h5_file_content, "KEY")

    # Load raw data from h5_file content
    labal = tdtsk.get_kline_type_labal(kline_type) + '_DTSK'
    dataset = h5_file_content[labal]
    tsk_ndarray = np.asarray(dataset).reshape(len(time_list), len(symbol_list), len(key_list))
    tsk_xarray = xr.DataArray(tsk_ndarray, \
        coords=[time_list, symbol_list, key_list],\
        dims=['TIME', 'SYMBOL', 'KEY'])

    # Convert raw data to real dtsk that fit local_symbol_list
    symbol_set = set(symbol_list)
    tsk_ndarray_symbol_nan = np.ndarray(shape=(len(time_list), 1, len(key_list))) * np.nan
    tsk_select_ndarray = np.ndarray(shape=\
        (len(time_list), len(local_symbol_list), len(key_list)))
    for index, sym in enumerate(local_symbol_list):
        if sym in symbol_set:
            tsk_select_ndarray[:, index:index + 1, :] = \
                tsk_xarray.loc[:, sym, :].values.reshape(len(time_list), 1, len(key_list))
        else:
            tsk_select_ndarray[:, index:index + 1, :] = tsk_ndarray_symbol_nan

    eps = 1e-5
    tsk_select_ndarray[abs(tsk_select_ndarray + 2e10) <= eps] = np.nan

    tsk_select_xarray = xr.DataArray(tsk_select_ndarray, \
        coords=[time_list, local_symbol_list, key_list], \
        dims=['TIME', 'SYMBOL', 'KEY'])

    return tsk_select_xarray

def save(h5_file_path, tsk, kline_type): 
	''' Not sure whether the data is correct '''

	time_list = get_key_ascii_array_list(tsk, 'TIME')
	symbol_list = get_key_ascii_array_list(tsk, "SYMBOL")
	key_list = get_key_ascii_array_list(tsk, "KEY")

	file = h5py.File(h5_file_path, 'w')
	file[tdtsk.get_kline_type_labal(kline_type)] = time_list
	file["SYMBOL"] = symbol_list
	file["KEY"] = key_list
	file[tdtsk.get_kline_type_labal(kline_type)+'_DTSK'] = tsk
	''' Not sure whether the tsk is correct '''
	file.close()

