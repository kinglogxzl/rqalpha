import ascii
import os
import tdtsk
import h5py
import numpy as np
import xarray as xr
import directory

def get_key_string_list(h5_file_content, dimension_name):
	ascii_arr = h5_file_content[dimension_name][()]
	string_list = []
	for current_ascii in ascii_arr.tolist():
		string_list.append(ascii.ascii_to_string(current_ascii))
	return string_list

def get_key_ascii_array_list(tsk, dimension_name):
	ascii_solid_len = np.nan
	if dimension_name == "KEY":
		ascii_solid_len = 128
		return ascii.parse_ascii_array_list_from_string_list(\
			[tsk], ascii_solid_len)
	if dimension_name == "SYMBOL":
		ascii_solid_len = 20
	if dimension_name == "TIME":
		ascii_solid_len = 8

	key_string_list = tsk.coords[dimension_name].values
	key_ascii_array_list = ascii.parse_ascii_array_list_from_string_list(\
		key_string_list, ascii_solid_len)
	return key_ascii_array_list

def get_index_list(origin_list = [], target_list = []):
	index_list = []
	mapping = {}
	origin_set = set(origin_list)
	for index, content in enumerate(origin_list):
		mapping[content] = index
	for index, content in enumerate(target_list):
		if content in origin_set:
			index_list.append(mapping[content])
		else:
			index_list.append(-1)
	return index_list

def load(local_cache_root = '', remote_root = 'Default', file_relative_path = '',\
	kline_time_list = [], stock_list = [], key = '', kline_type = '1_day',\
	current_date = ''):
	
	if not file_relative_path:
		raise ValueError('file_relative_path could not be null.')
	if not key:
		return
	if not current_date:
		raise ValueError('current_date should not be null.')

	# Load h5 file
	h5_file_absolute_path = directory.posite_file_with_priority_rule(\
		local_cache_root = local_cache_root, remote_root = remote_root, \
		file_relative_path = file_relative_path)

	h5_file_content = h5py.File(h5_file_absolute_path, 'r')

	 # Load dimension names from h5_file content
	time_list = get_key_string_list(h5_file_content, \
		tdtsk.get_kline_type_labal(kline_type))
	symbol_list = get_key_string_list(h5_file_content, "SYMBOL")
	key_list = get_key_string_list(h5_file_content, "KEY")
	if len(key_list) != 1 or key_list[0].lower() != key.lower():
		raise Exception("Key error.")

	# Load raw data from h5_file content
	labal = tdtsk.get_kline_type_labal(kline_type) + '_DTSK'
	dataset = h5_file_content[labal]
	h5_numpy = np.asarray(dataset).reshape(1, len(time_list), len(symbol_list), 1)

	# Fill in nan in black cells
	eps = 1e-5
	h5_numpy[abs(h5_numpy + 2e10) <= eps] = np.nan

	# Fit stock_list
	symbol_nan = np.zeros(shape=(1, len(time_list), 1, 1)) * np.nan
	stock_fit_numpy = np.concatenate((h5_numpy, symbol_nan), axis = 2)
	stock_index_list = get_index_list(origin_list = symbol_list, target_list = stock_list)
	stock_fit_numpy = stock_fit_numpy[:,:,stock_index_list,:]

	# Fit kline_time_list
	time_nan = np.zeros(shape=(1, 1, len(stock_list), 1)) * np.nan
	ts_numpy = np.concatenate((stock_fit_numpy, time_nan), axis = 1)
	time_index_list = get_index_list(origin_list = time_list, target_list = kline_time_list)
	ts_numpy = ts_numpy[:,time_index_list,:,:]

	ts_xarray = xr.DataArray(ts_numpy, \
		coords = [[current_date], kline_time_list, stock_list, [key]],\
		dims = ['DATE', 'TIME', 'SYMBOL', 'KEY'])

	# return ts_xarray # For test
	return ts_numpy # For product

def save(h5_file_path = '', ts_entity = '', kline_type = '', current_key = ''): 
	''' Not sure whether the data is correct '''

	if not h5_file_path:
		raise ValueError('h5_file_path should not be null.')
	if not ts_entity.any():
		raise ValueError('ts_entity should not be null.')
	if not kline_type:
		raise ValueError('kline_type should not be null.')
	if not current_key:
		raise ValueError('current_key should not be null.')

	time_list = get_key_ascii_array_list(ts_entity, 'TIME')
	symbol_list = get_key_ascii_array_list(ts_entity, "SYMBOL")
	key_list = get_key_ascii_array_list(current_key, 'KEY')

	file = h5py.File(h5_file_path, 'w')
	file[tdtsk.get_kline_type_labal(kline_type)] = time_list
	file["SYMBOL"] = symbol_list
	file["KEY"] = key_list

	tsk_numpy = np.asarray(ts_entity.values).reshape(len(time_list), len(symbol_list), 1)

	file[tdtsk.get_kline_type_labal(kline_type)+'_DTSK'] = tsk_numpy
	file.close()

