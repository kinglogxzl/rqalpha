import directory
import date

def get_multiple_keys():
	return ['Close', 'High', 'HighLimit', 'Low', 'LowLimit', 'Open']

def get_divided_keys():
	return ['Volume']

def contains_key(key_list = []):
	for key in get_multiple_keys():
		if key in key_list:
			return True
	for key in get_divided_keys():
		if key in key_list:
			return True
	return False

def calculate_restoration(restoration_base_date = 'no_restoration', end_date = 'today',\
	stock_name = '', price_list = [], date_list = [], re_start_date_list = [],\
	re_end_date_list = [], start_date = 'very_beginning',  dtsk_data = ''):

	multiple_keys = get_multiple_keys()
	divided_keys = get_divided_keys()

	if restoration_base_date == 'no_restoration':
		return dtsk_data
	restoration_base_date = date.parse(restoration_base_date)
	start_date = date.parse(start_date)
	end_date = date.parse(end_date)

	base_restoration_price = 1
	for index, price in enumerate(price_list):
		if restoration_base_date >= re_start_date_index[index] and \
			restoration_base_date <= re_end_date_list[index]:
			base_restoration_price = price
			break

	if stock_name in dtsk_data['SYMBOL'].values:
		start = date_list[bisect.bisect_left(date_list, start_date)]
		end = date_list[bisect.bisect_right(date_list, end_date) - 1]

		dtsk_data.loc[start : end, :, stock_name, multiple_keys].values /= base_restoration_price
		dtsk_data.loc[start : end, :, stock_name, divided_keys].values *= base_restoration_price

		for index, price in enumerate(price_list):
			re_start_date = re_start_date_list[index]
			if start_date > re_start_date:
				re_start_date = start_date

			re_end_date = re_end_date_list[index]
			if end_date < re_end_date:
				re_end_date = end_date

			start = date_list[bisect.bisect_left(date_list, re_start_date)]
			end = date_list[bisect.bisect_right(date_list, re_end_date) - 1]
			
			dtsk_data.loc[start : end, :, stock_name, multiple_keys].values *= price
			dtsk_data.loc[start : end, :, stock_name, divided_keys].values /= price

	return dtsk_data


def apply_after_restoration(restoration_base_date = 'no_restoration',\
	start_date = 'very_beginning',\
	end_date = 'today',\
	remote_root = 'Default',\
	local_cache_root = '',\
	dtsk_data = ''):

	restoration_factor_file = directory.open_prioritized_file(\
		file_relative_path = 'StockInfo/after_restoration_factor_list.txt',
		remote_root = remote_root, local_cache_root = local_cache_root)

	
	trading_days_file = directory.open_prioritized_file(\
		file_relative_path = 'StockInfo/tradingdays.txt',\
		remote_root = remote_root, local_cache_root = local_cache_root)

	date_list = trading_days_file.read().splitlines()
	start_date = date.parse(start_date)
	end_date = date.parse(end_date)
	
	stock_name = ''
	re_start_date_list = []
	re_end_date_list = []
	price_list = []
	for line in reversed(restoration_factor_file.read().splitlines()):
		if not line:
			continue

		line_words = line.split()
		symbol = line_words[0]
		re_start_date = line_words[1]   # include
		re_start_date = date.parse(re_start_date)
		re_end_date = line_words[2]     # include
		re_end_date = date.parse(re_end_date)
		price = float(line_words[3])

		if not stock_name or stock_name.lower() == symbol.lower():
			stock_name = symbol
			re_end_date_list.append(re_end_date)
			re_start_date_list.append(re_start_date)
			price_list.append(price)
		else:
			dtsk_date = calculate_restoration(\
				restoration_base_date = restoration_base_date, dtsk_data = dtsk_data,\
				stock_name = stock_name, price_list = price_list, end_date = end_date,\
				date_list = date_list, re_start_date_list = re_start_date_list,\
				re_end_date_list = re_end_date_list, start_date = start_date)

			stock_name = ''
			re_end_date_list = []
			re_start_date_list = []
			price_list = []

	dtsk_date = calculate_restoration(\
		restoration_base_date = restoration_base_date, dtsk_data = dtsk_data,\
		stock_name = stock_name, price_list = price_list, end_date = end_date,\
		date_list = date_list, re_start_date_list = re_start_date_list,\
		re_end_date_list = re_end_date_list, start_date = start_date)

	return dtsk_data


def apply_forward_restoration(restoration_base_date = 'no_restoration',\
	start_date = 'very_beginning',\
	end_date = 'today',\
	remote_root = 'Default',\
	local_cache_root = '',\
	dtsk_data = ''):

	restoration_factor_file = directory.open_prioritized_file(\
		file_relative_path = 'StockInfo/forward_restoration_factor_list.txt',\
		remote_root = remote_root, local_cache_root = local_cache_root)

	trading_days_file = directory.open_prioritized_file(\
		file_relative_path = 'StockInfo/tradingdays.txt',\
		remote_root = remote_root, local_cache_root = local_cache_root)

	date_list = trading_days_file.read().splitlines()
	start_date = date.parse(start_date)
	end_date = date.parse(end_date)

	stock_name = ''
	re_start_date_list = []
	re_end_date_list = []
	price_list = []
	for line in restoration_factor_file.read().splitlines():
		if not line:
			continue

		line_words = line.split()
		symbol = line_words[0]
		re_end_date = line_words[1]     # include
		re_end_date = date.parse(re_end_date)
		re_start_date = line_words[2]   # not include
		re_start_date = date.add_1_day(re_start_date) # after adding, include
		price = float(line_words[3])

		if not stock_name or stock_name.lower() == symbol.lower():
			stock_name = symbol
			re_end_date_list.append(re_end_date)
			re_start_date_list.append(re_start_date)
			price_list.append(price)
		else:
			dtsk_date = calculate_restoration(\
				restoration_base_date = restoration_base_date, dtsk_data = dtsk_data,\
				stock_name = stock_name, price_list = price_list, end_date = end_date,\
				date_list = date_list, re_start_date_list = re_start_date_list,\
				re_end_date_list = re_end_date_list, start_date = start_date)

			stock_name = ''
			re_end_date_list = []
			re_start_date_list = []
			price_list = []

	dtsk_date = calculate_restoration(\
		restoration_base_date = restoration_base_date, dtsk_data = dtsk_data,\
		stock_name = stock_name, price_list = price_list, end_date = end_date,\
		date_list = date_list, re_start_date_list = re_start_date_list,\
		re_end_date_list = re_end_date_list, start_date = start_date)

	return dtsk_data


def apply(restoration_base_date = 'no_restoration',\
		start_date = 'very_beginning',\
		end_date = 'today',\
		dtsk_data = ''):

	# First, Check parameters.
	# Using the same utility with dtsk.load()
	
	if restoration_base_date.lower() == 'no_restoration'.lower():
		print 'no restoration.'
		return dtsk_data
	restoration_base_date = date.parse(restoration_base_date)
	start_date = date.parse(start_date) 
	end_date = date.parse(end_date)
	new_dtsk = dtsk_data

	# Second, Seperate 3 cases and apply:
	# Situation 1: restoration_base_date < start_date,
	# DTSK will apply after_restoration based on restoration_base_date
	if restoration_base_date < start_date:
		print 'try to apply after restoration.'
		new_dtsk = apply_after_restoration(\
			restoration_base_date = restoration_base_date,\
			start_date = start_date,\
			end_date = end_date,\
			dtsk_data = dtsk_data)
	
	# Situation 2: restoration_base_date > end_date.
	# DTSK will apply forward_restoration based on restoration_base_date.
	elif restoration_base_date > end_date:
		print 'try to apply forward restoration.'
		new_dtsk = apply_forward_restoration(\
			restoration_base_date = restoration_base_date,\
			start_date = start_date,\
			end_date = end_date,\
			dtsk_data = dtsk_data)

	# Situation 3: start_date <= restorateion_base_date <= end_date:
	# DTSK will not apply any restoration factor.
	else:
		print 'no restoration.'
		return dtsk_data


	# [To Implement]. Third, Automation of test.

	return new_dtsk