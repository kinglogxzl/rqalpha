import dateutil.parser
from datetime import time, datetime, timedelta, date
import directory
from dateutil.relativedelta import relativedelta

def calculate_start_date(end_date = 'today', delta_date = 0, \
	remote_root = 'Default', local_cache_root = ''):
	end_date = parse(end_date)
	end_date_index = -1
	trading_days_list = get_trading_days_list(remote_root = remote_root,\
		local_cache_root = local_cache_root)
	for index, current_date in enumerate(trading_days_list):
		if current_date == end_date:
			end_date_index = index
	if end_date_index == -1:
		raise ValueError("end_date {0} is not a trading day.".format(end_date))
	start_date_index = end_date_index - delta_date
	if start_date_index < 0:
		return '' # means start date not exist
	else:
		return trading_days_list[start_date_index]

def create_kline_time_string(hour, minute, delta_minutes):
	t = time(hour, minute)
	delta = timedelta(minutes = delta_minutes)
	new_t = datetime.combine(date.today(), t) + delta
	st = new_t.strftime("%H:%M:%S")
	return st

def get_date_list_in_range(start_date = 'very_beginning', end_date = 'today',\
	all_date_list = []):
	start_date = parse(start_date)
	end_date = parse(end_date)
	new_date_list = []
	for current_date in all_date_list:
		if current_date >= start_date and current_date <= end_date:
			new_date_list.append(current_date)
	return new_date_list

def get_trading_days_list(remote_root = 'Default', local_cache_root = ''):
	trading_days_file = directory.open_prioritized_file(\
		file_relative_path = 'StockInfo/tradingdays.txt',\
		remote_root = remote_root, local_cache_root = local_cache_root)
	date_list = trading_days_file.read().splitlines()
	return date_list

def parse(raw_date):
	''' dateutil library for timezone handling and generally solid date parsing '''
	if raw_date:
		if raw_date.lower() == 'very_beginning'.lower():
			return '1990-11-25'
		if raw_date.lower() == 'today'.lower():
			from datetime import date
			return str(date.today())

		try:
			date = dateutil.parser.parse(raw_date)
			date_string = "%04d-%02d-%02d" % (date.year, date.month, date.day)
			return date_string
		except ValueError:
			raise ValueError("Incorrect data format of raw_date, "
				"should be YYYY-MM-DD")
	else:
		raise ValueError('raw_date should not be null.')

def add_1_day(origin_date):
	date = dateutil.parser.parse(origin_date)
	new_date = date + timedelta(days = 1)
	new_date_string = "%04d-%02d-%02d" % (new_date.year, new_date.month, new_date.day)
	return new_date_string

def get_month(date_string):
	date = dateutil.parser.parse(parse(date_string))
	return "%04d%02d" % (date.year, date.month)
