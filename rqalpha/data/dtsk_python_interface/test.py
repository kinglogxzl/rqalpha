import ts
import dts
import stock_symbol_list
import tdtsk
import numpy as np
import date
from .. import dtsk

key = 'Close'
current_date = '2013-03-14'
time_interval = '1'

file_path = "KLineNewSeperate/{2}min/{0}/{1}.h5".format(key, current_date, time_interval)
kline_list = tdtsk.get_kline_time_list('{0}_min'.format(time_interval))

stock_list = stock_symbol_list.load()
date_list = date.get_date_list_in_range(start_date = '2012-11-27',\
    end_date = '2013-01-03', all_date_list = date.get_trading_days_list())

print date_list

if False:
	ts_entity = ts.load(file_relative_path = file_path, \
		kline_time_list = kline_list, stock_list = stock_list, \
		key = key, kline_type = '{0}_min'.format(time_interval), \
		current_date = current_date)

	print ts_entity
	print ts_entity.loc[:,'09:30:00','600097.SH',key].values


if False:

	dts_entity = dts.load(kline_type = '{0}_min'.format(time_interval), \
		kline_time_list = kline_list, date_list = date_list, key = key,\
	    stock_list = stock_list, dts_folder_relative_path = \
	    "KLineNewSeperate/{1}min/{0}".format(key, time_interval))

	print dts_entity
	print dts_entity.shape

if False:

	dtsk_entity = dtsk.load(restoration_base_date = 'no_restoration',\
        start_date = 'very_beginning',\
        end_date = '2010.01.15',\
        kline_type = '{0}_min'.format(time_interval),\
        stock_list = stock_list,\
        key_group = 'Group.Basic',\
        remote_root = 'Default',\
        local_cache_root = '')
	# print dtsk_entity
	print dtsk_entity.coords
	print dtsk_entity.shape

if True:

	dtsk_entity = dtsk.load(start_date = '2017-01-15')
	dtsk.save(dtsk_entity = dtsk_entity, kline_type = '1_day',\
		root = '/home/anlson/sal/temp_will_be_deleted/save_test_root')