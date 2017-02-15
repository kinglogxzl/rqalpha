import dtsk
import numpy as np
import xarray as xr

from dask.array.core import reshape
from audioop import reverse
from copy import deepcopy



def stock_concatenate(x1, x2):
    result = np.concatenate((x1, x2), axis = 2)
    return result

#abandon those stock whose average turnover rank in the last  abandonpercent percent among all stocks
def relative_filter(dtsk, start_date, end_date, abandon_percent):
    datelist = []
    for date in dtsk.coords['DATE'].values:
        if date >= start_date and date <= end_date:
            datelist.append(date)
    stock_num = {'':''}
    stock_average = {'':''}
    nnan = 0
    for stock in dtsk.coords['SYMBOL'].values:
        for date in datelist:
            for time in dtsk.coords['TIME'].values:
                if np.isnan(dtsk.loc[date, time, stock, 'Turnover'].values):
                    nnan+=1
                else:
                    if stock_num.has_key(stock):
                        stock_num[stock]+=1
                        stock_average[stock]+=dtsk.loc[date, time, stock, 'Turnover'].values
                    else :
                        stock_num.setdefault(stock,1)
                        stock_average.setdefault(stock, dtsk.loc[date, time, stock, 'Turnover'].values)
    
    del stock_num['']
    del stock_average['']
    nstock=len(stock_num)
    remainstock=nstock*(1-abandon_percent)
    for stock in stock_num.keys():
        stock_average[stock]=stock_average[stock]/stock_num[stock]
    stock_average=sorted(stock_average.iteritems(), key=lambda d:d[1], reverse=True)
    i = 0
    stocklist = []
    for item in stock_average:
        stocklist.append(item[0])
        i += 1
        if i>remainstock:
            break
    i = 0
    for stock in stocklist:
        list1 = []
        list1.append(stock)
        this_stock = np.asarray(dtsk.loc[:, :, stock, :]).reshape(len(dtsk.coords['DATE']),len(dtsk.coords['TIME']), 1, len(dtsk.coords['KEY']))
        this_stock = xr.DataArray(this_stock, coords=[dtsk.coords['DATE'].values, dtsk.coords['TIME'].values, list1, dtsk.coords['KEY'].values], dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])
        if i == 0:
            i = 1
            filtered_dtsk = this_stock
        else:
            filtered_dtsk = stock_concatenate(filtered_dtsk, this_stock)
    
    filtered_dtsk = xr.DataArray(filtered_dtsk, coords=[dtsk.coords['DATE'].values, dtsk.coords['TIME'].values, stocklist, dtsk.coords['KEY'].values], dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])           
    return filtered_dtsk



#abandon those stock whose average turnover during the given period is lower than the given absthreshold 
def abs_filter(dtsk, start_date, end_date, abs_threshold):
    datelist = []
    for date in dtsk.coords['DATE'].values:
        if date >= start_date and date <= end_date:
            datelist.append(date)
    stock_num = {'':''}
    stock_average = {'':''}
    nnan = 0
    for stock in dtsk.coords['SYMBOL'].values:
        for date in datelist:
            for time in dtsk.coords['TIME'].values:
                if np.isnan(dtsk.loc[date, time, stock, 'Turnover'].values):
                    nnan += 1
                else:
                    if stock_num.has_key(stock):
                        stock_num[stock] += 1
                        stock_average[stock] += dtsk.loc[date, time, stock, 'Turnover'].values
                    else:
                        stock_num.setdefault(stock, 1)
                        stock_average.setdefault(stock, dtsk.loc[date, time, stock, 'Turnover'].values)
    stocklist = []
    del stock_num['']
    del stock_average['']
    print len(stock_num)
    for stocksymbol in stock_num.keys():
        stock_average[stocksymbol] = stock_average[stocksymbol]/stock_num[stocksymbol]
        if stock_average[stocksymbol] >= abs_threshold:
            stocklist.append(stocksymbol)
    i = 0
    for stock in stocklist:
        list1 = []
        list1.append(stock)
        this_stock = np.asarray(dtsk.loc[:, :, stock, :]).reshape(len(dtsk.coords['DATE']), len(dtsk.coords['TIME']), 1, len(dtsk.coords['KEY']))
        this_stock = xr.DataArray(this_stock, coords=[dtsk.coords['DATE'].values, dtsk.coords['TIME'].values, list1, dtsk.coords['KEY'].values], dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])
        if i == 0:
            i = 1
            filtered_dtsk = this_stock
        else:
            filtered_dtsk = stock_concatenate(filtered_dtsk, this_stock) 
    filtered_dtsk = xr.DataArray(filtered_dtsk, coords=[dtsk.coords['DATE'].values, dtsk.coords['TIME'].values, stocklist, dtsk.coords['KEY'].values], dims=['DATE', 'TIME', 'SYMBOL', 'KEY'])
    return filtered_dtsk

def remove_suspension(dtsk):
    #calculate the middle time which should be abandoned (all nan)
    total_time = len(dtsk.coords['TIME'].values)
    mid_time = total_time/2-1
    #reverse the date list to traverse from last day to first day
    reversed_date = list(reversed(dtsk.coords['DATE'].values))
    #traverse all stocks
    for stock in dtsk.coords['SYMBOL'].values:
    #traverse all date,set the date 's key to nan  where the last 20 days before it has at least 3 nan
        for this_date_index, this_date in enumerate(reversed_date):
            #if there are less than 20 days before this day,break
            if this_date_index+20 >= len(reversed_date):
                break
            suspension_days = 0
            for next_date_index in range(1, 21):
                next_date = reversed_date[this_date_index+next_date_index]
                for index, time in enumerate(dtsk.coords['TIME'].values):
                    if  index == mid_time:
                        continue
                    if  np.isnan(dtsk.loc[next_date, time, stock, 'Close'].values):
                        suspension_days+=1
                        break
            # suspension ,set this day's key to nan
            if suspension_days > 3:
                for time in dtsk.coords['TIME'].values:
                    for key in dtsk.coords['KEY'].values:
                        dtsk.loc[this_date, time, stock, key] = np.nan
    return dtsk


def remove_limitup_limitdown(dtsk):
    reversed_date = list(reversed(dtsk.coords['DATE'].values))
    for stock in dtsk.coords['SYMBOL'].values:
        for this_date_index, this_date in enumerate(reversed_date):
            if this_date_index+3 >= len(reversed_date):
                break
            #if this day's close is not nan
            if  not np.isnan(dtsk.loc[this_date, '0', stock, 'Close']):
                nan3 = True
                #traverse the last 3 days before this day
                for next_date_index in range(1, 4):
                    next_date = reversed_date[this_date_index+next_date_index]
                    #find the first day before this day whose close is not nan
                    if not np.isnan(dtsk.loc[next_date, '0', stock, 'Close'].values):
                        #if limit up or limit down,set today's close to nan
                        if (abs(dtsk.loc[this_date, '0', stock, 'Close'].values-dtsk.loc[next_date, '0', stock, 'Close'].values) > 9.97):
                            dtsk.loc[this_date, '0', stock, 'Close'] = np.nan
                        nan3 = False
                        break
                #if all 3 days 's close price  before this day are close,set this day's close to nan
                if nan3:
                    dtsk.loc[this_date, '0', stock, 'Close'] = np.nan
    return dtsk
