import csv
import directory

def load(remote_root = 'Default', local_cache_root = ''):
    """ Return a list that represent all loaded symbols """

    symbol_list_file = directory.open_prioritized_file(\
        file_relative_path = 'StockInfo/stock_symbol_list.txt',\
        remote_root = remote_root, local_cache_root = local_cache_root)

    stock_symbol_list = symbol_list_file.read().splitlines()
        
    return stock_symbol_list
