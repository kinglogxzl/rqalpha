def test_date_parse(raw_date):
    try:
    	print "test_date_parse....."
        print '{0}\t-> {1}'.format(raw_date, date.parse(raw_date))
    except Exception, e:
        print '{0}\t-> Exception: {1}'.format(raw_date, str(e))
    print('\n\n')

def test_directory(file_relative_path):
	try:
		print "test_directory....."
		print '{0}\t-> {1}'.format(file_relative_path, \
			directory.pick_file_with_priority_rule(file_relative_path))
	except Exception, e:
		print '{0}\t-> Exception: {1}'.format(file_relative_path, str(e))
	print('\n\n')

def test_directory_cache(file_relative_path, local_cache_folder):
	try:
		print "test_directory_cache....."
		print '{0},{1}\t-> {2}'.format(file_relative_path, local_cache_folder,\
			directory.pick_file_with_priority_rule(\
				file_relative_path = file_relative_path,\
				local_cache_root = local_cache_folder))
	except Exception, e:
		print '{0},{1}\t-> Exception: {2}'.format(file_relative_path, \
			local_cache_folder, str(e))
	print('\n\n')


test_label = True
if test_label:

	test_date_parse_label = False
	if test_date_parse_label:
	    import date
	    test_date_parse('2000-01-1')
	    test_date_parse('verY_Beginning')
	    test_date_parse('Today')
	    test_date_parse(' ')
	    
	test_directory_label = False
	if test_directory_label:
		import directory
		test_directory('__init__.py')
		test_directory('StockInfo/forward_restoration_factor_list.txt')
		test_directory('StockInfo/local_build_test.txt')
		test_directory('abc.def')
		test_directory_cache('StockInfo/forward_restoration_factor_list.txt', \
			'~/sal/temp_will_be_deleted/temp_cache/')
