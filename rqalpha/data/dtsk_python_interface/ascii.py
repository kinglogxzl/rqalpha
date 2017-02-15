import string
import numpy as np

def ascii_to_string(ascii_array):
	tmp_string = ''
	for char in ascii_array:
		if char != 0:
			tmp_string += chr(char)
		else:
			break
	return tmp_string

def string_to_ascii(string, solid_len = np.nan):
	num_list = map(ord, list(string))
	num_list.append(0)
	if not np.isnan(solid_len):
		while len(num_list) > solid_len:
			del num_list[-1]
		while len(num_list) < solid_len:
			num_list.append(0)
	return np.asarray(num_list)

def parse_ascii_array_list_from_string_list(string_list, ascii_solid_len = np.nan):
	ascii_list = []
	for string in string_list:
		ascii_list.append(string_to_ascii(string, ascii_solid_len))
	ascii_arr = np.asarray(ascii_list)
	return ascii_arr