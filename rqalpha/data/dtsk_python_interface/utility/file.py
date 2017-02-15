import os
import string
import re
import filecmp
import hashlib

# Consider save this hash value in disk to compare
def hash_cmp(text1, text2):
    hash1 = hashlib.md5()
    hash1.update(text1)
    hash1 = hash1.hexdigest()

    hash2 = hashlib.md5()
    hash2.update(text2)
    hash2 = hash2.hexdigest()

    return  hash1 == hash2

def check_same(file1, file2):
	if os.path.isfile(file1) and os.path.isfile(file2):
		# Way1. Completely comparision
		# return filecmp.cmp(file1, file2)
		if os.stat(file1).st_size == os.stat(file2).st_size:
			# Way2. Hash comparision
			# return hash_cmp(open(file1, 'r'), open(file2, 'r'))

			# Way3. Only compare file size
			return True
		else:
			# Missing any file must always return False!!!
			return False
	else:
		return False

def get_extension(full_path):
    """ Return a string represents the file extension """
    extension = os.path.splitext(full_path)[-1]
    if extension:
        extension = re.sub('\.', '', extension)
    return extension


def get_name_without_extension(full_path):
    full_path_without_extension = os.path.splitext(full_path)[0]
    if full_path_without_extension:
        file_name = os.path.basename(full_path_without_extension)
    return file_name
