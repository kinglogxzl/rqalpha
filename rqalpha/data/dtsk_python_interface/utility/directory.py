import os
import file
import platform
import tempfile
import glob
import getpass
import shutil

def get_default_remote_folder():
    platform_system = platform.system()
    if 'Windows' in platform_system:
        network_base_folder = '//192.168.222.199/AShareData/'
    elif 'Linux' in platform_system:
        network_base_folder = os.path.join('/var/local/AShareData')
    elif 'Darwin' in platform_system:
        network_base_folder = os.path.join('/Users', getpass.getuser(), 'AShareData')
    else:
        raise Exception('Not support ' + platform_system)
    return network_base_folder

def get_file_absolute_path_with_check(root, file_relative_path):
    if not os.path.isdir(root):
        return ''
    file_absolute_path = os.path.join(root, file_relative_path)
    if os.path.isfile(file_absolute_path):
        return file_absolute_path
    else:
        return ''

def posite_initialization(file_relative_path, remote_root, local_cache_root):
    # Init
    if remote_root:
        if remote_root.lower() == 'Default'.lower():
            remote_root = get_default_remote_folder()
    else:
        remote_root = os.path.join(os.getcwd(), next(tempfile._get_candidate_names()))

    if local_cache_root:
        local_cache_root = os.path.expanduser(local_cache_root)
        if not os.path.isdir(local_cache_root):
            os.makedirs(local_cache_root)

    # 4 level folder
    file_path_in_current_folder = get_file_absolute_path_with_check(\
        os.getcwd(), file_relative_path)
    file_path_in_remote_folder = get_file_absolute_path_with_check(\
        remote_root, file_relative_path)
    if local_cache_root:
        file_path_in_local_cache = os.path.join(local_cache_root, \
            file_relative_path)
    else:
        file_path_in_local_cache = ''
    build_roots = glob.glob(os.path.join(os.getcwd(), \
        'dtsk_python_interface*', 'AShareData'))
    file_path_in_build_default = ''
    for build_root in build_roots:
        file_path_in_build_default = get_file_absolute_path_with_check(\
            build_root, file_relative_path)
        if file_path_in_build_default:
            break

    return file_path_in_current_folder, file_path_in_remote_folder,\
        file_path_in_local_cache, file_path_in_build_default

def posite_file_with_priority_rule(file_relative_path, \
    remote_root = 'Default', local_cache_root = ''):

    file_path_in_current_folder, file_path_in_remote_folder, \
    file_path_in_local_cache, file_path_in_build_default = posite_initialization(\
        file_relative_path = file_relative_path, remote_root = remote_root, \
        local_cache_root = local_cache_root)

    '''
    # For test
    print '4 path'
    print "current = ", file_path_in_current_folder
    print "remote = ", file_path_in_remote_folder
    print "cache = ", file_path_in_local_cache
    print "default = ", file_path_in_build_default
    '''

    # Priority 1: current folder    
    if file_path_in_current_folder:
        return file_path_in_current_folder

    # Priority 2: remote folder
    if file_path_in_remote_folder:
        if local_cache_root:
            if file.check_same(file_path_in_local_cache, file_path_in_remote_folder):
                return file_path_in_local_cache
            else:
                dir_path = os.path.dirname(file_path_in_local_cache)
                if not os.path.isdir(dir_path):
                    os.makedirs(dir_path)
                shutil.copy(file_path_in_remote_folder, file_path_in_local_cache)
                return file_path_in_local_cache
        else:
            return file_path_in_remote_folder
    else:
        # Priority 3: local cache folder
        if file_path_in_local_cache:
            return file_path_in_local_cache
        # Priority 4: local build folder
        else:
            if file_path_in_build_default:
                return file_path_in_build_default
            else:
                return ''

def open_prioritized_file(file_relative_path, remote_root = 'Default', \
    local_cache_root = ''):
    if file_relative_path:
        file_absolute_path = posite_file_with_priority_rule(\
            file_relative_path = file_relative_path,\
            remote_root = remote_root,\
            local_cache_root = local_cache_root)
        return open(file_absolute_path, 'r')
    else:
        raise ValueError('file_relative_path could not be null.')

def list_all_files_recursively(full_path):
    """ Return a file list
    List all files recursively under full_path
    """

    def append_all_files(arg, dirname, names):
        for name in names:
            path = os.path.join(dirname, name)
            if os.path.isfile(path):
                all_files.append(path)

    all_files = []
    os.path.walk(full_path, append_all_files, '')
    all_files.sort()
    return all_files


def get_h5_file_list(h5_folder):
    h5_files = []
    all_files = list_all_files_recursively(h5_folder)
    for file_path in all_files:
        if file.get_extension(file_path) == "h5":
            h5_files.append(file_path)
    return h5_files

def enforce_create_directory(directory_path):
    if not os.path.isdir(directory_path):
        os.makedirs(directory_path)
