import json
import directory

def parse_key_group_name(key_group_name = 'Group.basic'):
    line = key_group_name.split('.')
    if len(line) != 2 or not line[0] or not line[1]:
        raise ValueError('key_group_name not correct, please see dtsk_python_load_demo.py')
    name_type = line[0]
    name_value = line[1]
    if name_type.lower() == 'folder'.lower():
        return 'folder', name_value
    elif name_type.lower() == 'group'.lower():
        return 'group', name_value.lower()
    else:
        raise ValueError('key_group_name not support {0}', name_type)

def get_key_list(key_type, key_value, config_json_content):
    if key_type == 'folder':
        return [key_value]
    elif key_type == 'group':
        return config_json_content[key_value]
    else:
        raise ValueError('key_type {0} not supported', key_type)

def load(key_group = 'Group.basic', remote_root = 'Default', local_cache_root = ''):
    key_group_file = directory.open_prioritized_file(\
        file_relative_path = 'StockInfo/dtsk_key_group.json',\
        remote_root = remote_root, local_cache_root = local_cache_root)
    config_json_content = json.load(key_group_file)
    key_type, key_value = parse_key_group_name(key_group)
    key_list = get_key_list(key_type, key_value, config_json_content)
    return key_list
