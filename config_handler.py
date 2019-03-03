import sys

config_dict = None


def init_config():
    config_filename = sys.argv[1]
    with open(config_filename, 'r') as config_file:
        config_file_content = config_file.read()
        global config_dict
        config_dict = {}
        exec(config_file_content, config_dict, config_dict)


def load_config(name: str):
    if config_dict is None:
        init_config()
    return config_dict[name]
