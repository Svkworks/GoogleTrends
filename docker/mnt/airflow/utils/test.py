import os
import yaml

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))

config_file_path = base_path + '/utils/config.yaml'

with open(config_file_path, 'r+') as stream:
    yaml_dict = yaml.safe_load(stream=stream)


print(yaml_dict['keywords'])
print(base_path + yaml_dict['gcp_auth_path'])
