import sys
 
# setting Sinara abstract class
sys.path.append('../../sinara')

# importing
from sinara.settings.settings import _SinaraSettings

import json
import os
class SinaraSettings(_SinaraSettings):
    def get_tmp_paths():
        return {
            "test": "/data/tmp/test",
            "prod": "/data/tmp/prod",
            "user": "/data/tmp/user"
        }

    def get_tmp_path(env_name):

        tmp_paths = SinaraSettings.get_tmp_paths()
        if env_name not in tmp_paths:
            raise Exception("Unexpected env_name value:" + env_name)

        return tmp_paths[env_name]

    def get_user():
        return os.getenv("DSML_USER") or 'jovyan'

    def get_data_paths():
        data_paths = {
            "test": "/data/products",
            "prod": "/data/production",
            "user": f"/data/home/{SinaraSettings.get_user()}"
        }

        custom_data_paths = {}
        custom_config_path = f"sinara/infra/{os.environ['INFRA_NAME']}/settings/env.json"
        if os.path.isfile(custom_config_path):            
            with open(custom_config_path) as json_file:
                custom_data_paths = json.load(json_file)

            data_paths = {**data_paths,**custom_data_paths}
        return data_paths

    def get_env_path(env_name):
        env_paths = SinaraSettings.get_data_paths()
        if env_name not in env_paths:
            raise Exception("Unexpected env_name value:" + env_name)
        return env_paths[env_name]