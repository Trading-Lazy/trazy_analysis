import os
import json

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
with open("{}/config.json".format(PROJECT_ROOT)) as config_json:
    CONFIG = json.load(config_json)
