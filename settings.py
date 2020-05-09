import os
import json

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
try:
    with open("{}/config.json".format(PROJECT_ROOT)) as config_json:
        CONFIG = json.load(config_json)
        SYMBOL = CONFIG['symbol']
except IOError:
    raise Exception('config.json does not exist')

# Strategies
if not 'strategies' in CONFIG or len(CONFIG['strategies']) == 0:
    raise Exception('config.json does not have strategies')

# Alpha Vantage api key
API_KEY = os.environ.get('TRAZY_API_KEY')
if API_KEY is None:
    raise Exception('TRAZY_API_KEY is not defined')

# MongoDB connection
DB_CONN = os.environ.get('TRAZY_CONN')
if DB_CONN is None:
    raise Exception('TRAZY_CONN is not defined')

# RabbitMq connection
CLOUDAMQP_URL = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
