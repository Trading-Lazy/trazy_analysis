import os
import json

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
try:
    with open("{}/config.json".format(PROJECT_ROOT)) as config_json:
        CONFIG = json.load(config_json)
        SYMBOL = CONFIG["symbol"]
except IOError:
    raise Exception("config.json does not exist")

# Strategies
if not "strategies" in CONFIG or len(CONFIG["strategies"]) == 0:
    raise Exception("config.json does not have strategies")

# RabbitMq connection
CLOUDAMQP_URL = os.environ.get("CLOUDAMQP_URL", "amqp://guest:guest@localhost/%2f")
RABBITMQ_QUEUE_NAME = os.environ.get("RABBITMQ_QUEUE_NAME")

TIINGO_API_TOKEN = os.environ.get("TIINGO_API_TOKEN")
IEX_CLOUD_API_TOKEN = os.environ.get("IEX_CLOUD_API_TOKEN")
MEGA_API_EMAIL = os.environ.get("MEGA_API_EMAIL")
MEGA_API_PASSWORD = os.environ.get("MEGA_API_PASSWORD")

HISTORICAL_FEED_DAY_OF_WEEK = os.environ.get("HISTORICAL_FEED_DAY_OF_WEEK")
HISTORICAL_FEED_HOUR = os.environ.get("HISTORICAL_FEED_HOUR")
HISTORICAL_FEED_MINUTE = os.environ.get("HISTORICAL_FEED_MINUTE")

DATABASE_URL = os.environ.get("DATABASE_URL")
DATABASE_NAME = os.environ.get("DATABASE_NAME")

CANDLES_COLLECTION_NAME = os.environ.get("CANDLES_COLLECTION_NAME")
ORDERS_COLLECTION_NAME = os.environ.get("ORDERS_COLLECTION_NAME")
DOCUMENTS_COLLECTION_NAME = os.environ.get("DOCUMENTS_COLLECTION_NAME")

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
