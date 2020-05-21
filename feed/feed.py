from alpha_vantage.timeseries import TimeSeries
from pymongo import MongoClient

import pandas as pd
import pika
import logger
import os

import settings

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

m_client = MongoClient(settings.DB_CONN)
ts = TimeSeries(key=settings.API_KEY)


def get_alpha_vantage() -> list:
    data, meta_data = ts.get_intraday(settings.SYMBOL, interval='1min')
    return transform_av_response_to_candles(data,meta_data)


def transform_av_response_to_candles(av_data, av_meta_data) -> list:
    df_data = pd.DataFrame.from_dict(av_data, orient='index')
    df_data.columns = ['open', 'high', 'low', 'close', 'volume']
    df_data['symbol'] = settings.SYMBOL
    df_data['__interval'] = av_meta_data['4. Interval']
    df_data.index = pd.to_datetime(df_data.index)
    df_data.index = df_data.index.tz_localize(av_meta_data['6. Time Zone']).tz_convert('Europe/Paris')
    df_data.index = df_data.index.rename('timestamp')
    df_data = df_data.reset_index()
    df_data = df_data[['timestamp', 'symbol', '__interval', 'open', 'high', 'low', 'close', 'volume']]
    return list(df_data.T.to_dict().values())


def insert_candles_to_db(l_candle) -> list:
    db = m_client['djongo_connection']
    l_new_candle = []
    for candle in l_candle:
        key = {'timestamp': candle['timestamp'],
               'symbol': candle['symbol'],
               '__interval': candle['__interval']}
        res = db['candles'].update(key, candle, upsert=True)
        if not res['updatedExisting']:
            l_new_candle.append(candle)

    return l_new_candle


def push_latest_candle_to_rabbit(candle):
    connection = pika.BlockingConnection(pika.URLParameters(settings.CLOUDAMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue='candles')
    channel.basic_publish(exchange='',
                          routing_key='candles',
                          body=candle)
    LOG.info("Sent new candle!")
    connection.close()


def get_latest_candle_json(l_candles) -> str:
    df_new_candles = pd.DataFrame(l_candles)
    return df_new_candles.iloc[df_new_candles['timestamp'].idxmax()].to_json()



