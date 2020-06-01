from alpha_vantage.timeseries import TimeSeries
from pymongo import MongoClient

import pandas as pd
import pika
import logger
import os
from actionsapi.models import Candle
from typing import List
from django.forms import model_to_dict
from common.utils import build_candle_from_dict, candles_to_dict

import settings

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

ts = TimeSeries(key=settings.API_KEY)


def get_alpha_vantage() -> list:
    data, meta_data = ts.get_intraday(settings.SYMBOL, interval='1min')
    return transform_av_response_to_candles(data,meta_data)


def transform_av_response_to_candles(av_data, av_meta_data) -> List[Candle]:
    df_data = pd.DataFrame.from_dict(av_data, orient='index')
    df_data.columns = ['open', 'high', 'low', 'close', 'volume']
    df_data['open'] = pd.to_numeric(df_data['open'])
    df_data['high'] = pd.to_numeric(df_data['high'])
    df_data['low'] = pd.to_numeric(df_data['low'])
    df_data['close'] = pd.to_numeric(df_data['close'])
    df_data['volume'] = pd.to_numeric(df_data['volume'])
    df_data['symbol'] = settings.SYMBOL
    df_data['interval'] = av_meta_data['4. Interval']
    df_data.index = pd.to_datetime(df_data.index)
    df_data.index = df_data.index.tz_localize(av_meta_data['6. Time Zone']).tz_convert('Europe/Paris')
    df_data.index = df_data.index.rename('timestamp')
    df_data = df_data.reset_index()
    df_data = df_data[['timestamp', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']]
    l_candles = [build_candle_from_dict(c) for c in list(df_data.T.to_dict().values())]
    return l_candles


def insert_candles_to_db(l_candle) -> List[Candle]:
    l_new_candle = []
    for candle in l_candle:
        existing_candle = Candle.objects.all().filter(
            symbol=candle.symbol,
            timestamp=candle.timestamp
        )
        if existing_candle is None or len(existing_candle) == 0:
            candle.save()
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


def get_latest_candle_json(l_candles: List[Candle]) -> str:
    candles_list = candles_to_dict(l_candles)
    df_new_candles = pd.DataFrame(candles_list)
    df_new_candles['_id'] = df_new_candles['_id'].astype(str)
    return df_new_candles.iloc[df_new_candles['timestamp'].idxmax()].to_json(date_format='iso')






