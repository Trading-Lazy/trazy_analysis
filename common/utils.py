from django.db.models.query import QuerySet
from django.forms import model_to_dict
from pandas import DataFrame, Timestamp
import json
from actionsapi.models import Candle

def validate_dataframe_columns(df: DataFrame, required_columns: list):
    sorted_required_columns = sorted(required_columns)
    sorted_columns = sorted(df.columns.tolist())
    if sorted_columns != sorted_required_columns:
        raise Exception(
            "The input dataframe is malformed. It must contain only columns: {} but has instead {}".format(
                required_columns, df.columns.tolist()
            )
        )


def build_candle_from_dict(candle_dict: dict) -> Candle:
    return Candle(symbol=candle_dict['symbol'],
                  open=candle_dict['open'],
                  high=candle_dict['high'],
                  low=candle_dict['low'],
                  close=candle_dict['close'],
                  volume=candle_dict['volume'],
                  timestamp=candle_dict['timestamp'])


def build_candle_from_json_string(str_candle) -> Candle:
    action_json = json.loads(str_candle)
    action_json['timestamp'] = Timestamp(action_json['timestamp'])
    return build_candle_from_dict(action_json)


def candles_to_dict(l_candles):
    candles_list = []
    for candle in l_candles:
        candles_list.append(model_to_dict(candle))
    return candles_list