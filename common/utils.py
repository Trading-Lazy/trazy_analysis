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
    return Candle(symbol=action_json['symbol'],
                    open=action_json['open'],
                    high=action_json['high'],
                    low=action_json['low'],
                    close=action_json['close'],
                    volume=action_json['volume'],
                    timestamp=Timestamp(action_json['timestamp']))