import json
from typing import List

from django.forms import model_to_dict
from pandas import Timestamp
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.core.frame import DataFrame

from actionsapi.models.models import Candle


def lists_equal(list1: list, list2: list) -> bool:
    return sorted(list1) == sorted(list2)


def validate_dataframe_columns(df: DataFrame, required_columns: list) -> None:
    if not lists_equal(required_columns, df.columns.tolist()):
        raise Exception(
            "The input dataframe is malformed. It must contain only columns: {} but has instead {}".format(
                required_columns, df.columns.tolist()
            )
        )


def build_candle_from_dict(candle_dict: dict) -> Candle:
    candle: Candle = Candle(
        symbol=candle_dict["symbol"],
        open=candle_dict["open"],
        high=candle_dict["high"],
        low=candle_dict["low"],
        close=candle_dict["close"],
        volume=candle_dict["volume"],
        timestamp=candle_dict["timestamp"],
    )
    if "_id" in candle_dict:
        candle._id = candle_dict["_id"]
    return candle


def build_candle_from_json_string(str_candle: str) -> Candle:
    action_json = json.loads(str_candle)
    action_json["timestamp"] = Timestamp(action_json["timestamp"], tz="UTC")
    return build_candle_from_dict(action_json)


def candles_to_dict(l_candles: List[Candle]) -> List[dict]:
    candles_list = []
    for candle in l_candles:
        candles_list.append(model_to_dict(candle))
    return candles_list
