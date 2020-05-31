from feed.feed import transform_av_response_to_candles, insert_candles_to_db, get_latest_candle_json
from common.utils import build_candle_from_dict
from actionsapi.models import Candle
import pandas as pd
from test.tools.tools import clean_candles_in_db
from typing import List
import json


def test_transform_av_response_to_candles():
    data = {
        '2020-04-30 11:15:00': {
            '1. open': '91.2800',
            '2. high': '91.2900',
            '3. low': '91.2800',
            '4. close': '91.2800',
            '5. volume': '8504'
        },
        '2020-04-30 10:45:00': {
            '1. open': '91.4000',
            '2. high': '91.4000',
            '3. low': '90.9500',
            '4. close': '90.9500',
            '5. volume': '33'
        },
        '2020-04-30 10:30:00': {
            '1. open': '91.8000',
            '2. high': '91.9200',
            '3. low': '91.8000',
            '4. close': '91.9200',
            '5. volume': '25'
        }
    }
    meta_data = {
        '1. Information': 'Intraday (1min) open, high, low, close prices and volume',
        '2. Symbol': 'ANX.PA',
        '3. Last Refreshed': '2020-04-30 11:18:00',
        '4. Interval': '1min',
        '5. Output Size': 'Compact',
        '6. Time Zone': 'US/Eastern'  # GMT-2
    }

    l_candles = transform_av_response_to_candles(data,meta_data)

    assert len(l_candles) == 3, "test failed"

    candle_0 = l_candles[0]
    assert candle_0.symbol == 'ANX.PA'
    assert candle_0.open == 91.28
    assert candle_0.high == 91.29
    assert candle_0.low == 91.28
    assert candle_0.close == 91.28
    assert candle_0.volume == 8504
    assert candle_0.timestamp == pd.Timestamp('2020-04-30T17:15:00', tz='Europe/Paris')

    candle_1 = l_candles[1]
    assert candle_1.symbol == 'ANX.PA'
    assert candle_1.open == 91.4
    assert candle_1.high == 91.4
    assert candle_1.low == 90.95
    assert candle_1.close == 90.95
    assert candle_1.volume == 33
    assert candle_1.timestamp == pd.Timestamp('2020-04-30T16:45:00', tz='Europe/Paris')

    candle_2 = l_candles[2]
    assert candle_2.symbol == 'ANX.PA'
    assert candle_2.open == 91.8
    assert candle_2.high == 91.92
    assert candle_2.low == 91.8
    assert candle_2.close == 91.92
    assert candle_2.volume == 25
    assert candle_2.timestamp == pd.Timestamp('2020-04-30T16:30:00', tz='Europe/Paris')


def test_insert_candles_to_db():
    clean_candles_in_db()
    candle = Candle(symbol='ANX.PA',
                    open=91.8,
                    high=91.92,
                    low=91.8,
                    close=91.92,
                    volume=25,
                    timestamp=pd.Timestamp('2020-04-30T16:30:00', tz='Europe/Paris')
    )
    new_candles: List[Candle] = insert_candles_to_db([candle])
    assert len(new_candles) == 1
    inserted_candle = new_candles[0]
    assert inserted_candle.symbol == 'ANX.PA'
    assert inserted_candle.open == 91.8
    assert inserted_candle.high == 91.92
    assert inserted_candle.low == 91.8
    assert inserted_candle.close == 91.92
    assert inserted_candle.volume == 25
    assert inserted_candle.timestamp == pd.Timestamp('2020-04-30T16:30:00', tz='Europe/Paris')

    second_candle = Candle(symbol='ANX.PA',
                    open=91.92,
                    high=92,
                    low=91,
                    close=92,
                    volume=20,
                    timestamp=pd.Timestamp('2020-04-30T17:30:00', tz='Europe/Paris')
    )
    new_candles: List[Candle] = insert_candles_to_db([candle, second_candle])
    assert len(new_candles) == 1
    inserted_candle = new_candles[0]
    assert inserted_candle.symbol == 'ANX.PA'
    assert inserted_candle.open == 91.92
    assert inserted_candle.high == 92
    assert inserted_candle.low == 91
    assert inserted_candle.close == 92
    assert inserted_candle.volume == 20
    assert inserted_candle.timestamp == pd.Timestamp('2020-04-30T17:30:00', tz='Europe/Paris')

    all_candles: List[Candle] = Candle.objects.all()
    assert len(all_candles) == 2


def test_get_latest_candle_json():
    candle_1 = Candle(
        _id='5ed4239be4805bb09a8939f9',
        symbol='ANX.PA',
        open=91.8,
        high=91.92,
        low=91.8,
        close=91.92,
        volume=25,
        timestamp=pd.Timestamp('2020-04-30T16:30:00', tz='Europe/Paris')
    )
    candle_2 = Candle(
        _id='5ed4239be4805bb09a89399a',
        symbol='ANX.PA',
        open=91.92,
        high=92.00,
        low=91.00,
        close=92.00,
        volume=20,
        timestamp=pd.Timestamp('2020-04-30T17:30:00', tz='Europe/Paris')
    )
    l_candles = [candle_1, candle_2]
    str_json = get_latest_candle_json(l_candles)
    obj_json = json.loads(str_json)

    assert obj_json['symbol'] == 'ANX.PA'
    assert obj_json['open'] == 91.92
    assert obj_json['high'] == 92.00
    assert obj_json['low'] == 91.00
    assert obj_json['close'] == 92.00
    assert obj_json['volume'] == 20
    assert pd.Timestamp(obj_json['timestamp'], tz='Europe/Paris') == pd.Timestamp('2020-04-30T17:30:00', tz='Europe/Paris')
