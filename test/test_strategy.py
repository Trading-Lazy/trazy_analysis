from strategy.strategy import StrategyName
from strategy.strategies.SmaCrossoverStrategy import SmaCrossoverStrategy
from actionsapi.models import Action, Candle
from actionsapi.models import PositionType, ActionType
import pandas as pd
from datetime import datetime
from pytz import timezone
from strategy.constants import DATE_FORMAT
import math
import numpy as np

SCO: SmaCrossoverStrategy = SmaCrossoverStrategy()
OBJECT_ID_BASE = "5eae9ddd4d6f4e006f67c9c"


def get_df_hist() -> pd.DataFrame:
    df_hist = {
            'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                          '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
            'symbol': ['ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA'],
            'open': [94.12, 94.07, 94.07, 94.17, 94.17],
            'high': [94.15, 94.10, 94.10, 94.18, 94.18],
            'low': [94.00, 93.95, 93.95, 94.05, 94.05],
            'close': [94.13, 94.08, 94.08, 94.18, 94.18],
            'volume': [7, 91, 0, 0, 0]
        }
    df_hist = pd.DataFrame(df_hist,
                               columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'], format=DATE_FORMAT)
    df_hist.set_index('timestamp', inplace=True)
    return df_hist


def test_smacrossover_get_time_offset():
    SCO.set_interval('1 day')
    assert (SCO.get_time_offset() == pd.offsets.Day(1))

    SCO.set_interval('30 minute')
    assert (SCO.get_time_offset() == pd.offsets.Minute(30))


def test_smacrossover_calc_time_range_1_day_interval():
    SCO.set_parameters({
        'interval': '1 day',
        'short_period': 3,
        'long_period': 8
    })

    start = SCO.calc_required_history_start_timestamp(datetime(2020, 5, 1, 0, 0, tzinfo=timezone('UTC')))
    assert (start == datetime(2020, 4, 20, 0, 0, tzinfo=timezone('UTC')))


def test_smacrossover_calc_time_range_1_day_interval_2():
    SCO.set_parameters({
        'interval': '1 day',
        'short_period': 3,
        'long_period': 8
    })

    start = SCO.calc_required_history_start_timestamp(datetime(2020, 4, 30, 0, 0, tzinfo=timezone('UTC')))
    assert (start == datetime(2020, 4, 17, 0, 0, tzinfo=timezone('UTC')))


def test_smacrossover_calc_time_range_30_minute_interval_on_business_hour():
    SCO.set_parameters({
        'interval': '30 minute',
        'short_period': 2,
        'long_period': 4
    })

    start = SCO.calc_required_history_start_timestamp(datetime(2020, 4, 30, 15, 0, tzinfo=timezone('UTC')))
    assert (start == datetime(2020, 4, 30, 13, 00, tzinfo=timezone('UTC')))


def test_smacrossover_calc_time_range_30_minute_interval_on_non_business_hour():
    SCO.set_parameters({
        'interval': '30 minute',
        'short_period': 2,
        'long_period': 4
    })
    start = SCO.calc_required_history_start_timestamp(datetime(2020, 5, 1, 12, 0, tzinfo=timezone('UTC')))
    assert (start == datetime(2020, 4, 30, 13, 30, tzinfo=timezone('UTC')))


def test_smacrossover_get_candles_with_signals_positions():
    SCO.set_parameters({
        'interval': '30 minute',
        'short_period': 2,
        'long_period': 4
    })

    df_signals_positions = SCO.get_candles_with_signals_positions(get_df_hist())

    expected_df_signals_positions = {
        'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                      '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
        'signals': [0.0, 0.0, 0.0, 1.0, 1.0],
        'positions': [np.nan, 0.0, 0.0, 1.0, 0.0]
    }
    expected_df_signals_positions = pd.DataFrame(expected_df_signals_positions,
                           columns=['timestamp','signals', 'positions'])
    expected_df_signals_positions['timestamp'] = pd.to_datetime(expected_df_signals_positions['timestamp'],
                                                                format=DATE_FORMAT)
    expected_df_signals_positions.set_index('timestamp', inplace=True)

    assert (math.isnan(df_signals_positions.iloc[0]['positions']))

    comparison = (df_signals_positions[['signals','positions']].iloc[1:,:] ==
            expected_df_signals_positions[['signals', 'positions']].iloc[1:, :])

    assert (comparison.all().all())

    assert (math.isnan(df_signals_positions.iloc[0]['positions']))
    assert (df_signals_positions.iloc[0]['signals']==0)


def test_smacrossover_conclude_action_position():
    # action = None and position = None
    df_signals_positions = {
        'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                      '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
        'signals': [0.0, 0.0, 0.0, 1.0, 1.0],
        'positions': [np.nan, 0.0, 0.0, 1.0, 0.0]
    }
    df_signals_positions = pd.DataFrame(df_signals_positions,
                                                 columns=['timestamp', 'signals', 'positions'])
    df_signals_positions['timestamp'] = pd.to_datetime(df_signals_positions['timestamp'],
                                                                format=DATE_FORMAT)
    df_signals_positions.set_index('timestamp', inplace=True)

    action, position = SmaCrossoverStrategy.conclude_action_position(df_signals_positions)
    assert (action is None and position is None)

    # action = BUY and position = LONG
    df_signals_positions = {
        'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                      '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
        'signals': [0.0, 0.0, 0.0, 0.0, 1.0],
        'positions': [np.nan, 0.0, 0.0, 0.0, 1.0]
    }
    df_signals_positions = pd.DataFrame(df_signals_positions,
                                        columns=['timestamp', 'signals', 'positions'])
    df_signals_positions['timestamp'] = pd.to_datetime(df_signals_positions['timestamp'],
                                                       format=DATE_FORMAT)
    df_signals_positions.set_index('timestamp', inplace=True)
    action, position = SmaCrossoverStrategy.conclude_action_position(df_signals_positions)
    assert (action == ActionType.BUY and position == PositionType.LONG)

    # action = BUY and position = LONG
    df_signals_positions = {
        'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                      '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
        'signals': [0.0, 0.0, 0.0, 0.0, -1.0],
        'positions': [np.nan, 0.0, 0.0, 0.0, -1.0]
    }
    df_signals_positions = pd.DataFrame(df_signals_positions,
                                        columns=['timestamp', 'signals', 'positions'])
    df_signals_positions['timestamp'] = pd.to_datetime(df_signals_positions['timestamp'],
                                                       format=DATE_FORMAT)
    df_signals_positions.set_index('timestamp', inplace=True)
    action, position = SmaCrossoverStrategy.conclude_action_position(df_signals_positions)
    assert (action == ActionType.SELL and position == PositionType.LONG)


def test_smacrossover_build_action():
    candle = Candle(symbol='ANX.PA', open=94.10, high=94.12, low=94.00, close=94.12, volume=2, timestamp=None)
    action1 = SmaCrossoverStrategy.build_action(candle,ActionType.BUY, PositionType.LONG)
    assert (action1.action_type == ActionType.BUY)
    assert (action1.position_type == PositionType.LONG)
    assert (action1.symbol == 'ANX.PA')
    assert (action1.strategy == StrategyName.SMA_CROSSOVER.name)

    action2 = SmaCrossoverStrategy.build_action(candle, ActionType.SELL, PositionType.LONG)
    assert (action2.action_type == ActionType.SELL)
    assert (action2.position_type == PositionType.LONG)
    assert (action2.symbol == 'ANX.PA')
    assert (action2.strategy == StrategyName.SMA_CROSSOVER.name)


def test_smacrossover_calc_strategy():
    SCO.set_parameters({
        'interval': '30 minute',
        'short_period': 2,
        'long_period': 4
    })

    df_hist = {
        'timestamp': ['2020-05-08 14:00:00', '2020-05-08 14:30:00', '2020-05-08 15:00:00',
                      '2020-05-08 15:30:00', '2020-05-08 16:00:00'],
        'symbol': ['ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA'],
        'open': [94.12, 94.07, 94.07, 94.17, 94.17],
        'high': [94.15, 94.10, 94.10, 94.18, 94.18],
        'low': [94.00, 93.95, 93.95, 94.05, 94.05],
        'close': [94.13, 94.08, 94.08, 94.10, 94.18],
        'volume': [7, 91, 0, 0, 0]
    }
    df_hist = pd.DataFrame(df_hist,
                           columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'], format=DATE_FORMAT)
    df_hist.set_index('timestamp', inplace=True)
    candle: Candle = Candle(_id=OBJECT_ID_BASE + "1", symbol='ANX.PA', open=94.10, high=94.12, low=94.00,
                            close=94.12, volume=2, timestamp=None)
    action: Action = SCO.calc_strategy(candle, df_hist)
    assert (action.action_type == ActionType.BUY)
    assert (action.position_type == PositionType.LONG)
    assert (action.candle_id == OBJECT_ID_BASE + "1")
