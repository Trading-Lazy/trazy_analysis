from strategy.ta import ma
import pandas as pd
import math


def test_resample_candle_data_interval_5_minute():
    candles = {
        'close': [94.1300, 94.0800, 94.0800, 94.1800, 94.2000]
    }

    df = pd.DataFrame(candles,
                      columns=['close'])
    df = ma(df, 3)

    expected_df_candles = {
        'close': [94.13, 94.08, 94.08, 94.18, 94.20],
        'MA_3':[None, None, (94.13 + 94.08 + 94.08) / 3, (94.08 + 94.08 + 94.18)/3, (94.08 + 94.18 + 94.20) / 3]
    }

    expected_df = pd.DataFrame(expected_df_candles,
                               columns=['close','MA_3'])

    assert (math.isnan(df.iloc[0]['MA_3']))
    assert (math.isnan(df.iloc[1]['MA_3']))

    assert (math.isclose(expected_df.iloc[2]['MA_3'], df.iloc[2]['MA_3'], rel_tol=1e-06))
    assert (math.isclose(expected_df.iloc[3]['MA_3'], df.iloc[3]['MA_3'], rel_tol=1e-06))
    assert (math.isclose(expected_df.iloc[4]['MA_3'], df.iloc[4]['MA_3'], rel_tol=1e-06))
