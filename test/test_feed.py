from feed.feed import transform_av_response_to_candles


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
        '6. Time Zone': 'US/Eastern'
    }

    l_candles = transform_av_response_to_candles(data,meta_data)

    assert len(l_candles) == 3, "test failed"
    assert 'timestamp' in l_candles[0]
    assert 'open' in l_candles[0]
    assert 'high' in l_candles[0]
    assert 'low' in l_candles[0]
    assert 'close' in l_candles[0]
    assert 'volume' in l_candles[0]
    assert 'symbol' in l_candles[0]
