from alpha_vantage.timeseries import TimeSeries
import settings
from pymongo import MongoClient
import pandas as pd

m_client = MongoClient(settings.DB_CONN)
ts = TimeSeries(key=settings.API_KEY)


def get_alpha_vantage() -> list:
    data, meta_data = ts.get_intraday(settings.SYMBOL, interval='1min')
    return transform_av_response_to_candles(data,meta_data)


def transform_av_response_to_candles(av_data, av_meta_data) -> list:
    df_data = pd.DataFrame.from_dict(av_data, orient='index')
    df_data.columns = ['open', 'high', 'low', 'close', 'volume']
    df_data['symbol'] = settings.SYMBOL
    df_data['interval'] = av_meta_data['4. Interval']
    df_data.index = pd.to_datetime(df_data.index)
    df_data.index = df_data.index.tz_localize(av_meta_data['6. Time Zone']).tz_convert('Europe/Paris')
    df_data.index = df_data.index.rename('timestamp')
    df_data = df_data.reset_index()
    df_data = df_data[['timestamp', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']]
    print(list(df_data.T.to_dict().values()))
    return list(df_data.T.to_dict().values())


def insert_candles_to_db(l_candle):
    db = m_client['djongo_connection']
    for candle in l_candle:
        key = {'timestamp': candle['timestamp'],
               'symbol': candle['symbol'],
               'interval': candle['interval']}
        db['candles'].update(key, candle, upsert=True)

if __name__ == "__main__":
    candles = get_alpha_vantage()
    insert_candles_to_db(candles)