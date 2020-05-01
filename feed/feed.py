from alpha_vantage.timeseries import TimeSeries
import settings
from pymongo import MongoClient
import pandas as pd

m_client = MongoClient(settings.CONFIG['db_conn'])
ts = TimeSeries(key=settings.CONFIG['api_key'])


def get_alpha_vantage() -> list:
    data, meta_data = ts.get_intraday(settings.CONFIG['symbol'], interval='1min')

    df_data = pd.DataFrame.from_dict(data, orient='index')
    df_data.columns = ['open', 'high', 'low', 'close', 'volume']
    df_data['symbol'] = 'ANX.PA'
    df_data['interval'] = meta_data['4. Interval']
    df_data.index = pd.to_datetime(df_data.index)
    df_data.index = df_data.index.tz_localize(meta_data['6. Time Zone']).tz_convert('Europe/Paris')
    df_data.index = df_data.index.rename('timestamp')
    df_data = df_data.reset_index()
    df_data = df_data[['timestamp', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']]
    data = list(df_data.T.to_dict().values())
    return data


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