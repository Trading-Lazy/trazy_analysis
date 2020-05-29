from datetime import datetime, timedelta
from pandas_market_calendars import MarketCalendar
from common.calendar import is_business_hour, is_business_day
import pytz

MINUTE_IN_ONE_HOUR = 60


class TimeInterval:
    def __init__(self, interval_unit:str=None, interval_value:int=None):
        self.interval_unit: str = interval_unit
        self.interval_value: int = interval_value

    def __str__(self):
        return '{} {}'.format(self.interval_value, self.interval_unit)

    @staticmethod
    def process_interval(str_interval: str):
        i = str_interval.split(' ')
        if len(i) != 2:
            raise Exception('The input string interval is malformed.')
        interval_unit: str = i[1].lower()
        interval_value: int = int(i[0])
        if interval_unit not in ['day', 'minute']:
            raise Exception('Unknown interval unit {}'.format(interval_unit))
        return TimeInterval(interval_unit, interval_value)


def round_time(dt=None, time_delta=timedelta(minutes=1)):
    """Round down a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    time_delta : timedelta object, we round to a multiple of this, default 1 minute.
    """
    round_to = time_delta.total_seconds()
    seconds = dt.timestamp()

    # // is a floor division
    rounding = seconds // round_to * round_to

    return dt + timedelta(0, rounding-seconds, -dt.microsecond)


def find_start_interval_business_minute(end_timestamp: datetime,
                                        business_calendar: MarketCalendar,
                                        interval: TimeInterval,
                                        nb_valid_interval: int):
    """
    Returns the minimum start time that gives enough timespan from the end_timestamp to
    cover nb_valid_interval of business hours.
    :param end_timestamp: end timestamp of the time period
    :param business_calendar: business calendar to define the business hour
    :param interval: time interval
    :param nb_valid_interval: the number of valid business hours interval to cover
    :return:
    """
    if interval.interval_unit != 'minute':
        raise Exception("Time interval not recognized")

    if MINUTE_IN_ONE_HOUR % interval.interval_value != 0:
        raise Exception("Not a valid time interval")

    max_start_offset_in_days = 365
    max_start = end_timestamp - timedelta(days=max_start_offset_in_days)
    df_business_calendar = business_calendar.schedule(start_date=max_start.strftime('%Y-%m-%d'),
                                                      end_date=end_timestamp.strftime('%Y-%m-%d'))

    end = round_time(end_timestamp.astimezone(pytz.utc), time_delta=timedelta(minutes=interval.interval_value))
    start_timestamp = end.replace(tzinfo=pytz.UTC)

    i = 0
    while i < nb_valid_interval and start_timestamp > max_start:
        if is_business_hour(start_timestamp, df_business_calendar=df_business_calendar):
            i += 1
        start_timestamp = start_timestamp - timedelta(minutes=interval.interval_value)
        
    return start_timestamp


def find_start_interval_business_date(end_timestamp: datetime,
                                      business_calendar: MarketCalendar,
                                      interval: TimeInterval,
                                      nb_valid_interval: int):
    """
    Returns the minimum start date that gives enough timespan from the end_timestamp to
    cover nb_valid_interval of business days.
    :param end_timestamp: end timestamp of the time period
    :param business_calendar: business calendar to define the business hour
    :param interval: time interval
    :param nb_valid_interval: the number of valid business days interval to cover
    :return:
    """
    if interval.interval_unit != 'day':
        raise Exception("Time interval not recognized")

    max_start_offset_in_days = 365
    max_start = end_timestamp - timedelta(days=max_start_offset_in_days)
    df_business_calendar = business_calendar.schedule(start_date=max_start.strftime('%Y-%m-%d'),
                                                      end_date=end_timestamp.strftime('%Y-%m-%d'))

    td = timedelta(hours=end_timestamp.hour,
                                    minutes=end_timestamp.minute,
                                    seconds=end_timestamp.second,
                                    microseconds=end_timestamp.microsecond)

    if td.total_seconds() > 0 and end_timestamp.date() in df_business_calendar.index.date:
        i = 1
    else:
        i = 0
    end = end_timestamp - td
    start_timestamp = end.replace(tzinfo=pytz.UTC)

    while i < nb_valid_interval and start_timestamp > max_start:
        start_timestamp = start_timestamp - timedelta(days=interval.interval_value)
        if is_business_day(start_timestamp, df_business_calendar=df_business_calendar):
            i += 1

    return start_timestamp
