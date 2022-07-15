from datetime import time

from pandas.tseries.holiday import (
    Holiday,
)
from pandas_market_calendars import MarketCalendar
from pandas_market_calendars.market_calendar import (
    FRIDAY,
    MONDAY,
    THURSDAY,
    TUESDAY,
    WEDNESDAY,
)
from pytz import timezone

# New Year's Day
NewYearsDay = Holiday(
    "New Year's Day",
    month=1,
    day=1,
    days_of_week=(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY),
)

# Independence Day
IndependenceDay = Holiday(
    "Independence Day",
    month=7,
    day=5,
    days_of_week=(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY),
)

# Christmas Day
ChristmasDay = Holiday(
    "Christmas",
    month=12,
    day=24,
    days_of_week=(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY),
)


class CryptoExchangeCalendar(MarketCalendar):
    """
    Exchange calendar for Crypto stock exchange
    """

    aliases = ["CryptoStock"]
    regular_market_times = {
        "market_open": ((None, time(0)),),
        "market_close": ((None, time(23, 59)),),
    }

    def __init__(self, open_time=None, close_time=None):
        super().__init__(open_time, close_time)

    @property
    def name(self):
        return "CryptoStockExchange"

    @property
    def tz(self):
        return timezone("UTC")

    @property
    def open_time_default(self):
        return time(0, 0, tzinfo=self.tz)

    @property
    def close_time_default(self):
        return time(0, 0, tzinfo=self.tz)

    @property
    def weekmask(self):
        return "Mon Tue Wed Thu Fri Sat Sun"
