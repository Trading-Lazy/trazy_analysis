from datetime import time

from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    GoodFriday,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
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


class AmericanStockExchangeCalendar(MarketCalendar):
    """
    Exchange calendar for American stock exchange
    """

    aliases = ["AmericanStock"]

    def __init__(self, open_time=None, close_time=None):
        super().__init__(open_time, close_time)

    @property
    def name(self):
        return "AmericanStockExchange"

    @property
    def tz(self):
        return timezone("US/Eastern")

    @property
    def open_time_default(self):
        return time(9, 30, tzinfo=self.tz)

    @property
    def close_time_default(self):
        return time(16, 0, tzinfo=self.tz)

    @property
    def regular_holidays(self):
        return AbstractHolidayCalendar(
            rules=[
                NewYearsDay,
                USMartinLutherKingJr,
                USPresidentsDay,
                GoodFriday,
                USMemorialDay,
                IndependenceDay,
                USLaborDay,
                USThanksgivingDay,
                ChristmasDay,
            ]
        )

    @property
    def special_closes(self):
        return [
            (
                time(14, 5),
                AbstractHolidayCalendar(
                    rules=[
                        ChristmasDay,
                        NewYearsDay,
                    ]
                ),
            )
        ]
