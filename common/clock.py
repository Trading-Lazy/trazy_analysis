import abc
from datetime import datetime, timedelta

import pytz
from pandas_market_calendars import MarketCalendar

from trazy_analysis.common.american_stock_exchange_calendar import (
    AmericanStockExchangeCalendar,
)
from trazy_analysis.common.utils import timestamp_to_utc


class Clock:
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        self.market_cal = market_cal

    @abc.abstractmethod
    def current_time(self, tz=pytz.UTC) -> datetime:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_time(self, timestamp: datetime) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_bars(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def update(self, timestamp: datetime):
        self.update_bars()
        self.update_time(timestamp)

    @abc.abstractmethod
    def current_bars(self) -> int:  # pragma: no cover
        raise NotImplementedError

    def end_of_day(self, threshold: timedelta = timedelta(minutes=5)) -> bool:
        now = self.current_time()
        date = now.date()
        time = self.market_cal.close_time_default
        end_of_day_datetime = datetime(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=time.hour,
            minute=time.minute,
            microsecond=time.microsecond,
        )
        end_of_day_datetime = self.market_cal.tz.localize(end_of_day_datetime)
        end_of_day_datetime = timestamp_to_utc(end_of_day_datetime)
        return end_of_day_datetime <= now + threshold


class LiveClock(Clock):
    def current_time(self, tz=pytz.UTC) -> datetime:
        return datetime.now(tz=tz)

    def update_time(self, timestamp: datetime) -> None:  # pragma: no cover
        pass

    def update_bars(self) -> None:  # pragma: no cover
        pass

    def current_bars(self) -> int:  # pragma: no cover
        return 0


class SimulatedClock(Clock):
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        super().__init__(market_cal)
        self.timestamp: datetime = None
        self.bars: int = 0

    def update_time(self, timestamp: datetime) -> None:
        self.timestamp = timestamp

    def update_bars(self) -> None:
        self.bars += 1

    def current_time(self, tz=pytz.UTC) -> datetime:
        if self.timestamp is None:
            self.timestamp = datetime.now(pytz.UTC)
        return self.timestamp

    def current_bars(self) -> int:
        return self.bars
