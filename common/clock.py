import abc
from datetime import datetime, timedelta, timezone
from typing import Dict

from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from common.utils import timestamp_to_utc


class Clock:
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        self.market_cal = market_cal

    @abc.abstractmethod
    def current_time(self, tz="UTC", symbol: str = "") -> datetime:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_time(self, symbol: str, timestamp: datetime) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def update(self, symbol: str, timestamp: datetime):
        self.update_bars(symbol)
        self.update_time(symbol, timestamp)

    @abc.abstractmethod
    def bars(self, symbol) -> int:  # pragma: no cover
        raise NotImplementedError

    def end_of_day(
        self, symbol: str, threshold: timedelta = timedelta(minutes=5)
    ) -> bool:
        now = self.current_time(symbol=symbol)
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
    def current_time(self, tz=timezone.utc, symbol: str = "") -> datetime:
        return datetime.now(tz=tz)

    def update_time(self, symbol: str, timestamp: datetime) -> None:  # pragma: no cover
        pass

    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        pass

    def bars(self, symbol) -> int:  # pragma: no cover
        return 0


class SimulatedClock(Clock):
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        super().__init__(market_cal)
        self.time_dict: Dict[str, datetime] = {}
        self.bars_dict: Dict[str, int] = {}

    def update_time(self, symbol: str, timestamp: datetime) -> None:
        self.time_dict[symbol] = timestamp

    def update_bars(self, symbol: str) -> None:
        if symbol not in self.bars_dict:
            self.bars_dict[symbol] = 1
        else:
            self.bars_dict[symbol] += 1

    def current_time(self, tz=timezone.utc, symbol: str = "") -> datetime:
        if symbol not in self.time_dict:
            self.time_dict[symbol] = datetime.now(timezone.utc)
        return self.time_dict[symbol]

    def bars(self, symbol) -> int:
        if symbol not in self.bars_dict:
            return 0
        return self.bars_dict[symbol]
