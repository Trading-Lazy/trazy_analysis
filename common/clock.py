import abc
from datetime import datetime, timedelta, timezone
from typing import Dict

from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from common.utils import timestamp_to_utc
from models.asset import Asset


class Clock:
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        self.market_cal = market_cal

    @abc.abstractmethod
    def current_time(
        self, tz="UTC", asset: Asset = Asset(symbol="", exchange="")
    ) -> datetime:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_time(
        self, asset: Asset, timestamp: datetime
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_bars(self, asset: Asset) -> None:  # pragma: no cover
        raise NotImplementedError

    def update(self, asset: Asset, timestamp: datetime):
        self.update_bars(asset)
        self.update_time(asset, timestamp)

    @abc.abstractmethod
    def bars(self, asset: Asset) -> int:  # pragma: no cover
        raise NotImplementedError

    def end_of_day(
        self, asset: Asset, threshold: timedelta = timedelta(minutes=5)
    ) -> bool:
        now = self.current_time(asset=asset)
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
    def current_time(self, tz=timezone.utc, asset: Asset = "") -> datetime:
        return datetime.now(tz=tz)

    def update_time(
        self, asset: Asset, timestamp: datetime
    ) -> None:  # pragma: no cover
        pass

    def update_bars(self, asset: Asset) -> None:  # pragma: no cover
        pass

    def bars(self, asset: Asset) -> int:  # pragma: no cover
        return 0


class SimulatedClock(Clock):
    def __init__(
        self, market_cal: MarketCalendar = AmericanStockExchangeCalendar()
    ) -> None:
        super().__init__(market_cal)
        self.time_dict: Dict[str, datetime] = {}
        self.bars_dict: Dict[str, int] = {}

    def update_time(self, asset: Asset, timestamp: datetime) -> None:
        self.time_dict[asset] = timestamp

    def update_bars(self, asset: Asset) -> None:
        if asset not in self.bars_dict:
            self.bars_dict[asset] = 1
        else:
            self.bars_dict[asset] += 1

    def current_time(
        self, tz=timezone.utc, asset: Asset = Asset(symbol="", exchange="")
    ) -> datetime:
        if asset not in self.time_dict:
            self.time_dict[asset] = datetime.now(timezone.utc)
        return self.time_dict[asset]

    def bars(self, asset) -> int:
        if asset not in self.bars_dict:
            return 0
        return self.bars_dict[asset]
