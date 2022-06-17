from datetime import timedelta
from typing import Dict


class Asset:
    def __init__(self, symbol: str, exchange: str, time_unit=timedelta(minutes=1)):
        self.symbol = symbol
        self.exchange = exchange
        self.time_unit = time_unit

    def __hash__(self):
        return hash((self.symbol, self.exchange, self.time_unit))

    def __eq__(self, other: "Asset"):
        if isinstance(other, Asset):
            return (
                self.symbol == other.symbol
                and self.exchange == other.exchange
                and self.time_unit == other.time_unit
            )
        return False

    def __ne_(self, other: "Asset"):
        return not self.__eq__(other)

    def key(self):
        return f"{self.exchange}-{self.symbol}-{self.time_unit}"

    def __lt__(self, other: "Asset"):
        name = f"{self.exchange}-{self.symbol}"
        other_name = f"{other.exchange}-{other.symbol}"
        return (
            name < other_name or name == other_name and self.time_unit < other.time_unit
        )

    def __le__(self, other: "Asset"):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other: "Asset"):
        return not self.__le__(other)

    def __ge__(self, other: "Asset"):
        return not self.__lt__(other)

    def __str__(self):
        return (
            "Asset("
            'symbol="{}",'
            'exchange="{}",'
            'time_unit="{}")'.format(
                self.symbol, self.exchange, self.time_unit
            )
        )

    @staticmethod
    def from_dict(asset_dict: dict):
        from trazy_analysis.common.helper import parse_timedelta_str

        return Asset(
            symbol=asset_dict["symbol"],
            exchange=asset_dict["exchange"],
            time_unit=parse_timedelta_str(asset_dict["time_unit"]),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "time_unit": str(self.time_unit),
        }
