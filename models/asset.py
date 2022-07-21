from typing import Dict


class Asset:
    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange

    def __hash__(self):
        return hash((self.symbol, self.exchange))

    def __eq__(self, other: "Asset"):
        if isinstance(other, Asset):
            return self.symbol == other.symbol and self.exchange == other.exchange
        return False

    def __ne_(self, other: "Asset"):
        return not self.__eq__(other)

    def key(self):
        return f"{self.exchange}-{self.symbol}"

    def __lt__(self, other: "Asset"):
        return f"{self.exchange}-{self.symbol}" < f"{other.exchange}-{other.symbol}"

    def __le__(self, other: "Asset"):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other: "Asset"):
        return not self.__le__(other)

    def __ge__(self, other: "Asset"):
        return not self.__lt__(other)

    def __str__(self):
        return (
            "Asset(" 'symbol="{}",' 'exchange="{}")'.format(self.symbol, self.exchange)
        )

    @staticmethod
    def from_dict(asset_dict: dict):
        return Asset(symbol=asset_dict["symbol"], exchange=asset_dict["exchange"])

    def to_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
        }
