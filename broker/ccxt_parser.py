from abc import ABC
from typing import Any, Dict, Tuple

from trazy_analysis.models.enums import Action


class Parser(ABC):
    @classmethod
    def parse_lot_size_info(cls, symbol_info: Any) -> Tuple[str, float]:
        raise NotImplementedError("Should implement parse_lot_size_info()")

    @classmethod
    def parse_price_info(cls, price_info: Any) -> Tuple[str, float]:
        raise NotImplementedError("Should implement parse_price_info()")

    @classmethod
    def parse_balances_info(cls, balance_info: Any) -> dict[str, float]:
        raise NotImplementedError("Should implement parse_balances_info()")

    @classmethod
    def parse_trade_info(
        cls, trade_info: Any
    ) -> Tuple[int, str, float, Action, float, str, float, str]:
        raise NotImplementedError("Should implement parse_trade_info()")

    @classmethod
    def parse_order_info(cls, order_info: Any) -> Tuple[str, str]:
        raise NotImplementedError("Should implement parse_order_info()")


class DummyParser(Parser):
    @classmethod
    def parse_lot_size_info(cls, symbol_info: Any) -> Tuple[str, float]:
        symbol = "symbol"
        lot_size = 1
        return symbol, lot_size

    @classmethod
    def parse_price_info(cls, price_info: Any) -> Tuple[str, float]:
        symbol = "symbol"
        price = 1
        return symbol, price

    @classmethod
    def parse_balances_info(cls, balance_info: Any) -> dict[str, float]:
        symbol = "symbol"
        balance = 1
        return {symbol: balance}

    @classmethod
    def parse_trade_info(
        cls, trade_info: Any
    ) -> Tuple[int, str, float, Action, float, str, float, str]:
        trade_epoch_ms = 0
        symbol = "symbol"
        size = 1
        action = Action.BUY
        price = 1
        order_id = "1"
        commission = 0.5
        transaction_id = "1"
        return (
            trade_epoch_ms,
            symbol,
            size,
            action,
            price,
            order_id,
            commission,
            transaction_id,
        )

    @classmethod
    def parse_order_info(cls, order_info: Any) -> Tuple[str, str]:
        order_id = "1"
        order_status = "FILLED"
        return order_id, order_status


class BinanceParser(Parser):
    BINANCE_SYMBOL_TO_TRAZY_SYMBOL = {}

    @classmethod
    def parse_lot_size_info(cls, symbol_info: Any) -> Tuple[str, float]:
        symbol_base = symbol_info["baseAsset"]
        quote_base = symbol_info["quoteAsset"]
        symbol_filters = symbol_info["filters"]
        for symbol_filter in symbol_filters:
            if symbol_filter["filterType"] != "LOT_SIZE":
                continue
            symbol_lot_size = float(symbol_filter["minQty"])
            break
        symbol = symbol_base + "/" + quote_base
        CcxtBinanceParser.BINANCE_SYMBOL_TO_TRAZY_SYMBOL[
            symbol_base + quote_base
        ] = symbol
        return symbol, symbol_lot_size

    @classmethod
    def parse_price_info(cls, price_info: Any) -> Tuple[str, float]:
        symbol = price_info["symbol"]
        price = float(price_info["price"])
        return CcxtBinanceParser.BINANCE_SYMBOL_TO_TRAZY_SYMBOL[symbol], price

    @classmethod
    def parse_balances_info(cls, balance_info: Any) -> dict[str, float]:
        balances = balance_info["balances"]
        return {
            balance["asset"]: float(balance["free"])
            for balance in balances
            if float(balance["free"]) != 0.0
        }

    @classmethod
    def parse_trade_info(
        cls, trade_info: Any
    ) -> Tuple[int, str, float, Action, float, str, float, str]:
        trade_epoch_ms = int(trade_info["time"])
        symbol = trade_info["symbol"]
        size = float(trade_info["qty"])
        action = Action.BUY if trade_info["isBuyer"] else Action.SELL
        price = float(trade_info["price"])
        order_id = str(trade_info["orderId"])
        commission = float(trade_info["commission"])
        transaction_id = trade_info["id"]
        return (
            trade_epoch_ms,
            symbol,
            size,
            action,
            price,
            order_id,
            commission,
            transaction_id,
        )

    @classmethod
    def parse_order_info(cls, order_info: Any) -> Tuple[str, str]:
        return order_info["orderId"], order_info["status"]


class CcxtBinanceParser(BinanceParser):
    @classmethod
    def parse_price_info(cls, price_info: Any) -> Tuple[str, float]:
        symbol = price_info["symbol"]
        price = float(price_info["lastPrice"])
        return CcxtBinanceParser.BINANCE_SYMBOL_TO_TRAZY_SYMBOL[symbol], price


class KucoinParser(Parser):
    KUCOIN_SYMBOL_TO_TRAZY_SYMBOL = {}

    @classmethod
    def parse_lot_size_info(cls, symbol_info: Any) -> Tuple[str, float]:
        kucoin_symbol = symbol_info["symbol"]
        symbol = kucoin_symbol.replace("-", "/")
        KucoinParser.KUCOIN_SYMBOL_TO_TRAZY_SYMBOL[kucoin_symbol] = symbol
        symbol_lot_size = float(symbol_info["baseMinSize"])
        return symbol, symbol_lot_size

    @classmethod
    def parse_price_info(cls, price_info: Any) -> Tuple[str, float]:
        symbol = price_info["symbol"]
        price = float(price_info["last"])
        return KucoinParser.KUCOIN_SYMBOL_TO_TRAZY_SYMBOL[symbol], price

    @classmethod
    def parse_balances_info(cls, balance_info: Any) -> dict[str, float]:
        trade_accounts = [
            account for account in balance_info if account["type"] == "trade"
        ]
        return {
            account["currency"]: float(account["available"])
            for account in trade_accounts
        }

    @classmethod
    def parse_trade_info(
        cls, trade_info: Any
    ) -> Tuple[int, str, float, Action, float, str, float, str]:
        trade_epoch_ms = int(trade_info["createdAt"])
        symbol = trade_info["symbol"].replace("-", "/")
        size = float(trade_info["size"])
        action = Action.BUY if trade_info["side"] == "buy" else Action.SELL
        price = float(trade_info["price"])
        order_id = str(trade_info["orderId"])
        commission = float(trade_info["fee"])
        transaction_id = trade_info["tradeId"]
        return (
            trade_epoch_ms,
            symbol,
            size,
            action,
            price,
            order_id,
            commission,
            transaction_id,
        )

    @classmethod
    def parse_order_info(cls, order_info: Any) -> Tuple[str, str]:
        return str(order_info["orderId"]), "FILLED"
