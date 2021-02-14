import os
import queue
from abc import ABCMeta, abstractmethod
from typing import List, Union

import numpy as np

import settings
from common.clock import Clock
from common.helper import get_or_create_nested_dict
from logger import logger
from models.candle import Candle
from models.enums import Direction, OrderStatus
from models.multiple_order import MultipleOrder, OcoOrder, SequentialOrder
from models.order import Order
from portfolio.portfolio import Portfolio

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Broker:
    """
    This abstract class provides an interface to a
    generic broker entity. Both simulated and live brokers
    will be derived from this ABC. This ensures that trading
    algorithm specific logic is completely identical for both
    simulated and live environments.
    The Broker has an associated master denominated currency
    through which all subscriptions and withdrawals will occur.
    The Broker entity has a portfolio that handles PnL.
    The Broker can execute orders. It contains a queue of
    open orders, needed for handling closed market situations.
    The Broker also supports history events for the portfolio
    """

    __metaclass__ = ABCMeta

    def __init__(
        self,
        clock: Clock,
        base_currency: str = "EUR",
        supported_currencies: List[str] = ["EUR", "USD"],
    ):
        self.supported_currencies = supported_currencies
        self.base_currency = self._set_base_currency(base_currency)
        self.clock = clock
        self.cash_balances = dict(
            (currency, 0) for currency in self.supported_currencies
        )
        self.open_orders = self._set_initial_open_orders()
        self.exit_orders = self._set_initial_exit_orders()
        self.last_prices = self._set_initial_last_prices()

        # create portfolio
        self._create_initial_portfolio()

    def _set_base_currency(self, base_currency: str) -> str:
        """
        Check and set the base currency from a list of
        allowed currencies. Raise ValueError if the
        currency is currently not supported by QSTrader.
        Parameters
        ----------
        base_currency : `str`
            The base currency string.
        Returns
        -------
        `str`
            The base currency string.
        """
        if base_currency not in self.supported_currencies:
            raise ValueError(
                "Currency '%s' is not supported by QSTrader. Could not "
                "set the base currency in the SimulatedBroker "
                "entity." % base_currency
            )
        else:
            return base_currency

    def _set_initial_open_orders(self) -> queue.Queue():
        """
        Set the appropriate initial open orders dictionary.
        Returns
        -------
        `dict`
            The empty initial open orders dictionary.
        """
        return queue.Queue()

    def _set_initial_exit_orders(self) -> dict:
        """
        Set the appropriate initial exit orders dictionary.
        Returns
        -------
        `dict`
            The empty initial open orders dictionary.
        """
        return {}

    def _set_initial_last_prices(self) -> dict:
        """
        Set the appropriate initial last prices dictionary.
        Returns
        -------
        `dict`
            The empty initial last prices dictionary.
        """
        return {}

    @abstractmethod
    def has_opened_position(
        self, symbol: str, direction: Direction
    ) -> bool:  # pragma: no cover
        raise NotImplementedError("Should implement has_opened_position()")

    @abstractmethod
    def subscribe_funds_to_account(self, amount: float) -> None:  # pragma: no cover
        raise NotImplementedError("Should implement subscribe_funds_to_account()")

    @abstractmethod
    def withdraw_funds_from_account(self, amount: float) -> None:  # pragma: no cover
        raise NotImplementedError("Should implement withdraw_funds_from_account()")

    @abstractmethod
    def get_cash_balance(
        self, currency: str = None
    ) -> Union[dict, float]:  # pragma: no cover
        raise NotImplementedError("Should implement get_account_cash_balance()")

    @abstractmethod
    def max_entry_order_size(
        self, symbol: str, direction: Direction, cash: float = None
    ) -> int:  # pragma: no cover
        raise NotImplementedError("Should implement max_entry_order_size()")

    @abstractmethod
    def position_size(
        self, symbol: str, direction: Direction
    ) -> int:  # pragma: no cover
        raise NotImplementedError("Should implement position_size()")

    def _create_initial_portfolio(self) -> str:
        """
        Create a new portfolio
        Parameters
        """
        p = Portfolio(currency=self.base_currency)
        self.portfolio = p
        LOG.info(
            "(%s) - portfolio creation: Portfolio created", self.clock.current_time()
        )

    def get_portfolio_total_market_value(self) -> float:
        """
        Returns the current total market value of a Portfolio.
        Parameters
        ----------
        Returns
        -------
        `float`
            The total market value of the portfolio.
        """
        return self.portfolio.total_market_value

    def get_portfolio_total_equity(self) -> float:
        """
        Returns the current total equity of a Portfolio.
        Parameters
        ----------
        Returns
        -------
        `float`
            The total equity of the portfolio.
        """
        return self.portfolio.total_equity

    def get_portfolio_as_dict(self) -> dict:
        """
        Return a particular portfolio as
        a dictionary with Asset symbol strings as keys, with various
        attributes as sub-dictionaries.
        Parameters
        ----------
        Returns
        -------
        `dict{str}`
            The portfolio representation of Assets as a dictionary.
        """
        return self.portfolio.portfolio_to_dict()

    def get_portfolio_cash_balance(self) -> float:
        """
        Retrieve the cash balance of a sub-portfolio, if
        it exists. Otherwise raise a ValueError.
        Parameters
        ----------
        Returns
        -------
        `float`
            The cash balance of the portfolio.
        """
        return self.portfolio.cash

    def subscribe_funds_to_portfolio(self, amount: float) -> None:
        """
        Subscribe funds to a particular sub-portfolio, assuming
        it exists and the cash amount is positive. Otherwise raise
        a ValueError.
        Parameters
        ----------
        amount : `float`
            The amount of cash to subscribe to the portfolio.
        """
        if amount < 0:
            raise ValueError(
                "Cannot add negative amount: " "%s to a portfolio account." % amount
            )
        if amount > self.cash_balances[self.base_currency]:
            raise ValueError(
                "Not enough cash in the broker master account to "
                "fund portfolio. %s subscription amount exceeds "
                "current broker account cash balance of %s."
                % (amount, self.cash_balances[self.base_currency])
            )
        self.portfolio.subscribe_funds(amount)
        self.cash_balances[self.base_currency] -= amount
        LOG.info(
            "(%s) - subscription: %s subscribed to portfolio",
            self.clock.current_time(),
            amount,
        )

    def withdraw_funds_from_portfolio(self, amount: float) -> None:
        """
        Withdraw funds from the portfolio, assuming
        it exists, the cash amount is positive and there is
        sufficient remaining cash in the sub-portfolio to
        withdraw. Otherwise raise a ValueError.
        Parameters
        ----------
        amount : `float`
            The amount of cash to withdraw from the portfolio.
        """
        if amount < 0:
            raise ValueError(
                "Cannot withdraw negative amount: "
                "%s from a portfolio account." % amount
            )
        if amount > self.portfolio.cash:
            raise ValueError(
                "Not enough cash in portfolio to withdraw "
                "into brokerage master account. Withdrawal "
                "amount %s exceeds current portfolio cash "
                "balance of %s." % (amount, self.portfolio.cash)
            )
        self.portfolio.withdraw_funds(amount)
        self.cash_balances[self.base_currency] += amount
        LOG.info("withdrawal: %s withdrawn from portfolio", amount)

    def handle_exit_order(self, order: Order):
        if not isinstance(order, Order) or order.is_entry_order:
            return

        if order.is_exit_order:
            if (
                order.symbol in self.exit_orders
                and order.direction in self.exit_orders[order.symbol]
            ):
                exit_order = self.exit_orders[order.symbol][order.direction]
                if isinstance(exit_order, OcoOrder):
                    exit_order.add_order(order)
                else:
                    oco_order = OcoOrder(orders=[exit_order, order])
                    self.exit_orders[order.symbol][order.direction] = oco_order
            else:
                get_or_create_nested_dict(
                    self.exit_orders, order.symbol, order.direction
                )
                self.exit_orders[order.symbol][order.direction] = order

    def put_all_orders_in_queue(self, order: Order):
        if isinstance(order, MultipleOrder):
            for single_order in order.orders:
                self.put_all_orders_in_queue(single_order)
        else:
            self.handle_exit_order(order)
            self.open_orders.put(order)

    def submit_order(self, order: Order) -> None:
        """
        Submit an order instance, for multiple orders, break them down into simple orders
        Parameters
        ----------
        order : `Order`
            The Order instance to submit.
        """
        self.put_all_orders_in_queue(order)
        if order.status != OrderStatus.SUBMITTED:
            order.submit()
        if isinstance(order, SequentialOrder):
            for single_order in order.orders:
                self.handle_exit_order(single_order)
            LOG.info("Sequential order submitted")
        elif isinstance(order, MultipleOrder):
            LOG.info("Multiple order submitted")
        else:
            LOG.info("Submitted order: %s, qty: %s", order.symbol, order.size)

    def max_entry_order_size(
        self, symbol: str, direction: Direction, cash: float = None
    ) -> int:
        if cash is None:
            cash = self.portfolio.cash
        return cash // self.current_price(symbol)

    def position_size(self, symbol: str, direction: Direction) -> int:
        return self.portfolio.pos_handler.position_size(symbol, direction)

    @abstractmethod
    def execute_order(self, order: Order) -> None:  # pragma: no cover
        raise NotImplementedError("Should implement execute_order()")

    def execute_open_orders(self) -> None:
        # Try to execute orders
        orders = np.empty(shape=self.open_orders.qsize(), dtype=Order)
        index = 0
        while not self.open_orders.empty():
            orders[index] = self.open_orders.get()
            index += 1

        for order in orders:
            now = self.clock.current_time(symbol=order.symbol)
            if order.in_force(now) and order.status == OrderStatus.SUBMITTED:
                self.execute_order(order)
            else:
                LOG.info(
                    "Order with order id (%s) either expired or has been canceled",
                    order.order_id,
                )

    def current_price(self, symbol: str):
        return self.last_prices[symbol]

    def update_price(self, candle: Candle):
        self.last_prices[candle.symbol] = candle.close
        self.portfolio.update_market_value_of_symbol(
            candle.symbol, candle.close, candle.timestamp
        )

    def synchronize(self):  # pragma: no cover
        pass
