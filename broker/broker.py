from abc import ABCMeta, abstractmethod
from decimal import Decimal
from typing import Union, List

import pandas as pd

from broker.common import DEFAULT_PORTFOLIO_ID, DEFAULT_PORTFOLIO_NAME
from models.enums import Direction
from models.order import Order
from portfolio.portfolio import Portfolio


class Broker:
    """
    This abstract class provides an interface to a
    generic broker entity. Both simulated and live brokers
    will be derived from this ABC. This ensures that trading
    algorithm specific logic is completely identical for both
    simulated and live environments.
    The Broker has an associated master denominated currency
    through which all subscriptions and withdrawals will occur.
    The Broker entity can support multiple sub-portfolios, each
    with their own separate handling of PnL. The individual PnLs
    from each sub-portfolio can be aggregated to generate an
    account-wide PnL.
    The Broker can execute orders. It contains a queue of
    open orders, needed for handling closed market situations.
    The Broker also supports individual history events for each
    sub-portfolio, which can be aggregated, along with the
    account history, to produce a full trading history for the
    account.
    """

    __metaclass__ = ABCMeta

    def has_opened_position(self, symbol: str, direction: Direction) -> bool:
        raise NotImplementedError("Should implement has_opened_position()")

    @abstractmethod
    def subscribe_funds_to_account(self, amount: Decimal) -> None:
        raise NotImplementedError("Should implement subscribe_funds_to_account()")

    @abstractmethod
    def withdraw_funds_from_account(self, amount: Decimal) -> None:
        raise NotImplementedError("Should implement withdraw_funds_from_account()")

    @abstractmethod
    def get_account_cash_balance(self, currency: str = None) -> Union[dict, Decimal]:
        raise NotImplementedError("Should implement get_account_cash_balance()")

    @abstractmethod
    def get_account_total_non_cash_equity(self):
        raise NotImplementedError(
            "Should implement get_account_total_non_cash_equity()"
        )

    @abstractmethod
    def get_account_total_equity(self) -> dict:
        raise NotImplementedError("Should implement get_account_total_equity()")

    @abstractmethod
    def create_portfolio(
        self,
        portfolio_id: str = DEFAULT_PORTFOLIO_ID,
        name: str = DEFAULT_PORTFOLIO_NAME,
    ) -> None:
        raise NotImplementedError("Should implement create_portfolio()")

    @abstractmethod
    def list_all_portfolios(self) -> List[Portfolio]:
        raise NotImplementedError("Should implement list_all_portfolios()")

    @abstractmethod
    def subscribe_funds_to_portfolio(self, amount: Decimal, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> None:
        raise NotImplementedError("Should implement subscribe_funds_to_portfolio()")

    @abstractmethod
    def withdraw_funds_from_portfolio(self, amount: Decimal, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> None:
        raise NotImplementedError("Should implement withdraw_funds_from_portfolio()")

    @abstractmethod
    def get_portfolio_cash_balance(self, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Decimal:
        raise NotImplementedError("Should implement get_portfolio_cash_balance()")

    @abstractmethod
    def get_portfolio_total_non_cash_equity(self, portfolio_id: str = DEFAULT_PORTFOLIO_ID):
        raise NotImplementedError(
            "Should implement get_portfolio_total_non_cash_equity()"
        )

    @abstractmethod
    def get_portfolio_total_equity(self, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Decimal:
        raise NotImplementedError("Should implement get_portfolio_total_equity()")

    @abstractmethod
    def get_portfolio_as_dict(self, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> dict:
        raise NotImplementedError("Should implement get_portfolio_as_dict()")

    @abstractmethod
    def submit_order(self, order: Order, portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> None:
        raise NotImplementedError("Should implement submit_order()")

    @abstractmethod
    def update(self, timestamp: pd.Timestamp) -> None:
        raise NotImplementedError("Should implement update()")
