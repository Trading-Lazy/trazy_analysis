import copy
import os
from datetime import datetime
from decimal import Decimal

import pandas as pd

import settings
from common.constants import DATE_FORMAT
from common.helper import get_or_create_nested_dict
from logger import logger
from models.enums import Action
from portfolio.portfolio_event import PortfolioEvent
from position.position_handler import PositionHandler
from position.transaction import Transaction

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Portfolio:
    """
    Represents a portfolio of symbols. It contains a cash
    account with the ability to subscribe and withdraw funds.
    It also contains a list of positions in symbols, encapsulated
    by a PositionHandler instance.
    Parameters
    ----------
    start_timestamp : datetime
        Portfolio creation datetime.
    starting_cash : Decimal, optional
        Starting cash of the portfolio. Defaults to 100,000 USD.
    currency: str, optional
        The portfolio denomination currency.
    portfolio_id: str, optional
        An identifier for the portfolio.
    name: str, optional
        The human-readable name of the portfolio.
    """

    def __init__(
        self,
        starting_cash: Decimal = Decimal("0.0"),
        currency: str = "USD",
        portfolio_id: str = None,
        name: str = None,
        timestamp: pd.Timestamp = pd.Timestamp.now("UTC"),
    ) -> None:
        """
        Initialise the Portfolio object with a PositionHandler,
        an event history, along with cash balance. Make sure
        the portfolio denomination currency is also set.
        """
        self.starting_cash = starting_cash
        self.currency = currency
        self.portfolio_id = portfolio_id
        self.name = name

        self.pos_handler = PositionHandler()
        self.history = []

        self.logger = LOG
        self.logger.info('Portfolio "%s" instance initialised' % (self.portfolio_id,))

        self._initialise_portfolio_with_cash(timestamp)

    def _initialise_portfolio_with_cash(
        self, timestamp: pd.Timestamp = pd.Timestamp.now("UTC")
    ) -> None:
        """
        Initialise the portfolio with a (default) currency Cash Symbol
        with size equal to 'starting_cash'.
        """
        self.cash = copy.copy(self.starting_cash)

        if self.starting_cash > Decimal("0.0"):
            self.history.append(
                PortfolioEvent.create_subscription(
                    self.starting_cash, self.starting_cash, timestamp
                )
            )

        self.logger.info(
            '%s - Funds subscribed to portfolio "%s" '
            "- Credit: %s, Balance: %s"
            % (
                timestamp,
                self.portfolio_id,
                self.starting_cash,
                self.starting_cash,
            )
        )

    @property
    def total_market_value(self) -> Decimal:
        """
        Obtain the total market value of the portfolio excluding cash.
        """
        return self.pos_handler.total_market_value()

    @property
    def total_equity(self) -> Decimal:
        """
        Obtain the total market value of the portfolio including cash.
        """
        return self.total_market_value + self.cash

    @property
    def total_unrealised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' unrealised P&Ls.
        """
        return self.pos_handler.total_unrealised_pnl()

    @property
    def total_realised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' realised P&Ls.
        """
        return self.pos_handler.total_realised_pnl()

    @property
    def total_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' total P&Ls.
        """
        return self.pos_handler.total_pnl()

    def subscribe_funds(
        self, amount: Decimal, timestamp: pd.Timestamp = pd.Timestamp.now("UTC")
    ) -> None:
        """
        Credit funds to the portfolio.
        """

        if amount < Decimal("0.0"):
            raise ValueError(
                "Cannot credit negative amount: " "%s to the portfolio." % amount
            )

        self.cash += amount

        self.history.append(
            PortfolioEvent.create_subscription(amount, self.cash, timestamp)
        )

        self.logger.info(
            '%s - Funds subscribed to portfolio "%s" '
            "- Credit: %s, Balance: %s"
            % (
                timestamp,
                self.portfolio_id,
                amount,
                self.cash,
            )
        )

    def withdraw_funds(
        self, amount: Decimal, timestamp: pd.Timestamp = pd.Timestamp.now("UTC")
    ) -> None:
        """
        Withdraw funds from the portfolio if there is enough
        cash to allow it.
        """
        if amount < 0:
            raise ValueError(
                "Cannot debit negative amount: " "%s from the portfolio." % amount
            )

        if amount > self.cash:
            raise ValueError(
                "Not enough cash in the portfolio to "
                "withdraw. %s withdrawal request exceeds "
                "current portfolio cash balance of %s." % (amount, self.cash)
            )

        self.cash -= amount

        self.history.append(
            PortfolioEvent.create_withdrawal(amount, self.cash, timestamp)
        )

        self.logger.info(
            '%s - Funds withdrawn from portfolio "%s" '
            "- Debit: %s, Balance: %s"
            % (
                timestamp,
                self.portfolio_id,
                amount,
                self.cash,
            )
        )

    def transact_symbol(self, txn: Transaction) -> None:
        """
        Adjusts positions to account for a transaction.
        """
        txn_total_cost = txn.cost_with_commission

        if txn_total_cost > self.cash:
            self.logger.error(
                "WARNING: Not enough cash in the portfolio to "
                "carry out transaction. Transaction cost of %s "
                "exceeds remaining cash of %s. Transaction "
                "will proceed with a negative cash balance."
                % (txn_total_cost, self.cash)
            )
            return

        self.pos_handler.transact_position(txn)

        self.cash -= txn_total_cost

        # Form Portfolio history details
        direction = txn.direction.name
        description = "%s %s %s %s %s" % (
            direction,
            txn.size,
            txn.symbol.upper(),
            txn.price,
            datetime.strftime(txn.timestamp, "%d/%m/%Y"),
        )
        if txn.action == Action.BUY:
            pe = PortfolioEvent(
                timestamp=txn.timestamp,
                type="symbol_transaction",
                description=description,
                debit=txn_total_cost,
                credit=Decimal("0.0"),
                balance=self.cash,
            )
            self.logger.info(
                '(%s) Symbol "%s" %s %s in portfolio "%s" '
                "- Debit: %s, Balance: %s"
                % (
                    txn.timestamp.strftime(DATE_FORMAT),
                    txn.symbol,
                    txn.action.name,
                    direction,
                    self.portfolio_id,
                    txn_total_cost,
                    self.cash,
                )
            )
        else:
            pe = PortfolioEvent(
                timestamp=txn.timestamp,
                type="symbol_transaction",
                description=description,
                debit=Decimal("0.0"),
                credit=-Decimal("1.0") * round(txn_total_cost, 2),
                balance=round(self.cash, 2),
            )
            self.logger.info(
                '(%s) Symbol "%s" %s %s in portfolio "%s" '
                "- Credit: %s, Balance: %s"
                % (
                    txn.timestamp.strftime(DATE_FORMAT),
                    txn.symbol,
                    txn.action.name,
                    direction,
                    self.portfolio_id,
                    -Decimal("1.0") * txn_total_cost,
                    self.cash,
                )
            )
        self.history.append(pe)

    def portfolio_to_dict(self) -> dict:
        """
        Output the portfolio holdings information as a dictionary
        with Symbols as keys and sub-dictionaries as values.
        This excludes cash.
        Returns
        -------
        `dict`
            The portfolio holdings.
        """
        holdings = {}
        for symbol, values in self.pos_handler.positions.items():
            get_or_create_nested_dict(holdings, symbol)
            for direction, pos in values.items():
                holdings[symbol][direction] = {
                    "size": pos.net_size,
                    "market_value": pos.market_value,
                    "unrealised_pnl": pos.unrealised_pnl,
                    "realised_pnl": pos.realised_pnl,
                    "total_pnl": pos.total_pnl,
                }
        return holdings

    def update_market_value_of_symbol(
        self, symbol: str, current_price: Decimal, timestamp=pd.Timestamp.now("UTC")
    ) -> None:
        """
        Update the market value of the symbol to the current
        trade price.
        """
        if symbol not in self.pos_handler.positions:
            return
        else:
            if current_price < Decimal("0.0"):
                raise ValueError(
                    "Current trade price of %s is negative for "
                    "symbol %s. Cannot update position." % (current_price, symbol)
                )

            for direction in self.pos_handler.positions[symbol]:
                self.pos_handler.positions[symbol][direction].update_price(
                    current_price, timestamp
                )

    def history_to_df(self) -> pd.DataFrame:
        """
        Creates a Pandas DataFrame of the Portfolio history.
        """
        records = [pe.to_dict() for pe in self.history]
        return pd.DataFrame.from_records(
            records,
            columns=["timestamp", "type", "description", "debit", "credit", "balance"],
        ).set_index(keys=["timestamp"])

    def __eq__(self, other):
        if not isinstance(other, Portfolio):
            return False
        return self.__dict__ == other.__dict__
