import copy
import os
from datetime import datetime, timezone

import pandas as pd

import settings
from common.constants import DATE_FORMAT
from common.helper import get_or_create_nested_dict
from logger import logger
from models.asset import Asset
from models.enums import Action
from portfolio.portfolio_event import PortfolioEvent, TransactionEvent
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
    starting_cash : float, optional
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
        starting_cash: float = 0.0,
        currency: str = "USD",
        portfolio_id: str = None,
        name: str = None,
        timestamp: datetime = datetime.now(timezone.utc),
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
        self, timestamp: datetime = datetime.now(timezone.utc)
    ) -> None:
        """
        Initialise the portfolio with a (default) currency Cash Symbol
        with size equal to 'starting_cash'.
        """
        self.cash = copy.copy(self.starting_cash)

        if self.starting_cash > 0.0:
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
    def total_market_value(self) -> float:
        """
        Obtain the total market value of the portfolio excluding cash.
        """
        return self.pos_handler.total_market_value()

    @property
    def total_equity(self) -> float:
        """
        Obtain the total market value of the portfolio including cash.
        """
        return self.total_market_value + self.cash

    @property
    def total_unrealised_pnl(self) -> float:
        """
        Calculate the sum of all the positions' unrealised P&Ls.
        """
        return self.pos_handler.total_unrealised_pnl()

    @property
    def total_realised_pnl(self) -> float:
        """
        Calculate the sum of all the positions' realised P&Ls.
        """
        return self.pos_handler.total_realised_pnl()

    @property
    def total_pnl(self) -> float:
        """
        Calculate the sum of all the positions' total P&Ls.
        """
        return self.pos_handler.total_pnl()

    def subscribe_funds(
        self, amount: float, timestamp: datetime = datetime.now(timezone.utc)
    ) -> None:
        """
        Credit funds to the portfolio.
        """

        if amount < 0.0:
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
        self, amount: float, timestamp: datetime = datetime.now(timezone.utc)
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
        action = txn.action.name
        direction = txn.direction.name
        description = "%s %s %s %s %s %s" % (
            action,
            direction,
            txn.size,
            txn.asset.key().upper(),
            txn.price,
            datetime.strftime(txn.timestamp, "%d/%m/%Y"),
        )
        if txn.action == Action.BUY:
            pe = TransactionEvent(
                timestamp=txn.timestamp,
                description=description,
                debit=txn_total_cost,
                credit=0.0,
                balance=self.cash,
                direction=direction,
            )
            self.logger.info(
                '(%s) Symbol "%s" %s %s in portfolio "%s" '
                "- Debit: %s, Balance: %s"
                % (
                    txn.timestamp.strftime(DATE_FORMAT),
                    txn.asset,
                    txn.action.name,
                    direction,
                    self.portfolio_id,
                    txn_total_cost,
                    self.cash,
                )
            )
        else:
            pe = TransactionEvent(
                timestamp=txn.timestamp,
                description=description,
                debit=0.0,
                credit=-1.0 * round(txn_total_cost, 2),
                balance=round(self.cash, 2),
                direction=direction,
            )
            self.logger.info(
                '(%s) Symbol "%s" %s %s in portfolio "%s" '
                "- Credit: %s, Balance: %s"
                % (
                    txn.timestamp.strftime(DATE_FORMAT),
                    txn.asset,
                    txn.action.name,
                    direction,
                    self.portfolio_id,
                    -1.0 * txn_total_cost,
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
        for asset, values in self.pos_handler.positions.items():
            get_or_create_nested_dict(holdings, asset.key())
            for direction, pos in values.items():
                holdings[asset.key()][direction] = {
                    "size": pos.net_size,
                    "market_value": pos.market_value,
                    "unrealised_pnl": pos.unrealised_pnl,
                    "realised_pnl": pos.realised_pnl,
                    "total_pnl": pos.total_pnl,
                }
        return holdings

    def update_market_value_of_symbol(
        self, asset: Asset, current_price: float, timestamp=datetime.now(timezone.utc)
    ) -> None:
        """
        Update the market value of the asset to the current
        trade price.
        """
        if asset not in self.pos_handler.positions:
            return
        else:
            if current_price < 0.0:
                raise ValueError(
                    "Current trade price of %s is negative for "
                    "asset %s. Cannot update position." % (current_price, asset)
                )

            for direction in self.pos_handler.positions[asset]:
                self.pos_handler.positions[asset][direction].update_price(
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
