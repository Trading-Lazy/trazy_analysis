import os
from collections import deque
from typing import Dict, List, Union

import trazy_analysis.settings
from trazy_analysis.broker.broker import Broker
from trazy_analysis.broker.fee_model import FeeModel
from trazy_analysis.broker.fixed_fee_model import FixedFeeModel
from trazy_analysis.common.clock import Clock
from trazy_analysis.logger import logger
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.position.transaction import Transaction

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class SimulatedBroker(Broker):
    """
    A class to handle simulation of a brokerage that
    provides sensible defaults for both currency (USD) and
    transaction cost handling for execution.
    The default commission/fee model is a ZeroFeeModel
    that charges no commission or tax (such as stamp duty).
    Parameters
    ----------
    base_currency : `str`, optional
        The currency denomination of the brokerage account.
    initial_funds : `float`, optional
        An initial amount of cash to add to the broker account.
    fee_models : Dictionary of the `FeeModel` for each asset, optional
        The FeeModel is the commission/fee model used to simulate fees/taxes.
        Defaults to the FixedFeeModel.
    """

    def __init__(
        self,
        clock: Clock,
        events: deque,
        base_currency: str = "EUR",
        supported_currencies: list[str] = ["EUR", "USD"],
        initial_funds: float = 0.0,
        fee_models: FeeModel | dict[Asset, FeeModel] = FixedFeeModel(),
        exchange: str = "universal",
    ) -> None:
        self._check_initial_funds(initial_funds)
        super().__init__(
            clock=clock,
            events=events,
            base_currency=base_currency,
            supported_currencies=supported_currencies,
            fee_models=fee_models,
            exchange=exchange,
        )
        self._set_cash_balances(initial_funds)
        self.open_orders_bars_delay = 1

        LOG.info("Initialising simulated broker...")

    def _check_initial_funds(self, initial_funds: float) -> float:
        """
        Check and set the initial funds for the broker
        master account. Raise ValueError if the
        amount is negative.
        Parameters
        ----------
        initial_funds : `float`
            The initial cash provided to the Broker.
        Returns
        -------
        `float`
            The checked initial funds.
        """
        if initial_funds < 0.0:
            raise ValueError(
                "Could not create the SimulatedBroker entity as the "
                "provided initial funds of '%s' were "
                "negative." % initial_funds
            )

    def _set_cash_balances(self, initial_funds: float) -> dict[str, float]:
        """
        Set the appropriate cash balances in the various
        supported currencies, depending upon the availability
        of initial funds.
        Returns
        -------
        `dict{str: float}`
            The mapping of cash currency strings to
            amount stored by broker in local currency.
        """
        if initial_funds > 0.0:
            self.cash_balances[self.base_currency] = initial_funds

    def has_opened_position(self, asset: Asset, direction: Direction) -> bool:
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        return asset in positions and direction in positions[asset]

    def subscribe_funds_to_account(self, amount: float) -> None:
        """
        Subscribe an amount of cash in the base currency
        to the broker master cash account.
        Parameters
        ----------
        amount : `float`
            The amount of cash to subscribe to the master account.
        """
        if amount < 0.0:
            raise ValueError(
                "Cannot credit negative amount: " "'%s' to the broker account." % amount
            )
        self.cash_balances[self.base_currency] += amount
        LOG.info("Subscription: %s subscribed to broker", amount)

    def withdraw_funds_from_account(self, amount: float) -> None:
        """
        Withdraws an amount of cash in the base currency
        from the broker master cash account, assuming an
        amount equal to or more cash is present. If less
        cash is present, a ValueError is raised.
        Parameters
        ----------
        amount : `float`
            The amount of cash to withdraw from the master account.
        """
        if amount < 0:
            raise ValueError(
                "Cannot debit negative amount: "
                "'%s' from the broker account." % amount
            )
        if amount > self.cash_balances[self.base_currency]:
            raise ValueError(
                "Not enough cash in the broker account to "
                "withdraw. %s withdrawal request exceeds "
                "current broker account cash balance of %s."
                % (amount, self.cash_balances[self.base_currency])
            )
        self.cash_balances[self.base_currency] -= amount
        LOG.info("Withdrawal: %s withdrawn from broker", amount)

    def get_cash_balance(self, currency: str = None) -> dict | float:
        """
        Retrieve the cash dictionary of the account, or
        if a currency is provided, the cash value itself.
        Raises a ValueError if the currency is not
        found within the currency cash dictionary.
        Parameters
        ----------
        currency : `str`, optional
            The currency string to obtain the cash balance for.
        """
        if currency is None:
            return self.cash_balances
        if currency not in self.cash_balances.keys():
            raise ValueError(
                "Currency of type '%s' is not found within the "
                "broker cash master accounts. Could not retrieve "
                "cash balance." % currency
            )
        return self.cash_balances[currency]

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

    def execute_market_order(self, order: Order) -> None:
        """
        For a given portfolio ID string, create a Transaction instance from
        the provided Order and ensure the Portfolio is appropriately updated
        with the new information.
        Parameters
        ----------
        order : `Order`
            The Order instance to create the Transaction for.
        """
        price = self.current_price(order.asset)
        consideration = price * order.size
        total_commission = self.fee_models[order.asset].calc_total_cost(
            order.asset, order.size, consideration, self
        )

        # Check that sufficient cash exists to carry out the
        # order, else scale it down
        est_total_cost = consideration + total_commission
        total_cash = self.portfolio.cash

        if order.is_entry_order and est_total_cost > total_cash:
            LOG.error(
                "%s WARNING: Estimated transaction size of %s exceeds "
                "available cash of %s. Order id %s"
                "with a negative cash balance.",
                self.exchange,
                est_total_cost,
                total_cash,
                order.order_id,
            )
            order.cancel()
            return

        # Create a transaction entity and update the portfolio
        current_timestamp = self.clock.current_time()
        txn = Transaction(
            asset=order.asset,
            size=order.size,
            action=order.action,
            direction=order.direction,
            price=price,
            order_id=order.order_id,
            commission=total_commission,
            timestamp=current_timestamp,
        )
        self.portfolio.transact_symbol(txn)
        LOG.info(
            "(%s) - executed order: %s, qty: %s, price: %s, "
            "consideration: %s, commission: %s, total: %s",
            current_timestamp,
            order.asset,
            order.size,
            price,
            consideration,
            total_commission,
            consideration + total_commission,
        )
        order.complete()
        if (
            order.is_exit_order
            and order.asset in self.exit_orders
            and order.direction in self.exit_orders[order.asset]
        ):
            del self.exit_orders[order.asset][order.direction]

    def execute_limit_order(self, limit_order: Order) -> None:
        price = self.current_price(limit_order.asset)
        if (limit_order.action == Action.BUY and price <= limit_order.limit) or (
            limit_order.action == Action.SELL and price >= limit_order.limit
        ):
            self.execute_market_order(limit_order)
        else:
            self.open_orders.append(limit_order)

    def execute_stop_order(self, stop_order: Order) -> None:
        price = self.current_price(stop_order.asset)
        if (
            stop_order.action == Action.BUY
            and price >= stop_order.stop
            or stop_order.action == Action.SELL
            and price <= stop_order.stop
        ):
            self.execute_market_order(stop_order)
        else:
            self.open_orders.append(stop_order)

    def execute_target_order(self, target_order: Order) -> None:
        price = self.current_price(target_order.asset)
        if (
            target_order.action == Action.BUY
            and price <= target_order.target
            or target_order.action == Action.SELL
            and price >= target_order.target
        ):
            self.execute_market_order(target_order)
        else:
            self.open_orders.append(target_order)

    def execute_trailing_stop_order(
        self,
        trailing_stop_order: Order,
    ) -> None:
        price = self.current_price(trailing_stop_order.asset)
        if trailing_stop_order.action == Action.BUY:
            stop = price + price * trailing_stop_order.stop_pct
            if trailing_stop_order.stop is None:
                trailing_stop_order.stop = stop
            else:
                trailing_stop_order.stop = min(trailing_stop_order.stop, stop)
        else:
            stop = price - price * trailing_stop_order.stop_pct
            if trailing_stop_order.stop is None:
                trailing_stop_order.stop = stop
            else:
                trailing_stop_order.stop = max(trailing_stop_order.stop, stop)

        LOG.info("stop: %s", stop)
        LOG.info("last stop: %s", trailing_stop_order.stop)
        if (
            trailing_stop_order.action == Action.BUY
            and price >= trailing_stop_order.stop
            or trailing_stop_order.action == Action.SELL
            and price <= trailing_stop_order.stop
        ):
            self.execute_market_order(trailing_stop_order)
        else:
            self.open_orders.append(trailing_stop_order)

    def execute_order(self, order: Order) -> None:
        """
        Execute an order according to its type
        Parameters
        ----------
        order : `Order`
            The Order instance to execute.
        """
        match order.order_type:
            case OrderType.LIMIT:
                self.execute_limit_order(order)
            case OrderType.STOP:
                self.execute_stop_order(order)
            case OrderType.TARGET:
                self.execute_target_order(order)
            case OrderType.TRAILING_STOP:
                self.execute_trailing_stop_order(order)
            case OrderType.MARKET:
                self.execute_market_order(order)
