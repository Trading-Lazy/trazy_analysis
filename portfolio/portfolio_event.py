from datetime import datetime

import pytz

from trazy_analysis.models.enums import Action, Direction


class PortfolioEvent:
    """
    Stores an individual instance of a portfolio event used to create
    an event trail to track all changes to a portfolio through time.
    Parameters
    ----------
    timestamp : `datetime`
        Datetime of the event.
    type : `str`
        The type of portfolio event, e.g. 'subscription', 'withdrawal'.
    description ; `str`
        Human-readable portfolio event type.
    debit : `float`
        A debit to the cash balance of the portfolio.
    credit : `float`
        A credit to the cash balance of the portfolio.
    balance : `float`
        The current cash balance of the portfolio.
    """

    def __init__(
        self,
        timestamp: datetime,
        type: str,
        description: str,
        debit: float,
        credit: float,
        balance: float,
    ) -> None:
        self.timestamp = timestamp
        self.type = type
        self.description = description
        self.debit = debit
        self.credit = credit
        self.balance = balance

    def __eq__(self, other: "PortfolioEvent") -> bool:
        return (
            self.timestamp == other.timestamp
            and self.type == other.type
            and self.description == other.description
            and self.debit == other.debit
            and self.credit == other.credit
            and self.balance == other.balance
        )

    def __repr__(self) -> str:
        return (
            "PortfolioEvent(timestamp=%s, type=%s, description=%s, "
            "debit=%s, credit=%s, balance=%s)"
            % (
                self.timestamp,
                self.type,
                self.description,
                self.debit,
                self.credit,
                self.balance,
            )
        )

    @classmethod
    def create_subscription(
        cls,
        credit: float,
        balance: float,
        timestamp: datetime = datetime.now(pytz.UTC),
    ):
        return SubscriptionEvent(timestamp=timestamp, credit=credit, balance=balance)

    @classmethod
    def create_withdrawal(
        cls,
        debit: float,
        balance: float,
        timestamp: datetime = datetime.now(pytz.UTC),
    ):
        return WithdrawalEvent(timestamp=timestamp, debit=debit, balance=balance)

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "type": self.type,
            "description": self.description,
            "debit": self.debit,
            "credit": self.credit,
            "balance": self.balance,
        }


class SubscriptionEvent(PortfolioEvent):
    def __init__(
        self,
        timestamp: datetime,
        credit: float,
        balance: float,
    ):
        super().__init__(
            timestamp=timestamp,
            type="subscription",
            description="SUBSCRIPTION",
            debit=0.0,
            credit=round(credit, 2),
            balance=round(balance, 2),
        )


class WithdrawalEvent(PortfolioEvent):
    def __init__(
        self,
        timestamp: datetime,
        debit: float,
        balance: float,
    ):
        super().__init__(
            timestamp=timestamp,
            type="withdrawal",
            description="WITHDRAWAL",
            debit=round(debit, 2),
            credit=0.0,
            balance=round(balance, 2),
        )


class TransactionEvent(PortfolioEvent):
    def __init__(
        self,
        timestamp: datetime,
        description: str,
        debit: float,
        credit: float,
        balance: float,
        direction: Direction,
    ):
        super().__init__(
            timestamp=timestamp,
            type="symbol_transaction",
            description=description,
            debit=debit,
            credit=credit,
            balance=balance,
        )
        self.action = Action.BUY if credit == 0 else Action.SELL
        self.direction = direction
