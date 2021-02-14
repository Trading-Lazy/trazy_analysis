from datetime import datetime, timezone


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

    def __eq__(self, other) -> bool:
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
        cls, credit: float, balance, timestamp: datetime = datetime.now(timezone.utc)
    ):
        return cls(
            timestamp,
            type="subscription",
            description="SUBSCRIPTION",
            debit=0.0,
            credit=round(credit, 2),
            balance=round(balance, 2),
        )

    @classmethod
    def create_withdrawal(
        cls, debit, balance, timestamp: datetime = datetime.now(timezone.utc)
    ):
        return cls(
            timestamp,
            type="withdrawal",
            description="WITHDRAWAL",
            debit=round(debit, 2),
            credit=0.0,
            balance=round(balance, 2),
        )

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "type": self.type,
            "description": self.description,
            "debit": self.debit,
            "credit": self.credit,
            "balance": self.balance,
        }
