from decimal import Decimal

import pandas as pd
import pytz

from portfolio.portfolio_event import PortfolioEvent

PE1 = PortfolioEvent(
    timestamp=pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC),
    type="subscription",
    description="SUBSCRIPTION",
    debit=Decimal("0.0"),
    credit=Decimal("2000.0"),
    balance=Decimal("2000.0"),
)
PE2 = PortfolioEvent(
    timestamp=pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC),
    type="subscription",
    description="SUBSCRIPTION",
    debit=Decimal("0.0"),
    credit=Decimal("2000.0"),
    balance=Decimal("2000.0"),
)


def test_eq():
    assert PE1 == PE2


def test_repr():
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    pe1 = PortfolioEvent(
        timestamp=start_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("2000.0"),
        balance=Decimal("2000.0"),
    )
    expected_repr = (
        "PortfolioEvent(timestamp=2017-10-05 08:00:00+00:00, type=subscription, "
        "description=SUBSCRIPTION, debit=0.0, credit=2000.0, balance=2000.0)"
    )
    assert repr(pe1) == expected_repr


def test_to_dict():
    expected_dict = {
        "timestamp": pd.Timestamp("2017-10-05 08:00:00+0000", tz="UTC"),
        "type": "subscription",
        "description": "SUBSCRIPTION",
        "debit": Decimal("0.0"),
        "credit": Decimal("2000.0"),
        "balance": Decimal("2000.0"),
    }
    assert PE1.to_dict() == expected_dict
