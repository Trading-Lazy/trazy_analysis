from datetime import datetime

from trazy_analysis.portfolio.portfolio_event import PortfolioEvent

PE1 = PortfolioEvent(
    timestamp=datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    type="subscription",
    description="SUBSCRIPTION",
    debit=0.0,
    credit=2000.0,
    balance=2000.0,
)
PE2 = PortfolioEvent(
    timestamp=datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    type="subscription",
    description="SUBSCRIPTION",
    debit=0.0,
    credit=2000.0,
    balance=2000.0,
)


def test_eq():
    assert PE1 == PE2


def test_repr():
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    pe1 = PortfolioEvent(
        timestamp=start_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=2000.0,
        balance=2000.0,
    )
    expected_repr = (
        "PortfolioEvent(timestamp=2017-10-05 08:00:00+00:00, type=subscription, "
        "description=SUBSCRIPTION, debit=0.0, credit=2000.0, balance=2000.0)"
    )
    assert repr(pe1) == expected_repr


def test_to_dict():
    expected_dict = {
        "timestamp": datetime.strptime(
            "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
        ),
        "type": "subscription",
        "description": "SUBSCRIPTION",
        "debit": float("0.0"),
        "credit": float("2000.0"),
        "balance": float("2000.0"),
    }
    assert PE1.to_dict() == expected_dict
