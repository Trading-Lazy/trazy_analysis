from broker.fee_model import FeeModel


class PercentFeeModel(FeeModel):
    """
    A FeeModel subclass that produces a percentage cost
    for tax and commission.
    Parameters
    ----------
    commission_pct : `float`, optional
        The percentage commission applied to the consideration.
        0-100% is in the range [0.0, 1.0]. Hence, e.g. 0.1% is 0.001
    tax_pct : `float`, optional
        The percentage tax applied to the consideration.
        0-100% is in the range [0.0, 1.0]. Hence, e.g. 0.1% is 0.001
    """

    def __init__(
        self,
        commission_pct: float = 0.0,
        tax_pct: float = 0.0,
    ) -> None:
        super().__init__()
        self.commission_pct = commission_pct
        self.tax_pct = tax_pct

    def _calc_commission(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:
        """
        Returns the percentage commission from the consideration.
        Parameters
        ----------
        symbol : `str`
            The symbol symbol string.
        size : `int`
            The size of symbols (needed for InteractiveBrokers
            style calculations).
        consideration : `float`
            Price times size of the order.
        broker : `Broker`, optional
            An optional Broker reference.
        Returns
        -------
        `float`
            The percentage commission.
        """
        return self.commission_pct * abs(consideration)

    def _calc_tax(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:
        """
        Returns the percentage tax from the consideration.
        Parameters
        ----------
        symbol : `str`
            The symbol symbol string.
        size : `int`
            The size of symbols (needed for InteractiveBrokers
            style calculations).
        consideration : `float`
            Price times size of the order.
        broker : `Broker`, optional
            An optional Broker reference.
        Returns
        -------
        `float`
            The percentage tax.
        """
        return self.tax_pct * abs(consideration)
