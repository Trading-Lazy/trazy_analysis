from broker.fee_model import FeeModel


class FixedFeeModel(FeeModel):
    """
    A FeeModel subclass that produces a fixed commission and fixed taxes. This is the default fee model.
    """

    def __init__(
        self,
        fixed_commission: float = 0.0,
        fixed_tax: float = 0.0,
    ) -> None:
        self.fixed_commission = fixed_commission
        self.fixed_tax = fixed_tax

    def _calc_commission(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:
        """
        Returns zero commission.
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
            The zero-cost commission.
        """
        return self.fixed_commission

    def _calc_tax(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:
        """
        Returns zero tax.
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
            The zero-cost tax.
        """
        return self.fixed_tax
