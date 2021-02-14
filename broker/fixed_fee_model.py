from decimal import Decimal

from broker.fee_model import FeeModel


class FixedFeeModel(FeeModel):
    """
    A FeeModel subclass that produces a fixed commission and fixed taxes. This is the default fee model.
    """

    def __init__(
        self,
        fixed_commission: Decimal = Decimal("0.0"),
        fixed_tax: Decimal = Decimal("0.0"),
    ) -> None:
        self.fixed_commission = fixed_commission
        self.fixed_tax = fixed_tax

    def _calc_commission(
        self, symbol: str, size: int, consideration: Decimal, broker: "Broker" = None
    ) -> Decimal:
        """
        Returns zero commission.
        Parameters
        ----------
        symbol : `str`
            The symbol symbol string.
        size : `int`
            The size of symbols (needed for InteractiveBrokers
            style calculations).
        consideration : `Decimal`
            Price times size of the order.
        broker : `Broker`, optional
            An optional Broker reference.
        Returns
        -------
        `Decimal`
            The zero-cost commission.
        """
        return self.fixed_commission

    def _calc_tax(
        self, symbol: str, size: int, consideration: Decimal, broker: "Broker" = None
    ) -> Decimal:
        """
        Returns zero tax.
        Parameters
        ----------
        symbol : `str`
            The symbol symbol string.
        size : `int`
            The size of symbols (needed for InteractiveBrokers
            style calculations).
        consideration : `Decimal`
            Price times size of the order.
        broker : `Broker`, optional
            An optional Broker reference.
        Returns
        -------
        `Decimal`
            The zero-cost tax.
        """
        return self.fixed_tax
