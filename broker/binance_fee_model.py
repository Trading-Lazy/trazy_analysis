from broker.percent_fee_model import PercentFeeModel


class BinanceFeeModel(PercentFeeModel):
    """
    A FeeModel subclass that produces a percentage cost
    for tax and commission.
    Parameters
    ----------
    tax_pct : `float`, optional
        The percentage tax applied to the consideration.
        0-100% is in the range [0.0, 1.0]. Hence, e.g. 0.1% is 0.001
    """

    def __init__(
        self,
        tax_pct: float = 0.0,
    ) -> None:
        super().__init__(commission_pct=0.001, tax_pct=tax_pct)

    def calc_max_size_for_cash(self, cash: float, price: float) -> int:
        return cash / (price * (1 + self.commission_pct))
