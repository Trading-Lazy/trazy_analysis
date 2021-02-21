from abc import ABCMeta, abstractmethod


class FeeModel:
    """
    Abstract class to handle the calculation of brokerage
    commission, fees and taxes.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _calc_commission(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:  # pragma: no cover
        raise NotImplementedError("Should implement _calc_commission()")

    @abstractmethod
    def _calc_tax(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:  # pragma: no cover
        raise NotImplementedError("Should implement _calc_tax()")

    @abstractmethod
    def calc_total_cost(
        self, symbol: str, size: int, consideration: float, broker: "Broker" = None
    ) -> float:
        """
        Calculate the total of any commission and/or tax
        for the trade of size 'consideration'.
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
            The zero-cost total commission and tax.
        """
        commission = self._calc_commission(symbol, size, consideration, broker)
        tax = self._calc_tax(symbol, size, consideration, broker)
        return commission + tax
