from abc import ABCMeta, abstractmethod
from typing import Union, Dict

from trazy_analysis.models.asset import Asset


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

    @abstractmethod
    def calc_max_size_for_cash(self, cash: float, price: float) -> float:
        """
        Calculate the maximum order size that we can afford for
        a cash amount for a security at a given price taking
        into consideration the fee model.
        Parameters
        ----------
        cash : `float`
        price : `float`
        Returns
        -------
        `float`
            The maximum order size
        """
        raise NotImplementedError("Should implement calc_max_size_for_cash()")


class FeeModelManager:
    def __init__(self, fee_models: Union[FeeModel, Dict[Asset, FeeModel]]):
        if issubclass(type(fee_models), FeeModel):
            self.default_fee_model = fee_models
            self.fee_models: Dict[Asset, FeeModel] = {}
        elif isinstance(fee_models, dict):
            self.default_fee_model = None
            self.fee_models: Dict[Asset, FeeModel] = fee_models
        else:
            self.default_fee_model = self.fee_models = None

    def update(self, asset: Asset, fee_model: FeeModel):
        self.fee_models[asset] = fee_model

    def multi_update(self, fee_models: Dict[Asset, FeeModel]):
        self.fee_models.update(fee_models)

    def __getitem__(self, asset: Asset) -> FeeModel:
        if self.fee_models is None or asset not in self.fee_models:
            return self.default_fee_model
        return self.fee_models.get(asset, None)

    def __contains__(self, asset: Asset):
        return asset in self.fee_models
