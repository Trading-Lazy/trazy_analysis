from datetime import datetime

from simulator.simulator import Simulator
from strategy.strategies.DumbLongStrategy import DumbLongStrategy
from strategy.strategies.DumbShortStrategy import DumbShortStrategy


def test_run_simulation():
    sim = Simulator()
    start_str = '2020-05-08 07:00:00'
    start = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    sim.add_candles_data_from_db('ANX.PA', start)
    sim.fund(1000)
    sim.set_commission(0.04)
    sim.add_strategy(DumbLongStrategy())
    sim.add_strategy(DumbShortStrategy())
    sim.run()
