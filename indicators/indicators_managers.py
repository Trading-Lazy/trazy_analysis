import glob
import importlib
import inspect
import os
import uuid
from pathlib import Path
from typing import Callable, Set, Tuple

import networkx as nx
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.models.enums import IndicatorMode


def get_module_classes(module_name: str, module_path: str) -> Set[type]:
    classes = set()
    for file in glob.glob(str(module_path) + "/*.py"):
        name = os.path.splitext(os.path.basename(file))[0]
        # Ignore __ files
        if name.startswith("__"):
            continue
        module = importlib.import_module(module_name + "." + name, package=None)

        for member in dir(module):
            indicator_class = getattr(module, member)
            if (
                indicator_class
                and inspect.isclass(indicator_class)
                and issubclass(indicator_class, Indicator)
            ):
                classes.add(indicator_class)
    return classes


current_path = Path(__file__)
module_path = current_path.parent
MODULE_NAME = "trazy_analysis.indicators"
indicators_classes = get_module_classes(MODULE_NAME, str(module_path))


class ReactiveIndicators:
    def __init__(self, memoize: bool = True, mode: IndicatorMode = IndicatorMode.LIVE):
        self.memoize = memoize
        self.mode = mode
        self.instances = set()
        self.source_edges = []
        self.input_edges = []

        for class_to_enrich in indicators_classes:
            def indicator_call(
                indicator_class: type,
            ) -> Callable:
                def indicator_call_helper(*args, **kwargs) -> Indicator:
                    if not hasattr(indicator_class, "_instances"):
                        indicator_class._instances = {}
                    if not hasattr(indicator_class, "_max_sizes"):
                        indicator_class._max_sizes = {}
                    key = [id(self)]
                    if self.memoize:
                        key = [id(self), indicator_class.__name__]
                        key.extend(map(id, args))
                        if kwargs:
                            for k, v in kwargs.items():
                                key.append(id(k))
                                key.append(id(v))
                    else:
                        key.append(uuid.uuid4())
                    key = tuple(key)

                    if key not in indicator_class._instances:
                        instance = indicator_class(
                            *args, **kwargs
                        )
                        instance.id = key
                        indicator_class._instances[key] = instance
                        indicator_class._instances[key].setup(self)
                        self.instances.add(indicator_class._instances[key])
                    return indicator_class._instances[key]

                return indicator_call_helper

            setattr(
                self,
                class_to_enrich.__name__,
                indicator_call(class_to_enrich),
            )

    def add_source_edge(self, edge: Tuple["Indicator", "Indicator"]):
        if edge[0] is not None and edge[1] is not None:
            self.source_edges.append(edge)

    def add_input_edge(self, edge: Tuple["Indicator", "Indicator"]):
        if edge[0] is not None and edge[1] is not None:
            self.input_edges.append(edge)

    def remove_edges(self, src: "Indicator", dst: "Indicator"):
        self.source_edges = [
            edge
            for edge in self.source_edges
            if (id(edge[0]) != id(src) or id(edge[1]) != id(dst))
            and (id(edge[1]) != id(src) or id(edge[0]) != id(dst))
        ]

    def plot_instances_graph(self):
        instances_graph = nx.MultiDiGraph()
        instances_graph.add_edges_from(self.source_edges)
        graph_nodes = list(instances_graph.nodes)
        node_from_ind = {i: graph_nodes[i] for i in range(0, len(graph_nodes))}
        topological_levels = list(nx.topological_generations(instances_graph))
        instances_graph.add_edges_from(self.input_edges)

        # set the position according to column (x-coord)
        pos = {}
        n = len(topological_levels)
        for i in range(0, n):
            topological_level = topological_levels[i]
            pos.update({node: (j, i) for j, node in enumerate(topological_level)})

        fig, ax = plt.subplots()
        nodes = nx.draw_networkx_nodes(instances_graph, pos=pos, ax=ax)
        nx.draw_networkx_edges(
            instances_graph,
            edgelist=self.source_edges,
            pos=pos,
            ax=ax,
            edge_color="b",
            connectionstyle="arc3,rad=-0.2",
        )
        nx.draw_networkx_edges(
            instances_graph,
            edgelist=self.input_edges,
            pos=pos,
            ax=ax,
            edge_color="r",
            style="--",
            connectionstyle="arc3,rad=0.2",
        )
        annot = ax.annotate(
            "",
            xy=(0, 0),
            xytext=(20, 20),
            textcoords="offset points",
            bbox=dict(boxstyle="round", fc="w"),
            arrowprops=dict(arrowstyle="->"),
        )
        annot.set_visible(False)

        def update_annot(ind):
            node = node_from_ind[ind["ind"][0]]
            xy = pos[node]
            annot.xy = xy
            node_attr = {
                "node": node,
                "input_window": node.input_window,
                "source_minimal_size": node.source_minimal_size,
                "size": node.size,
                "dtype": node.dtype,
                "memoize": node.memoize,
                "mode": node.indicator_mode.name,
            }
            node_attr.update(instances_graph.nodes[node])
            text = "\n".join(f"{k}: {v}" for k, v in node_attr.items())
            annot.set_text(text)

        def hover(event):
            vis = annot.get_visible()
            if event.inaxes == ax:
                cont, ind = nodes.contains(event)
                if cont:
                    update_annot(ind)
                    annot.set_visible(True)
                    fig.canvas.draw_idle()
                else:
                    if vis:
                        annot.set_visible(False)
                        fig.canvas.draw_idle()

        fig.canvas.mpl_connect("motion_notify_event", hover)
        ax.margins(0.20)
        plt.axis("off")
        plt.ion()
        plt.show()


class Indicators:
    pass


for indicators_class in indicators_classes:

    def compute(compute_class: Indicator):
        def compute_helper(*args, **kwargs):
            data = None
            if args:
                data = args[0]
                args = list(args)[1:]
            elif kwargs:
                data = kwargs.pop("data", None)
            if data is None:
                raise Exception("There should be a data parameter for the indicator.")
            elif not isinstance(data, np.ndarray) and not isinstance(
                data, pd.DataFrame
            ) and not isinstance(data, pd.Series):
                raise Exception("data should be a numpy array, a pandas dataframe or a pandas series")
            if isinstance(data, pd.DataFrame) or isinstance(data, pd.Series):
                data = data.to_numpy()
                shape = list(data.shape)
                while len(shape) > 1 and shape[-1] == 1:
                    data = data.flatten()
                    shape = list(data.shape)

            return compute_class.compute(data, *args, **kwargs)

        return compute_helper

    setattr(Indicators, indicators_class.__name__, compute(indicators_class))
