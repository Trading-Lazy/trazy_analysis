from abc import ABC, abstractmethod
from collections import Callable
from typing import Any, Dict, List

import GPyOpt
import black_box as bb
import numpy as np

# import optuna
# import sherpa
# from bayes_opt import BayesianOptimization
from hyperopt import Trials, fmin, hp, tpe

# from skopt import gp_minimize
# from skopt.space import Categorical, Integer, Real
from trazy_analysis.models.parameter import (
    Choice,
    Continuous,
    Discrete,
    Parameter,
    Static,
)


class Optimizer(ABC):
    @abstractmethod
    def maximize(
        self,
        func: Callable,
        space: dict[str, Parameter],
        nb_iter: int = 54,
        max_evals: int = 1,
    ) -> dict[str, Any]:
        raise NotImplementedError("Should implement maximize()")

    def minimize(
        self,
        func: Callable,
        space: dict[str, Parameter],
        nb_iter: int,
        max_evals: int = 52,
    ):
        def max_func(*args):
            return -func(*args)

        return self.maximize(max_func, space, nb_iter, max_evals)


class BlackBoxOptimizer(Optimizer):
    def maximize(
        self,
        func: Callable,
        space: dict[str, Parameter],
        nb_iter: int = 54,
        max_evals: int = 1,
    ) -> dict[str, Any]:
        domain = []
        map_index_to_param_key = {}
        index = 0
        for param_key, param_value in space.items():
            if not isinstance(param_value, Continuous) and not isinstance(
                param_value, Discrete
            ):
                raise Exception(
                    f"Black box doesn't support {type(param_value)} parameters"
                )
            if isinstance(param_value, Continuous) and param_value.scale == "log":
                raise Exception(
                    "Black box doesn't support log scale for Continuous parameters"
                )
            domain.append(param_value.range)
            map_index_to_param_key[index] = param_key
            index += 1

        global bb_func

        def bb_func(params: list[Any]):
            kwargs = {}
            for index in range(0, len(params)):
                param_key = map_index_to_param_key[index]
                kwargs[param_key] = params[index]
                param_value = space[param_key]
                if isinstance(param_value, Discrete):
                    kwargs[param_key] = int(kwargs[param_key])
            return -func(**kwargs)

        best_params = bb.search_min(
            f=bb_func,  # given function
            domain=domain,  # ranges of each parameter,
            budget=nb_iter,  # total number of function calls available
            batch=max_evals,  # number of calls that will be evaluated in parallel
            resfile="output.txt",
        )

        best_params_dict = {}
        for index in range(0, len(best_params)):
            param_key = map_index_to_param_key[index]
            param_value = space[param_key]
            best_params_dict[param_key] = best_params[index]
            if isinstance(param_value, Discrete):
                best_params_dict[param_key] = int(best_params_dict[param_key])
            index += 1

        best_result = func(**best_params_dict)
        best_params_dict["best_result"] = best_result

        return best_params_dict


# class SkoptOptimizer(Optimizer):
#     def maximize(
#         self,
#         func: Callable,
#         space: dict[str, Parameter],
#         nb_iter: int = 54,
#         max_evals: int = 1,
#     ) -> dict[str, Any]:
#         skopt_space = []
#         map_index_to_param_key = {}
#         index = 0
#         for param_key, param_value in space.items():
#             if (
#                 not isinstance(param_value, Continuous)
#                 and not isinstance(param_value, Discrete)
#                 and not isinstance(param_value, Choice)
#             ):
#                 raise Exception(f"Skopt doesn't support {type(param_value)} parameters")
#
#             if isinstance(param_value, Discrete):
#                 skopt_space.append(
#                     Integer(param_value.range[0], param_value.range[-1], name=param_key)
#                 )
#             elif isinstance(param_value, Continuous):
#                 skopt_space.append(
#                     Real(
#                         low=param_value.range[0],
#                         high=param_value.range[-1],
#                         prior="log-uniform"
#                         if param_value.scale == "log"
#                         else "uniform",
#                         name=param_key,
#                     )
#                 )
#             elif isinstance(param_value, Choice):
#                 skopt_space.append(
#                     Categorical(
#                         categories=param_value.range,
#                         transform="identity",
#                         name=param_key,
#                     )
#                 )
#             map_index_to_param_key[index] = param_key
#             index += 1
#
#         global skopt_func
#
#         def skopt_func(params: list[Any]):
#             kwargs = {}
#             for index in range(0, len(params)):
#                 param_key = map_index_to_param_key[index]
#                 kwargs[param_key] = params[index]
#             return -func(**kwargs)
#
#         res_gp = gp_minimize(
#             skopt_func, skopt_space, n_calls=nb_iter, random_state=0, n_jobs=max_evals
#         )
#
#         best_params = res_gp.x
#
#         best_params_dict = {}
#         for index in range(0, len(best_params)):
#             param_key = map_index_to_param_key[index]
#             param_value = space[param_key]
#             best_params_dict[param_key] = best_params[index]
#             if isinstance(param_value, Discrete):
#                 best_params_dict[param_key] = int(best_params_dict[param_key])
#             index += 1
#
#         best_result = -res_gp.fun
#         best_params_dict["best_result"] = best_result
#
#         return best_params_dict


class HyperOptimizer(Optimizer):
    def maximize(
        self,
        func: Callable,
        space: dict[str, Parameter],
        nb_iter: int = 54,
        max_evals: int = 1,
    ) -> dict[str, Any]:
        hyperopt_space = {}
        for param_key, param_value in space.items():
            if (
                not isinstance(param_value, Continuous)
                and not isinstance(param_value, Discrete)
                and not isinstance(param_value, Choice)
                and not isinstance(param_value, Static)
            ):
                raise Exception(
                    f"Hyperopt doesn't support {type(param_value)} parameters"
                )

            if isinstance(param_value, Discrete):
                hyperopt_space[param_key] = hp.randint(
                    param_key, param_value.range[0], param_value.range[-1]
                )
            elif isinstance(param_value, Continuous):
                hp_space_func = (
                    hp.loguniform if param_value.scale == "log" else hp.uniform
                )
                hyperopt_space[param_key] = hp_space_func(
                    param_key, param_value.range[0], param_value.range[-1]
                )
            elif isinstance(param_value, Choice):
                hyperopt_space[param_key] = hp.choice(param_key, param_value.range)
            elif isinstance(param_value, Static):
                hyperopt_space[param_key] = hp.choice(param_key, param_value.range)

        global hyperopt_func

        def hyperopt_func(params: dict[str, Any]):
            return -func(**params)

        trials = Trials()
        best_params_dict = fmin(
            fn=hyperopt_func,
            space=hyperopt_space,
            algo=tpe.suggest,
            max_evals=nb_iter,
            trials=trials,
        )

        for param_key, param_value in space.items():
            if isinstance(param_value, Choice) or isinstance(param_value, Static):
                best_params_dict[param_key] = param_value.range[
                    best_params_dict[param_key]
                ]

        best_result = func(**best_params_dict)
        best_params_dict["best_result"] = best_result

        return best_params_dict


# class OptunaOptimizer(Optimizer):
#     def maximize(
#         self,
#         func: Callable,
#         space: dict[str, Parameter],
#         nb_iter: int = 54,
#         max_evals: int = 1,
#     ) -> dict[str, Any]:
#         for param_key, param_value in space.items():
#             if (
#                 not isinstance(param_value, Continuous)
#                 and not isinstance(param_value, Discrete)
#                 and not isinstance(param_value, Choice)
#             ):
#                 raise Exception(
#                     f"Optuna doesn't support {type(param_value)} parameters"
#                 )
#
#         global optuna_func
#
#         def optuna_func(trial: optuna.Trial):
#             kwargs = {}
#             for param_key, param_value in space.items():
#                 if isinstance(param_value, Discrete):
#                     kwargs[param_key] = trial.suggest_int(
#                         name=param_key,
#                         low=param_value.range[0],
#                         high=param_value.range[-1],
#                     )
#                 elif isinstance(param_value, Continuous):
#                     log = True if param_value.scale == "log" else False
#                     kwargs[param_key] = trial.suggest_float(
#                         name=param_key,
#                         low=param_value.range[0],
#                         high=param_value.range[-1],
#                         log=log,
#                     )
#                 elif isinstance(param_value, Choice):
#                     kwargs[param_key] = trial.suggest_categorical(
#                         param_key, param_value.range
#                     )
#             return -func(**kwargs)
#
#         study = optuna.create_study()
#         study.optimize(
#             func=optuna_func, n_trials=nb_iter, n_jobs=max_evals, show_progress_bar=True
#         )
#         best_params_dict = study.best_params
#         best_result = study.best_value
#         best_params_dict["best_result"] = -best_result
#
#         return best_params_dict


# class BayesianOptimizer(Optimizer):
#     def maximize(
#         self,
#         func: Callable,
#         space: dict[str, Parameter],
#         nb_iter: int = 54,
#         max_evals: int = 1,
#     ) -> dict[str, Any]:
#         pbounds = {}
#         for param_key, param_value in space.items():
#             if not isinstance(param_value, Continuous) and not isinstance(
#                 param_value, Discrete
#             ):
#                 raise Exception(
#                     f"BayesianOptimization doesn't support {type(param_value)} parameters"
#                 )
#             if isinstance(param_value, Continuous) and param_value.scale == "log":
#                 raise Exception(
#                     "BayesianOptimization doesn't support log scale for Continuous parameters"
#                 )
#             pbounds[param_key] = (param_value.range[0], param_value.range[-1])
#
#         global bayesianopt_func
#
#         def bayesianopt_func(**kwargs):
#             return func(**kwargs)
#
#         optimizer = BayesianOptimization(
#             f=bayesianopt_func,
#             pbounds=pbounds,
#             random_state=0,
#         )
#
#         optimizer.maximize(
#             init_points=1,
#             n_iter=nb_iter - 1,
#         )
#
#         best_params_dict = optimizer.max["params"]
#         for param_key, param_value in space.items():
#             if isinstance(param_value, Discrete):
#                 best_params_dict[param_key] = int(best_params_dict[param_key])
#         best_params_dict["best_result"] = optimizer.max["target"]
#
#         return best_params_dict


# class SherpaOptimizer(Optimizer):
#     def maximize(
#         self,
#         func: Callable,
#         space: dict[str, Parameter],
#         nb_iter: int = 54,
#         max_evals: int = 1,
#     ) -> dict[str, Any]:
#         sherpaopt_space = []
#         for param_key, param_value in space.items():
#             if (
#                 not isinstance(param_value, Continuous)
#                 and not isinstance(param_value, Discrete)
#                 and not isinstance(param_value, Choice)
#                 and not isinstance(param_value, Ordinal)
#             ):
#                 raise Exception(
#                     f"Sherpa doesn't support {type(param_value)} parameters"
#                 )
#
#             if isinstance(param_value, Discrete):
#                 sherpaopt_space.append(
#                     sherpa.Discrete(name=param_key, range=param_value.range)
#                 )
#             elif isinstance(param_value, Continuous):
#                 sherpaopt_space.append(
#                     sherpa.Continuous(
#                         name=param_key, range=param_value.range, scale=param_value.scale
#                     )
#                 )
#             elif isinstance(param_value, Choice):
#                 sherpaopt_space.append(
#                     sherpa.Choice(name=param_key, range=param_value.range)
#                 )
#             elif isinstance(param_value, Ordinal):
#                 sherpaopt_space.append(
#                     sherpa.Ordinal(name=param_key, range=param_value.range)
#                 )
#
#         alg = sherpa.algorithms.GPyOpt(max_num_trials=nb_iter)
#         study = sherpa.Study(
#             parameters=sherpaopt_space, algorithm=alg, lower_is_better=False
#         )
#
#         iteration = 1
#         for trial in study:
#             result = func(**trial.parameters)
#             study.add_observation(trial=trial, iteration=iteration, objective=result)
#             iteration += 1
#
#         best_params = study.get_best_result()
#         best_params_dict = {
#             param_key: param_value
#             for param_key, param_value in best_params.items()
#             if param_key in space
#         }
#         best_params_dict["best_result"] = best_params["Objective"]
#
#         return best_params_dict


class GPyOptimizer(Optimizer):
    def maximize(
        self,
        func: Callable,
        space: dict[str, Parameter],
        nb_iter: int = 54,
        max_evals: int = 1,
    ) -> dict[str, Any]:
        gpyopt_space = []
        for param_key, param_value in space.items():
            if (
                not isinstance(param_value, Continuous)
                and not isinstance(param_value, Discrete)
                and not isinstance(param_value, Choice)
            ):
                raise Exception(
                    f"GPyOpt doesn't support {type(param_value)} parameters"
                )

            if isinstance(param_value, Continuous) and param_value.scale == "log":
                raise Exception(
                    "GPyOpt doesn't support log scale for Continuous parameters"
                )

            if isinstance(param_value, Discrete):
                gpyopt_space.append(
                    {
                        "name": param_key,
                        "type": "discrete",
                        "domain": (param_value.range[0], param_value.range[-1]),
                    }
                )
            elif isinstance(param_value, Continuous):
                gpyopt_space.append(
                    {
                        "name": param_key,
                        "type": "continuous",
                        "domain": (param_value.range[0], param_value.range[-1]),
                    }
                )
            elif isinstance(param_value, Choice):
                gpyopt_space.append(
                    {
                        "name": param_key,
                        "type": "categorical",
                        "domain": param_value.range,
                    }
                )

        global gpyopt_func

        def gpyopt_func(params: np.ndarray):
            return -func(*params[0])

        bayesian_opt = GPyOpt.methods.BayesianOptimization(gpyopt_func, gpyopt_space)
        bayesian_opt.run_optimization(nb_iter)

        index = 0
        best_params_dict = {}
        best_params_list = bayesian_opt.x_opt
        for param_key, param_value in space.items():
            best_params_dict[param_key] = best_params_list[index]
            index += 1
        best_params_dict["best_result"] = -bayesian_opt.fx_opt

        return best_params_dict
