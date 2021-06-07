"""
Import as:

import core.config_utils as cfgut
"""

import collections
import importlib
import itertools
import logging
import os
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import pandas as pd

import core.config as cfg
import helpers.dbg as dbg
import helpers.dict as dct
import helpers.pickle_ as hpickle

_LOG = logging.getLogger(__name__)


def validate_configs(configs: List[cfg.Config]) -> None:
    """
    Assert if the list of configs contains duplicates.
    """
    dbg.dassert_container_type(configs, List, cfg.Config)
    dbg.dassert_no_duplicates(
        list(map(str, configs)), "There are duplicate configs in passed list"
    )


def get_config_from_flattened_dict(flattened: Dict[Tuple[str], Any]) -> cfg.Config:
    """
    Build a config from the flattened config representation.

    :param flattened: flattened config like result from `config.flatten()`
    :return: `Config` object initialized from flattened representation
    """
    dbg.dassert_isinstance(flattened, dict)
    dbg.dassert(flattened)
    config = cfg.Config()
    for k, v in flattened.items():
        config[k] = v
    return config


def get_config_from_nested_dict(nested: Dict[str, Any]) -> cfg.Config:
    """
    Build a `Config` from a nested dict.

    :param nested: nested dict, with certain restrictions:
      - only leaf nodes may not be a dict
      - every nonempty dict must only have keys of type `str`
    """
    dbg.dassert_isinstance(nested, dict)
    dbg.dassert(nested)
    iter_ = dct.get_nested_dict_iterator(nested)
    flattened = collections.OrderedDict(iter_)
    return get_config_from_flattened_dict(flattened)


# ################################################################################


def make_hashable(obj: Any) -> collections.abc.Hashable:
    """
    Coerce `obj` to a hashable type if not already hashable.
    """
    if isinstance(obj, collections.abc.Hashable):
        return obj
    if isinstance(obj, collections.abc.Iterable):
        return tuple(map(make_hashable, obj))
    return tuple(obj)


def intersect_configs(configs: Iterable[cfg.Config]) -> cfg.Config:
    """
    Return a config formed by taking the intersection of configs.

    - Key insertion order is not taken into consideration for the purpose of
      calculating the config intersection
    - The key insertion order of the returned config will respect the key
      insertion order of the first config passed in
    """
    # Flatten configs and convert to sets for intersection.
    # We create a list so that we can reference a flattened config later.
    flattened = [c.flatten() for c in configs]
    dbg.dassert(flattened, "Empty iterable `configs` received.")
    # Obtain a reference config.
    # The purpose of this is to ensure that the config intersection respects a key
    # ordering. We also make this copy so as to maintain the original (not
    # necessarily hashable) values.
    reference_config = flattened[0].copy()
    # Make values hashable.
    for flat in flattened:
        for k, v in flat.items():
            flat[k] = make_hashable(v)
    sets = [set(c.items()) for c in flattened]
    intersection_of_flattened = set.intersection(*sets)
    # Create intersection.
    # Rely on the fact that Config keys are of type `str`.
    intersection = cfg.Config()
    for k, v in reference_config.items():
        if (k, make_hashable(v)) in intersection_of_flattened:
            intersection[k] = v
    return intersection


def subtract_config(minuend: cfg.Config, subtrahend: cfg.Config) -> cfg.Config:
    """
    Return a `Config` defined via minuend - subtrahend.

    :return: return a `Config` with (path, val pairs) in `minuend` that are not in
        `subtrahend` (like a set difference). Equivalently, return a `Config`-like
        `minuend` but with the intersection of `minuend` and `subtrahend`
        removed.
    """
    dbg.dassert(minuend)
    flat_m = minuend.flatten()
    flat_s = subtrahend.flatten()
    diff = cfg.Config()
    for k, v in flat_m.items():
        if (k not in flat_s) or (flat_m[k] != flat_s[k]):
            diff[k] = v
    return diff


def diff_configs(configs: Iterable[cfg.Config]) -> List[cfg.Config]:
    """
    Diff `Config`s with respect to their common intersection.

    :return: for each config `config` in `configs`, return a new `Config` consisting
        of the part of `config` not in the intersection of the configs
    """
    # Convert the configs to a list for convenience.
    configs = list(configs)
    # Find the intersection of all the configs.
    intersection = intersect_configs(configs)
    # For each config, compute the diff between the config and the intersection.
    config_diffs = []
    for config in configs:
        config_diff = subtract_config(config, intersection)
        config_diffs.append(config_diff)
    dbg.dassert_eq(len(config_diffs), len(configs))
    return config_diffs


# # #############################################################################


def convert_to_series(config: cfg.Config) -> pd.Series:
    """
    Convert a config into a flattened series representation.

    - This is lossy but useful for comparing multiple configs
    - `str` tuple paths are joined on "."
    - Empty leaf configs are converted to an empty tuple
    """
    dbg.dassert_isinstance(config, cfg.Config)
    dbg.dassert(config, msg="`config` is empty")
    flat = config.flatten()
    keys: List[str] = []
    vals: List[tuple] = []
    for k, v in flat.items():
        key = ".".join(k)
        keys.append(key)
        if isinstance(v, cfg.Config):
            vals.append(tuple())
        else:
            vals.append(v)
    dbg.dassert_no_duplicates(keys)
    srs = pd.Series(index=keys, data=vals)
    return srs


def convert_to_dataframe(configs: Iterable[cfg.Config]) -> pd.DataFrame:
    """
    Convert multiple configs into flattened dataframe representation.

    E.g., to highlight config differences in a dataframe, for an iterable
    `configs`, do
        ```
        diffs = diff_configs(configs)
        df = convert_to_dataframe(diffs)
        ```
    """
    dbg.dassert_isinstance(configs, Iterable)
    srs = list(map(convert_to_series, configs))
    dbg.dassert(srs)
    df = pd.concat(srs, axis=1).T
    return df


# # #############################################################################
# # Utilities
# # #############################################################################
#
#
# # TODO(*): Deprecate.
# def _flatten_config(config: cfg.Config) -> Dict[str, collections.abc.Hashable]:
#     """
#     Flatten configs, join tuples of strings with "." and make vals hashable.
#
#     Someday you may realize that you want to use "." in the strings of
#     your keys. That likely won't be a very fun day.
#     """
#     flattened = config.flatten()
#     normalized = {}
#     for k, v in flattened.items():
#         val = cfg.make_hashable(v)
#         normalized[".".join(k)] = val
#     return normalized
#
#
# # TODO(*): Deprecate.
# def _flatten_configs(configs: Iterable[cfg.Config]) -> List[Dict[str, Any]]:
#     """
#     Flatten configs, squash the str keys, and make vals hashable.
#
#     :param configs: configs
#     :return: flattened config dicts
#     """
#     return list(map(_flatten_config, configs))
#
#
# # TODO(*): Are the values of this ever used anywhere?
# # TODO(*): Try to deprecate. If needed, compose with `cfg.diff_configs()`.
# # It's not used but unit tested
# def get_config_difference(configs: List[cfg.Config]) -> Dict[str, List[Any]]:
#     """
#     Find parameters in configs that are different and provide the varying
#     values.
#
#     :param configs: A list of configs.
#     :return: A dictionary of varying params and lists of their values.
#     """
#     # Flatten configs into dicts.
#     flattened_configs = _flatten_configs(configs)
#     # Convert dicts into sets of items for comparison.
#     flattened_configs = [set(config.items()) for config in flattened_configs]
#     # Build a dictionary of common config values.
#     union = set.union(*flattened_configs)
#     intersection = set.intersection(*flattened_configs)
#     config_varying_params = union - intersection
#     # Compute params that vary among different configs.
#     config_varying_params = dict(config_varying_params).keys()
#     # Remove `meta` params that always vary.
#     # TODO(*): Where do these come from?
#     redundant_params = ["meta.id", "meta.experiment_result_dir"]
#     config_varying_params = [
#         param for param in config_varying_params if param not in redundant_params
#     ]
#     # Build the difference of configs by considering the parts that vary.
#     config_difference = dict()
#     for param in config_varying_params:
#         param_values = []
#         for flattened_config in flattened_configs:
#             try:
#                 param_values.append(dict(flattened_config)[param])
#             except KeyError:
#                 param_values.append(None)
#         config_difference[param] = param_values
#     return config_difference


# # TODO(*): Deprecate. Switch to `cfg.convert_to_dataframe()`.
# # > jackpy get_configs_dataframe
# # amp/core/test/test_config_builders.py:275:    `cfgb.get_configs_dataframe` using `pd.DataFrame.equals()`
# # amp/core/test/test_config_builders.py:286:        actual_result = cfgb.get_configs_dataframe([config_1, config_2])
# # amp/core/test/test_config_builders.py:309:        actual_result = cfgb.get_configs_dataframe(
# # amp/core/test/test_config_builders.py:326:        actual_result = cfgb.get_configs_dataframe(
# # amp/core/config_builders.py:233:def get_configs_dataframe(
# def get_configs_dataframe(
#         configs: List[cfg.Config],
#         params_subset: Optional[Union[str, List[str]]] = None,
# ) -> pd.DataFrame:
#     """
#     Convert the configs into a df with full nested names.
#
#     The column names should correspond to `subconfig1.subconfig2.parameter`
#     format, e.g.: `build_targets.target_asset`.
#
#     :param configs: Configs used to run experiments. TODO(*): What experiments?
#     :param params_subset: Parameters to include as table columns.
#     :return: Table of configs.
#     """
#     # Convert configs to flattened dicts.
#     flattened_configs = _flatten_configs(configs)
#     # Convert dicts to pd.Series and create a df.
#     config_df = map(pd.Series, flattened_configs)
#     config_df = pd.concat(config_df, axis=1).T
#     # Process the config_df by keeping only a subset of keys.
#     if params_subset is not None:
#         if params_subset == "difference":
#             config_difference = get_config_difference(configs)
#             params_subset = list(config_difference.keys())
#         # Filter config_df for the desired columns.
#         dbg.dassert_is_subset(params_subset, config_df.columns)
#         config_df = config_df[params_subset]
#     return config_df
#
#
