"""
Import as:

import helpers.hpandas as hhpandas
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd

import helpers.dbg as hdbg


def dassert_index_is_datetime(
    df: pd.DataFrame, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Ensure that the dataframe has an index containing datetimes.
    """
    # TODO(gp): Add support also for series.
    hdbg.dassert_isinstance(df, pd.DataFrame, msg, *args)
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex, msg, *args)


def dassert_strictly_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing index.
    """
    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    hdbg.dassert(index.is_monotonic_increasing, msg=msg, *args)
    hdbg.dassert(index.is_unique, msg=msg, *args)


# TODO(gp): Factor out common code related to extracting the index from several
#  pandas data structures.
# TODO(gp): Not sure it's used or useful?
def dassert_monotonic_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing or decreasing index.
    """
    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    hdbg.dassert(cond, msg=msg, *args)
    hdbg.dassert(index.is_unique, msg=msg, *args)


def dassert_valid_remap(to_remap: List[str], remap_dict: Dict[str, str]) -> None:
    """
    Ensure that remapping rows / columns is valid.
    """
    hdbg.dassert_isinstance(to_remap, list)
    hdbg.dassert_isinstance(remap_dict, dict)
    # All the rows / columns to remap, should exist.
    hdbg.dassert_is_subset(remap_dict.keys(), to_remap)
    # The mapping is invertible.
    hdbg.dassert_no_duplicates(remap_dict.keys())
    hdbg.dassert_no_duplicates(remap_dict.values())
    # Rows / columns should not be remapped on existing rows / columns.
    hdbg.dassert_not_intersection(remap_dict.values(), to_remap)
