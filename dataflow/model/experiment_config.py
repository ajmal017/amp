"""
Import as:

import dataflow.model.experiment_config as dtfmoexcon
"""
import datetime
import logging
from typing import Any, List, Optional, Tuple

import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# #############################################################################


def parse_universe_str(universe_str: str) -> Tuple[str, Optional[int]]:
    """
    Parse a string representing an universe

    E.g., "kibot_v1_0-top100", "kibot_v2_0-all".
    """
    data = universe_str.split("-")
    hdbg.dassert_eq(len(data), 2)
    universe_version, top_n = data
    if top_n == "all":
        top_n = None
    else:
        prefix = "top"
        hdbg.dassert(top_n.startswith(prefix), "Invalid top_n='%s'", top_n)
        top_n = int(top_n[len(prefix) :])
    return universe_version, top_n


def get_universe_top_n(universe: List[Any], n: Optional[int]) -> List[Any]:
    if n is None:
        # No filtering.
        pass
    else:
        hdbg.dassert_lte(1, n, "Invalid n='%s'", n)
        hdbg.dassert_lte(n, len(universe))
        universe = universe[:n]
    universe = sorted(universe)
    return universe


# #############################################################################


def get_period(period: str) -> Tuple[datetime.date, datetime.date]:
    if period == "2days":
        start_date = datetime.date(2020, 1, 6)
        end_date = datetime.date(2020, 1, 7)
    elif period == "Jan2020":
        # Jan in 2020.
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2020, 2, 1)
    elif period == "2018":
        # 2018.
        start_date = datetime.date(2018, 1, 1)
        end_date = datetime.date(2019, 1, 1)
    elif period == "2009_2019":
        # Entire 2009-2019 period.
        start_date = datetime.date(2009, 1, 1)
        end_date = datetime.date(2019, 1, 1)
    else:
        hdbg.dfatal("Invalid period='%s'" % period)
    _LOG.info("start_date=%s end_date=%s", start_date, end_date)
    hdbg.dassert_lte(start_date, end_date)
    return start_date, end_date


# #############################################################################


def parse_experiment_config(experiment_config: str) -> Tuple[str, str, str]:
    """
    Parse a string representing an experiment in the format:
    ```
    <universe>.<date_period>.<time_interval>
    ```
    E.g., "top100.15T.all"

    Each token can be composed of multiple chunks separated by `-`. E.g.,
    `universe_str = "eg_v1_0-top100"`

    <time_interval> needs to be a valid input to `get_period()`

    :return: universe_str, date_period_str, time_interval_str
    """
    _LOG.info(hprint.to_str("experiment_config"))
    #
    data = experiment_config.split(".")
    hdbg.dassert_eq(len(data), 3)
    universe_str, date_period_str, time_interval_str = data
    #
    _LOG.info(hprint.to_str("universe_str date_period_str time_interval_str"))
    return universe, date_period, time_interval
