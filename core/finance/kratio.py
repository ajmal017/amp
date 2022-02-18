"""
Import as:

import core.finance.kratio as cfinkrat
"""
import logging
from typing import cast

import numpy as np
import pandas as pd
import statsmodels.api as sm

import helpers.hdataframe as hdatafr
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


# TODO(Paul): Move to `statistics` module.
def compute_kratio(pnl: pd.Series) -> float:
    """
    Calculate K-Ratio of a time series.

    :param pnl: time series of log returns
    :return: K-Ratio
    """
    hdbg.dassert_isinstance(pnl, pd.Series)
    # TODO(Paul): Remove the implicit resampling.
    # pnl = maybe_resample(pnl)
    pnl = hdatafr.apply_nan_mode(pnl, mode="fill_with_zero")
    cum_rets = pnl.cumsum()
    # Fit the best line to the daily rets.
    x = range(len(cum_rets))
    x = sm.add_constant(x)
    reg = sm.OLS(cum_rets, x)
    model = reg.fit()
    # Compute k-ratio as slope / std err of slope.
    kratio = model.params[1] / model.bse[1]
    # Adjust k-ratio by the number of observations and points per year.
    ppy = hdatafr.infer_sampling_points_per_year(pnl)
    kratio = kratio * np.sqrt(ppy) / len(pnl)
    kratio = cast(float, kratio)
    return kratio
