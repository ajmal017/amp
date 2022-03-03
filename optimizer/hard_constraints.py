"""
Import as:

import optimizer.hard_constraints as oharcons
"""

import logging
from typing import Optional

import cvxpy as cvx
import pandas as pd

import helpers.hdbg as hdbg
import optimizer.base as opbase

_LOG = logging.getLogger(__name__)


# #############################################################################
# Hard constraints.
# #############################################################################


class ConcentrationHardConstraint(opbase.Expression):
    """
    Restrict max weight in an asset.
    """

    def __init__(self, concentration_bound: float) -> None:
        hdbg.dassert_lte(0, concentration_bound)
        self._concentration_bound = concentration_bound

    def get_expr(self, target_weights, target_weight_diffs, gmv) -> opbase.EXPR:
        _ = target_weight_diffs
        _ = gmv
        # TODO(Paul): Volatility-normalize this?
        return cvx.max(cvx.abs(target_weights)) <= self._concentration_bound


class DollarNeutralityHardConstraint(opbase.Expression):
    """
    Require sum of longs weights equals sum of shorts weights.
    """

    def get_expr(self, target_weights, target_weight_diffs, gmv) -> opbase.EXPR:
        _ = target_weight_diffs
        _ = gmv
        return sum(target_weights) == 0


class TargetGmvHardConstraint(opbase.Expression):
    """
    Impose an upper bound on GMV.
    """

    def __init__(
        self,
        target_gmv: float,
        # lower_bound_multiple: float,
        upper_bound_multiple: float,
    ) -> None:
        hdbg.dassert_lte(0, target_gmv)
        # hdbg.dassert_lte(0, lower_bound_multiple)
        # hdbg.dassert_lte(lower_bound_multiple, 1.0)
        hdbg.dassert_lte(1.0, upper_bound_multiple)
        self._target_gmv = target_gmv
        # self._lower_bound_multiple = lower_bound_multiple
        self._upper_bound_multiple = upper_bound_multiple

    def get_expr(self, target_weights, target_weight_diffs, gmv) -> opbase.EXPR:
        _ = target_weight_diffs
        return (
            gmv * cvx.norm(target_weights, 1)
            <= self._target_gmv * self._upper_bound_multiple
        )


class DoNotTradeHardConstraint(opbase.Expression):
    def __init__(self, do_not_trade: pd.Series) -> None:
        self._do_not_trade = do_not_trade

    def get_expr(self, target_weights, target_weight_diffs, gmv) -> opbase.EXPR:
        _ = target_weights
        _ = gmv
        if self._do_not_trade.any():
            return cvx.multiply(target_weight_diffs, self._do_not_trade) == 0


class DoNotBuyHardConstraint(opbase.Expression):
    def __init__(self, do_not_buy: pd.Series) -> None:
        self._do_not_buy = do_not_buy

    def get_expr(
        self, target_weights, target_weight_diffs, gmv
    ) -> Optional[opbase.EXPR]:
        _ = target_weights
        _ = gmv
        if self._do_not_buy.any():
            return cvx.multiply(target_weight_diffs, self._do_not_buy.values) <= 0


class DoNotSellHardConstraint(opbase.Expression):
    def __init__(self, do_not_sell: pd.Series) -> None:
        self._do_not_sell = do_not_sell

    def get_expr(
        self, target_weights, target_weight_diffs, gmv
    ) -> Optional[opbase.EXPR]:
        _ = target_weights
        _ = gmv
        if self._do_not_sell.any():
            return (
                cvx.multiply(target_weight_diffs, self._do_not_sell.values) >= 0
            )
