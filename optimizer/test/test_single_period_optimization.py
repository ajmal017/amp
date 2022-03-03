import logging
from typing import Optional

import pandas as pd
import pytest

import core.config as cconfig
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

# TODO(gp): Use `hprintin.color_highlight`.
_WARNING = "\033[33mWARNING\033[0m"

try:
    import cvxpy as cvx

    _HAS_CVXPY = True
    _ = cvx
except ImportError as e:
    print(_WARNING + ": " + str(e))
    _HAS_CVXPY = False

if _HAS_CVXPY:
    import optimizer.single_period_optimization as osipeopt


@pytest.mark.skip(reason="Requires special docker container.")
class Test_SinglePeriodOptimizer1(hunitest.TestCase):
    def test_only_gmv_constraint(self) -> None:
        dict_ = {
            "volatility_penalty": 0.0,
            "dollar_neutrality_penalty": 0.0,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.00,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        actual = Test_SinglePeriodOptimizer1.helper(config, df, restrictions=None)
        expected = r"""
          target_positions  target_notional_trades  target_weights  target_weight_diffs
asset_id
1                 -3.71100             -1003.71100        -0.00124             -0.33457
2               2962.92836              1462.92836         0.98764              0.48764
3                 -3.71100               496.28900        -0.00124              0.16543"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_restrictions(self) -> None:
        dict_ = {
            "volatility_penalty": 0.0,
            "dollar_neutrality_penalty": 0.0,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.00,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        restrictions = pd.DataFrame(
            [[2, True, True, True, True]],
            range(0, 1),
            [
                "asset_id",
                "is_buy_restricted",
                "is_buy_cover_restricted",
                "is_sell_short_restricted",
                "is_sell_long_restricted",
            ],
        )
        actual = Test_SinglePeriodOptimizer1.helper(
            config, df, restrictions=restrictions
        )
        expected = r"""
          target_positions  target_notional_trades  target_weights  target_weight_diffs
asset_id
1               1565.67954               565.67954         0.52189              0.18856
2               1487.92516               -12.07484         0.49598             -0.00402
3                  2.82286               502.82286         0.00094              0.16761"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_mixed_constraints(self) -> None:
        dict_ = {
            "volatility_penalty": 0.75,
            "dollar_neutrality_penalty": 0.1,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.01,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        actual = Test_SinglePeriodOptimizer1.helper(config, df, restrictions=None)
        expected = r"""
          target_positions  target_notional_trades  target_weights  target_weight_diffs
asset_id
1                 -2.44463             -1002.44463        -0.00081             -0.33415
2               1523.65440                23.65440         0.50788              0.00788
3              -1508.57457             -1008.57457        -0.50286             -0.33619"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_short_ban(self) -> None:
        dict_ = {
            "volatility_penalty": 0.75,
            "dollar_neutrality_penalty": 0.1,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.01,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        restrictions = pd.DataFrame(
            [[3, False, False, True, False]],
            range(0, 1),
            [
                "asset_id",
                "is_buy_restricted",
                "is_buy_cover_restricted",
                "is_sell_short_restricted",
                "is_sell_long_restricted",
            ],
        )
        actual = Test_SinglePeriodOptimizer1.helper(
            config, df, restrictions=restrictions
        )
        expected = r"""
          target_positions  target_notional_trades  target_weights  target_weight_diffs
asset_id
1               -967.88563             -1967.88563        -0.32263             -0.65596
2               1506.87983                 6.87983         0.50229              0.00229
3               -529.55038               -29.55038        -0.17652             -0.00985"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @staticmethod
    def get_prediction_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [[1, 1000, 0.05, 0.05], [2, 1500, 0.09, 0.07], [3, -500, 0.03, 0.08]],
            range(0, 3),
            ["asset_id", "position", "prediction", "volatility"],
        )
        return df

    @staticmethod
    def helper(
        config: cconfig.Config,
        df: pd.DataFrame,
        restrictions: Optional[pd.DataFrame],
    ) -> str:
        spo = osipeopt.SinglePeriodOptimizer(
            config, df, restrictions=restrictions
        )
        actual = spo.optimize()
        precision = 5
        actual_str = hpandas.df_to_str(actual, precision=precision)
        return actual_str
