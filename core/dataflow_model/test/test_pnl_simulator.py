import logging

import numpy as np
import pandas as pd

import core.dataflow_model.pnl_simulator as pnlsim
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPnlSimulator1(hut.TestCase):
    """
    Verify that computing PnL using `compute_pnl_level1()`, `compute_lag_pnl()`
    and `compute_pnl_level2()` yield the same results.
    """

    def test1(self):
        """
        Run PnL on an handcrafted example.
        """
        df, df_5mins = pnlsim.get_example_data1()
        # Execute.
        self._run(df, df_5mins)

    def test_random1(self) -> None:
        """
        Run on a random example.
        """
        num_samples = 5 * 3 + 1
        seed = 42
        df, df_5mins = pnlsim.get_example_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def test_random2(self) -> None:
        num_samples = 5 * 10 + 1
        seed = 43
        df, df_5mins = pnlsim.get_example_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def test_random3(self) -> None:
        num_samples = 5 * 20 + 1
        seed = 44
        df, df_5mins = pnlsim.get_example_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def _run(self, df: pd.DataFrame, df_5mins: pd.DataFrame) -> None:
        """
        Compute pnl using level1 simulation and lag-based approach, checking that:
        - the intermediate PnL stream match
        - the total return from the different approaches matches
        """
        act = []
        act.append("df=\n%s" % hut.convert_df_to_string(df, index=True))
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using simulation level 1.
        initial_wealth = 1000.0
        (
            final_w,
            tot_ret,
            df_5mins,
        ) = pnlsim.compute_pnl_level1(initial_wealth, df, df_5mins)
        act.append("# tot_ret=%s" % tot_ret)
        act.append(
            "After pnl simulation level 1: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using lags.
        tot_ret_lag, df_5mins = pnlsim.compute_lag_pnl(df_5mins)
        act.append(
            "After pnl lag computation: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        act.append("# tot_ret_lag=%s" % tot_ret_lag)
        # Compute pnl using simulation level 2.
        config = {
            "price_column": "price",
            "future_snoop_allocation": True,
            "order_type": "price.end",
        }
        df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)
        act.append(
            "After pnl simulation level 2: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        #
        act = "\n".join(act)
        self.check_string(act)
        # Check that all the realized PnL are the same.
        df_5mins["pnl.sim2.shifted(-2)"] = df_5mins["pnl.sim2"].shift(-2)
        for col in ["pnl.lag", "pnl.sim1", "pnl.sim2.shifted(-2)"]:
            df_5mins[col] = df_5mins[col].replace(np.nan, 0)
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim1"])
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim2.shifted(-2)"])
        # Check that the results are the same.
        np.testing.assert_almost_equal(tot_ret, tot_ret_lag)


class TestPnlSimulator2(hut.TestCase):

    def test1(self):
        act = []
        #
        df = df_5mins = pnlsim.get_example_data1()
        #
        config = {
            "price_column": "price",
            "future_snoop_allocation": True,
            "order_type": "price.end",
        }
        df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        #
        act = "\n".join(act)
        self.check_string(act)
        # Check that all the realized PnL are the same.
        df_5mins["pnl.sim2.shifted(-2)"] = df_5mins["pnl.sim2"].shift(-2)
        for col in ["pnl.lag", "pnl.sim1", "pnl.sim2.shifted(-2)"]:
            df_5mins[col] = df_5mins[col].replace(np.nan, 0)
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim1"])
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim2.shifted(-2)"])