import logging

import numpy as np
import pandas as pd

import core.dataflow_model.pnl_simulator as pnlsim
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPnlSimulator1(hut.TestCase):

    def test_instantaneous_no_cost1(self) -> None:
        num_samples = 5 * 3 + 1
        seed = 42
        # Generate some random data.
        df = pnlsim.compute_data(num_samples, seed=seed)
        mode = "instantaneous"
        df_5mins = pnlsim.resample_data(df, mode)
        # Execute.
        self._run(df, df_5mins)

    def test_instantaneous_no_cost2(self) -> None:
        num_samples = 5 * 10 + 1
        seed = 43
        # Generate some random data.
        df = pnlsim.compute_data(num_samples, seed=seed)
        mode = "instantaneous"
        df_5mins = pnlsim.resample_data(df, mode)
        # Execute.
        self._run(df, df_5mins)

    def test_instantaneous_no_cost3(self) -> None:
        num_samples = 5 * 20 + 1
        seed = 44
        # Generate some random data.
        df = pnlsim.compute_data(num_samples, seed=seed)
        mode = "instantaneous"
        df_5mins = pnlsim.resample_data(df, mode)
        # Execute.
        self._run(df, df_5mins)

    def test1(self):
        df_5mins = pnlsim.get_example_data1()
        df = df_5mins
        # Execute.
        self._run(df, df_5mins)

    def _run(self, df: pd.DataFrame, df_5mins: pd.DataFrame) -> None:
        act = []
        act.append("df=\n%s" % hut.convert_df_to_string(df, index=True))
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using simulation.
        w0 = 1000.0
        (
            final_w,
            tot_ret,
            df_5mins,
        ) = pnlsim.compute_pnl_level1(w0, df, df_5mins)
        act.append("# tot_ret=%s" % tot_ret)
        act.append(
            "After pnl simulation: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using lags.
        tot_ret_lag, df_5mins = pnlsim.compute_lag_pnl(df_5mins)
        act.append(
            "After pnl lag computation: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        act.append("# tot_ret_lag=%s" % tot_ret_lag)
        #
        act = "\n".join(act)
        self.check_string(act)
        # Check that all the realized PnL are the same.
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"].replace(np.nan, 0), df_5mins["pnl.sim1"].replace(np.nan, 0))
        # Check that the results are the same.
        np.testing.assert_almost_equal(tot_ret, tot_ret_lag)


#class TestPnlSimulator1(hut.TestCase):
