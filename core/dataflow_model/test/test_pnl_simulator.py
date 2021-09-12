import logging

import numpy as np

import core.dataflow_model.pnl_simulator as pnlsim
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPnlSimulator1(hut.TestCase):

    def test_instantaneous_no_cost1(self) -> None:
        num_samples = 5 * 3 + 1
        seed = 42
        self._helper(num_samples, seed)

    def test_instantaneous_no_cost2(self) -> None:
        num_samples = 5 * 10 + 1
        seed = 43
        self._helper(num_samples, seed)

    def test_instantaneous_no_cost3(self) -> None:
        num_samples = 5 * 20 + 1
        seed = 44
        self._helper(num_samples, seed)
    def _helper(self, num_samples: int, seed: int) -> None:
        act = []
        # Generate some random data.
        df = pnlsim.compute_data(num_samples)
        act.append("df=\n%s" % hut.convert_df_to_string(df, index=True))
        mode = "instantaneous"
        df_5mins = pnlsim.resample_data(df, mode)
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using simulation.
        w0 = 100.0
        (
            final_w,
            tot_ret,
            df_5mins,
        ) = pnlsim.compute_pnl_for_instantaneous_no_cost_case(w0, df, df_5mins)
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
        # Check that the results are the same.
        np.testing.assert_almost_equal(tot_ret, tot_ret_lag)

    # Without costs, the pnl is the same as the lag accounting.

    # Without