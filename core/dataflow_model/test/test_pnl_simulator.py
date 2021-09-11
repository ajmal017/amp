import logging
import os

import numpy as np
import pandas as pd

import core.dataflow_model.pnl_simulator as pnlsim
import helpers.dbg as dbg
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPnlSimulator1(hut.TestCase):

    def test_instantaneous_no_cost1(self) -> None:
        act = []
        # Generate some random data.
        df = pnlsim.compute_data(21)
        act.append("df=\n%s" % hut.convert_df_to_string(df))
        mode = "instantaneous"
        df_5mins = pnlsim.resample_data(df, mode)
        act.append("df_5mins=\n%s" % hut.convert_df_to_string(df))
        # Compute pnl using simulation.
        w0 = 100.0
        final_w, tot_ret, df_5mins = pnlsim.compute_pnl_for_instantaneous_no_cost_case(
            w0, df, df_5mins
        )
        act.append("df_5mins=\n%s" % hut.convert_df_to_string(df))
        # Compute pnl using lags.
        df_5mins["pnl"] = df_5mins["preds"] * df_5mins["ret_0"].shift(-2)
        tot_ret2 = (1 + df_5mins["pnl"]).prod() - 1
        act.append("df_5mins=\n%s" % hut.convert_df_to_string(df))
        #
        act = "\n".join(act)
        self.check_string(act)
        # Check that the results are the same.
        #np.testing.assert_almost_equal(tot_ret, tot_ret2)


