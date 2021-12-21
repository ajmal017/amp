import logging

import pandas as pd

import dataflow.core.builders_example as dtfcobuexa
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import helpers.printing as hprint
import helpers.unit_test as hunitest
import oms.portfolio_example as oporexam

_LOG = logging.getLogger(__name__)


class TestRealtimeDagAdapter1(hunitest.TestCase):
    """
    Test RealTimeDagAdapter building various DAGs.
    """

    def testMvnReturnsBuilder1(self) -> None:
        """
        Build a realtime DAG from `MvnReturnsBuilder()`.
        """
        txt = []
        # Build a DagBuilder.
        dag_builder = dtfcobuexa.MvnReturnsBuilder()
        txt.append("dag_builder=")
        txt.append(hprint.indent(str(dag_builder)))
        # Build a Portfolio.
        event_loop = None
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_simulated_portfolio_example1(event_loop, initial_timestamp)
        # Build a DagAdapter.
        dag_adapter = dtfsrtdaad.RealTimeDagAdapter(dag_builder, portfolio)
        txt.append("dag_adapter=")
        txt.append(hprint.indent(str(dag_adapter)))
        # Check.
        txt = "\n".join(txt)
        self.check_string(txt, purify_text=True)
