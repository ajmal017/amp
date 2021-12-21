import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import dataflow.core.builders_example as dtfcobuexa
import dataflow.core.dag_adapter as dtfcodaada
import dataflow.core.nodes.sinks as dtfconosin
import dataflow.core.nodes.sources as dtfconosou
import dataflow.core.runners as dtfcorrunn
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################


def _get_data() -> pd.DataFrame:
    """
    Generate random data.
    """
    num_cols = 2
    seed = 42
    date_range_kwargs = {
        "start": pd.Timestamp("2010-01-01"),
        "end": pd.Timestamp("2010-01-10"),
        "freq": "1B",
    }
    data = hunitest.get_random_df(
        num_cols, seed=seed, date_range_kwargs=date_range_kwargs
    )
    return data


class TestDagAdapter1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Adapt a DAG injecting a data source and appending a `WriteDf` node.
        """
        txt = []
        #
        overriding_config = cconfig.Config()
        # Configure a `DataSourceNode`.
        overriding_config["load_prices"] = {
            "source_node_name": "DataLoader",
            "source_node_kwargs": {
                "func": _get_data,
            },
        }
        # Append a `WriteDf` node.
        nodes_to_append = []
        stage = "write_df"
        node_ctor = dtfconosin.WriteDf
        nodes_to_append.append((stage, node_ctor))
        # Build the `DagAdapter`.
        dag_builder = dtfcobuexa.ArmaReturnsBuilder()
        txt.append("dag_builder=")
        txt.append(hprint.indent(str(dag_builder)))
        #
        dag_adapter = dtfcodaada.DagAdapter(
            dag_builder, overriding_config, nodes_to_append
        )
        txt.append("dag_adapter=")
        txt.append(hprint.indent(str(dag_adapter)))
        # Check.
        txt = "\n".join(txt)
        self.check_string(txt)
