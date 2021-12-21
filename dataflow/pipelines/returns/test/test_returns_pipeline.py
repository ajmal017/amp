import logging
import os
from typing import Any, Dict

import pandas as pd
import pytest

import core.config as cconfig
import dataflow as dtf
import dataflow.core.builders_example as dtfcobuexa
import dataflow.core.dag_adapter as dtfcodaada
import dataflow.core.runners as dtfcorrunn
import dataflow.pipelines.returns.pipeline as dtfpirepip
import dataflow.system.dataflow_sink_nodes as dtfsdtfsino
import dataflow.system.dataflow_source_nodes as dtfsdtfsono
import helpers.printing as hprint
import helpers.unit_test as hunitest


_LOG = logging.getLogger(__name__)


class TestReturnsBuilder(hunitest.TestCase):
    """
    Test the ReturnsBuilder pipeline.
    """

    @pytest.mark.slow
    def test_equities1(self) -> None:
        # TODO(gp): This node doesn't work with the rest of the pipeline since
        #  it seems that it is multi-index.
        # source_node_kwargs = {
        #     "source_node_name": "kibot_equities",
        #     "source_node_kwargs": {
        #         "frequency": "T",
        #         "symbols": ["AAPL"],
        #         "start_date": "2019-01-04 09:00:00",
        #         "end_date": "2019-01-04 16:30:00",
        #     }
        # }
        from im.kibot.data.config import S3_PREFIX

        # TODO(gp): We could use directly a DiskDataSource here.
        ticker = "AAPL"
        config = {
            "func": dtf.load_data_from_disk,
            "func_kwargs": {
                "file_path": os.path.join(
                    S3_PREFIX, "pq/sp_500_1min", ticker + ".pq"
                ),
                "aws_profile": "am",
                "start_date": pd.to_datetime("2010-01-04 9:30:00"),
                "end_date": pd.to_datetime("2010-01-04 16:05:00"),
            },
        }
        self._helper(config)

    @pytest.mark.slow
    def test_futures1(self) -> None:
        source_node_kwargs = {
            "func": dtfsdtfsono.load_kibot_data,
            "func_kwargs": {
                "frequency": "T",
                "contract_type": "continuous",
                "symbol": "ES",
                "start_date": "2010-01-04 09:00:00",
                "end_date": "2010-01-04 16:30:00",
            },
        }
        self._helper(source_node_kwargs)

    def _helper(self, source_node_kwargs: Dict[str, Any]) -> None:
        dag_builder = dtfpirepip.ReturnsPipeline()
        config = dag_builder.get_config_template()
        # Inject the node.
        config["load_prices"] = cconfig.get_config_from_nested_dict(
            {
                "source_node_name": "DataLoader",
                "source_node_kwargs": source_node_kwargs,
            }
        )
        #
        dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)


# #############################################################################


class TestRealtimeDagAdapter1(hunitest.TestCase):

    def testMvnReturnsBuilder1(self) -> None:
        """
        Build a real-time DAG builder from a `MvnReturnsBuilder()`.
        """
        overriding_config = cconfig.Config()
        # Configure a DataSourceNode.
        period = "last_5mins"
        source_node_kwargs = {
            "market_data_interface": "market_data_interface_example",
            "period": period,
            "asset_id_col": "asset_id",
            "multiindex_output": True,
        }
        overriding_config["load_prices"] = {
            "source_node_name": "RealTimeDataSource",
            "source_node_kwargs": source_node_kwargs,
        }
        # Configure a ProcessForecast node.
        order_type = "price@twap"
        overriding_config["process_forecasts"] = {
            "prediction_col": "close",
            "execution_mode": "real_time",
            "process_forecasts_config": {},
        }
        # We could also write the `process_forecasts_config` key directly but we
        # want to show a `Config` created with multiple pieces.
        overriding_config["process_forecasts"]["process_forecasts_config"] = {
            "market_data_interface": "market_data_interface_example",
            "portfolio": "portfolio_example",
            "order_type": order_type,
            "ath_start_time": pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            ).time(),
            "trading_start_time": pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            ).time(),
            "ath_end_time": pd.Timestamp(
                "2000-01-01 16:40:00-05:00", tz="America/New_York"
            ).time(),
            "trading_end_time": pd.Timestamp(
                "2000-01-01 16:40:00-05:00", tz="America/New_York"
            ).time(),
        }
        _LOG.debug("overriding_config=%s\n", overriding_config)
        # Append a ProcessForecastNode node.
        nodes_to_append = []
        stage = "process_forecasts"
        node_ctor = dtfsdtfsino.ProcessForecasts
        nodes_to_append.append((stage, node_ctor))
        # Build the DagAdapter.
        txt = []
        dag_builder = dtfcobuexa.MvnReturnsBuilder()
        #
        dag_adapter = dtfcodaada.DagAdapter(
            dag_builder, overriding_config, nodes_to_append
        )
        txt.append("dag_adapter=")
        txt.append(hprint.indent(str(dag_adapter)))
        # Check.
        txt = "\n".join(txt)
        self.check_string(txt)
