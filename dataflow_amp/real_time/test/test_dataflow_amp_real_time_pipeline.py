import logging
from typing import Callable, Tuple

import pandas as pd

import core.config as cconfig
import core.dataflow.real_time as cdtfretim
import core.dataflow.runners as cdtfrun
import core.dataflow.test.test_real_time as cdtfttrt
import dataflow_amp.real_time.pipeline as dtfamretipip
import helpers.datetime_ as hdatetim
import helpers.hasyncio as hhasynci
import helpers.htypes as hhtypes
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


# TODO(gp): Use simulated time and freeze output.
class TestRealTimeReturnPipeline1(huntes.TestCase):
    """
    This test is similar to `TestRealTimeDagRunner` but using a real DAG
    (`ReturnPipeline`) together with real-time data source node.
    """

    def test1(self) -> None:
        """
        Test `RealTimeReturnPipeline` using synthetic data.
        """
        # Create the pipeline.
        dag_builder = dtfamretipip.RealTimeReturnPipeline()
        config = dag_builder.get_config_template()
        # Inject the real-time node.
        delay_in_secs = 0.0
        # Use a replayed real-time starting at the same time as the data.
        initial_replayed_dt = pd.Timestamp("2010-01-04 09:30:00")
        get_wall_clock_time = lambda: hdatetim.get_current_time("naive_ET")
        data_builder, data_builder_kwargs = self._get_test_data_builder()
        source_node_kwargs = {
            "delay_in_secs": delay_in_secs,
            "initial_replayed_dt": initial_replayed_dt,
            "get_wall_clock_time": get_wall_clock_time,
            "data_builder": data_builder,
            "data_builder_kwargs": data_builder_kwargs,
        }
        config["load_prices"] = cconfig.get_config_from_nested_dict(
            {
                "source_node_name": "ReplayedTimeDataSource",
                "source_node_kwargs": source_node_kwargs,
            }
        )
        # Set up the event loop.
        event_loop = None
        execute_rt_loop_kwargs = (
            cdtfttrt.get_replayed_time_execute_rt_loop_kwargs(event_loop)
        )
        kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
        }
        # Run.
        dag_runner = cdtfrun.RealTimeDagRunner(**kwargs)
        result_bundles = hhasynci.run(dag_runner.predict(), event_loop=event_loop)
        events = dag_runner.events
        # TODO(gp): Add a check for this.
        _LOG.debug("events=\n%s", events)
        _LOG.debug("result_bundles=\n%s", result_bundles)

    @staticmethod
    def _get_test_data_builder() -> Tuple[Callable, hhtypes.Kwargs]:
        """
        Return data between "2010-01-04 09:30:00" and "2010-01-04 09:35:00"
        every second.
        """
        data_builder = cdtfretim.generate_synthetic_data
        data_builder_kwargs = {
            "columns": ["close", "vol"],
            "start_datetime": pd.Timestamp("2010-01-04 09:30:00"),
            "end_datetime": pd.Timestamp("2010-01-05 09:30:00"),
            "freq": "1S",
            "seed": 42,
        }
        return data_builder, data_builder_kwargs
