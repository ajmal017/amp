import logging

import pandas as pd

import dataflow_amp.real_time.pipeline as dtfart
import core.dataflow.test.test_real_time as cdtfttrt
import helpers.datetime_ as hdatetime
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)




class TestRealTimeReturnPipeline1(hut.TestCase):
   """
   This is very similar to `TestRealTimeDagRunner` using a `ReturnPipeline` together
   with the real-time nodes.
   """

   def test1(self) -> None:
       """
       Test `RealTimeReturnPipeline` using synthetic data.
       """
       # Create the pipeline.
       dag_builder = dtfart.RealTimeReturnPipeline()
       config = dag_builder.get_config_template()
       # Inject the real-time node.
       nid = "rtds"
       delay_in_secs = 0.0
       # Use a replayed real-time starting at the same time as the data.
       initial_replayed_dt = pd.Timestamp("2010-01-04 09:30:00")
       get_wall_clock_time = lambda: hdatetime.get_current_time("naive_ET")
       data_builder, data_builder_kwargs = cdtfttrt.get_test_data_builder1()
       source_node_kwargs = {
           "delay_in_secs": delay_in_secs,
           "initial_replayed_dt": initial_replayed_dt,
           "get_wall_clock_time": get_wall_clock_time,
           "data_builder": data_builder,
           "data_builder_kwargs": data_builder_kwargs,
       }
       config["load_prices"] = cconfig.get_config_from_nested_dict({
           "source_node_name": "ReplayedTimeDataSource",
           "source_node_kwargs": source_node_kwargs
       })
       # Set up the event loop.
       execute_rt_loop_kwargs = (
           cdtfttrt.get_replayed_time_execute_rt_loop_kwargs(loop)
       )
       kwargs = {
           "config": config,
           "dag_builder": dag_builder,
           "fit_state": None,
           "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
           "dst_dir": None,
       }
       # Run.
       dag_runner = cdtfr.RealTimeDagRunner(**kwargs)
       result_bundles = hasyncio.run(dag_runner.predict(), loop)
       events = dag_runner.events
       #
       _LOG.debug("events=\n%s", events)
       _LOG.debug("result_bundles=\n%s", result_bundles)
       return events, result_bundles