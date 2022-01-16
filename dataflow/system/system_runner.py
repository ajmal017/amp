"""
Import as:

import dataflow.system.system_runner as dtfsysyrun
"""

import abc
import asyncio
import logging
import os
from typing import Coroutine, Tuple

import pandas as pd

import market_data_lime as mdlime

import core.config as cconfig
import core.real_time_example as cretiexa
import dataflow.core.builders as dtfcorbuil
import dataflow.system as dtfsys
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import market_data as mdata
import oms as oms

_LOG = logging.getLogger(__name__)


class SystemRunner(abc.ABC):
    """
    Create an end-to-end DataFlow-based system comprised of:
    - `MarketData`
    - `Dag`
    - `DagRunner`
    - `Portfolio`
    - `OrderProcessor`
    """

    @abc.abstractmethod
    def get_market_data(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> mdata.AbstractMarketData:
        ...

    @abc.abstractmethod
    def get_portfolio(
        self,
        event_loop: asyncio.AbstractEventLoop,
        market_data: mdata.AbstractMarketData,
    ) -> oms.AbstractPortfolio:
        ...

    @abc.abstractmethod
    def get_dag(
        self, portfolio: oms.AbstractPortfolio
    ) -> Tuple[cconfig.Config, dtfcorbuil.DagBuilder]:
        ...

    def get_order_processor(
        self, portfolio: oms.AbstractPortfolio
    ) -> oms.OrderProcessor:
        ...

    def get_order_processor_coroutine(
        self, portfolio: oms.AbstractPortfolio, real_time_loop_time_out_in_secs: int
    ) -> Coroutine:
        # Build OrderProcessor.
        order_processor = self.get_order_processor(portfolio)
        # TODO(Paul): Maybe make this public.
        initial_timestamp = portfolio._initial_timestamp
        offset = pd.Timedelta(real_time_loop_time_out_in_secs, unit="seconds")
        termination_condition = initial_timestamp + offset
        order_processor_coroutine = order_processor.run_loop(
            termination_condition
        )
        return order_processor_coroutine

    def get_dag_runner(
        self,
        dag_builder,
        config,
        event_loop,
        get_wall_clock_time,
        real_time_loop_time_out_in_secs,
    ):
        # Set up the event loop.
        sleep_interval_in_secs = 60 * 5
        execute_rt_loop_kwargs = (
            cretiexa.get_replayed_time_execute_rt_loop_kwargs(
                sleep_interval_in_secs,
                get_wall_clock_time=get_wall_clock_time,
                event_loop=event_loop,
            )
        )
        execute_rt_loop_kwargs[
            "time_out_in_secs"
        ] = real_time_loop_time_out_in_secs
        dag_runner_kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
        }
        dag_runner = dtfsys.RealTimeDagRunner(**dag_runner_kwargs)
        return dag_runner


# #############################################################################


class SystemWithOmsRunner:
    pass