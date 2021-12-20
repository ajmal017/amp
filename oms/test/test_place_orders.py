import asyncio
import datetime
import logging

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import market_data.market_data_interface_example as mdmdinex
import oms.mr_market as omrmark
import oms.oms_db as oomsdb
import oms.place_orders as oplaorde
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)


class TestPlaceOrders1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `place_orders()` logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(data, index=index, columns=columns)
        # Build a Portfolio.
        initial_timestamp = pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        )
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        config["market_data_interface"] = market_data_interface
        config["portfolio"] = portfolio
        config["broker"] = portfolio.broker
        config["order_type"] = "price@twap"
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        # Run.
        execution_mode = "batch"
        await oplaorde.place_orders(
            predictions,
            execution_mode,
            config,
        )
        # TODO(Paul): Re-check the correctness after fixing the issue with
        #  pricing assets not currently in the portfolio.
        actual = portfolio.get_historical_holdings()
        expected = r"""asset_id                         101         202            -1
2000-01-01 09:30:00-05:00        NaN         NaN  1000000.000000
2000-01-01 09:35:01-05:00        NaN         NaN  1000000.000000
2000-01-01 09:40:01-05:00  76.923077  153.846154   769250.118513
2000-01-01 09:45:01-05:00  -7.141889   21.425667   985761.256141"""
        self.assert_equal(str(actual), expected, fuzzy_match=True)


class TestMockedPlaceOrders1(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            # TODO(gp): Factor out in a single function.
            oomsdb.create_accepted_orders_table(
                self.connection, incremental=False
            )
            oomsdb.create_submitted_orders_table(
                self.connection, incremental=False
            )
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                initial_timestamp,
            )
            # Build OrderProcessor.
            get_wall_clock_time = portfolio._get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            # poll_kwargs["sleep_in_secs"] = 1
            poll_kwargs["timeout_in_secs"] = 60 * 10
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            termination_condition = 3
            order_processor = omrmark.order_processor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                termination_condition,
                poll_kwargs=poll_kwargs,
            )
            coroutines = [self._test_mocked_system1(portfolio), order_processor]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run place_orders() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(data, index=index, columns=columns)
        # TODO(gp): Remove mdi and broker since they are passed through Portfolio.
        config["market_data_interface"] = portfolio._market_data_interface
        config["portfolio"] = portfolio
        config["broker"] = portfolio.broker
        config["order_type"] = "price@twap"
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        # Run.
        execution_mode = "batch"
        await oplaorde.place_orders(
            predictions,
            execution_mode,
            config,
        )
        # TODO(Paul): Re-check the correctness after fixing the issue with
        #  pricing assets not currently in the portfolio.
        actual = portfolio.get_historical_holdings()
        expected = r"""asset_id                         101         202            -1
2000-01-01 09:30:00-05:00        NaN         NaN  1000000.000000
2000-01-01 09:35:01-05:00        NaN         NaN  1000000.000000
2000-01-01 09:40:01-05:00  76.923077  153.846154   769250.118513
2000-01-01 09:45:01-05:00  -7.141889   21.425667   985761.256141"""
        self.assert_equal(str(actual), expected, fuzzy_match=True)
