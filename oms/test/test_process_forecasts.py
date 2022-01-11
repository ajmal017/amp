import asyncio
import datetime
import logging

import pandas as pd

import core.signal_processing as csigproc
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.oms_db as oomsdb
import oms.order_processor as oordproc
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)


class TestSimulatedProcessForecasts1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        (
            market_data,
            get_wall_clock_time,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        # Build a Portfolio.
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            market_data=market_data,
            asset_ids=[101, 202],
        )
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        actual = str(portfolio)
        expected = r"""# historical holdings=
asset_id                         101        202            -1
2000-01-01 09:35:00-05:00        0.0        0.0  1000000.000000
2000-01-01 09:35:01-05:00        0.0        0.0  1000000.000000
2000-01-01 09:40:01-05:00  33.322939  66.645878   900039.564897
2000-01-01 09:45:01-05:00 -24.994967    74.9849   950024.378208
# historical holdings marked to market=
asset_id                            101           202            -1
2000-01-01 09:35:00-05:00           0.0           0.0  1000000.000000
2000-01-01 09:35:01-05:00           0.0           0.0  1000000.000000
2000-01-01 09:40:01-05:00  33329.649211  66659.298423   900039.564897
2000-01-01 09:45:01-05:00 -24992.928951  74978.786852   950024.378208
# historical statistics=
           net_asset_holdings            cash    net_wealth  gross_exposure  leverage        pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000        NaN
2000-01-01 09:35:01-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000   0.000000
2000-01-01 09:40:01-05:00        99988.947634   900039.564897  1.000029e+06    99988.947634  0.099986  28.512531
2000-01-01 09:45:01-05:00        49985.857901   950024.378208  1.000010e+06    99971.715802  0.099971 -18.276422"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts1(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            #
            oomsdb.create_oms_tables(self.connection, incremental=False)
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                asset_ids=[101, 202],
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
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
            )
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
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
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        # TODO(Paul): Re-check the correctness after fixing the issue with
        #  pricing assets not currently in the portfolio.
        actual = str(portfolio)
        # TODO(Paul): Get this and the simulated test output to agree perfectly.
        expected = r"""# historical holdings=
asset_id                         101        202            -1
2000-01-01 09:35:00-05:00        0.0        0.0  1000000.000000
2000-01-01 09:35:01-05:00        NaN        NaN  1000000.000000
2000-01-01 09:40:01-05:00  33.322939  66.645878   900039.564897
2000-01-01 09:45:01-05:00 -24.994967    74.9849   950024.378208
# historical holdings marked to market=
                                    101           202            -1
2000-01-01 09:35:00-05:00           0.0           0.0  1000000.000000
2000-01-01 09:35:01-05:00           NaN           NaN  1000000.000000
2000-01-01 09:40:01-05:00  33329.649211  66659.298423   900039.564897
2000-01-01 09:45:01-05:00 -24992.928951  74978.786852   950024.378208
# historical statistics=
                           net_asset_holdings            cash    net_wealth  gross_exposure  leverage        pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000        NaN
2000-01-01 09:35:01-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000   0.000000
2000-01-01 09:40:01-05:00        99988.947634   900039.564897  1.000029e+06    99988.947634  0.099986  28.512531
2000-01-01 09:45:01-05:00        49985.857901   950024.378208  1.000010e+06    99971.715802  0.099971 -18.276422"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts2(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            #
            oomsdb.create_oms_tables(self.connection, incremental=False)
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                asset_ids=[101],
            )
            # Build OrderProcessor.
            get_wall_clock_time = portfolio._get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            # poll_kwargs["sleep_in_secs"] = 1
            poll_kwargs["timeout_in_secs"] = 60 * 10
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            termination_condition = 4
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
            )
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        columns = [101]
        prediction_data = [
            [1],
            [-1],
            [1],
            [-1],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        #
        price = portfolio.market_data.get_data_for_interval(
            index[0],
            index[-1],
            ts_col_name="timestamp_db",
            asset_ids=[101],
        )["price"]
        #
        twap = csigproc.resample(price, rule="5T").mean().rename("twap")
        rets = twap.pct_change().rename("rets")
        predictions_srs = predictions[101].rename("prediction")
        research_pnl = (
            predictions_srs.shift(2).multiply(rets).rename("research_pnl")
        )
        #
        actual = []
        actual.append(
            "TWAP=\n%s"
            % hunitest.convert_df_to_string(twap, index=True, decimals=3)
        )
        actual.append(
            "rets=\n%s"
            % hunitest.convert_df_to_string(rets, index=True, decimals=3)
        )
        actual.append(
            "prediction=\n%s"
            % hunitest.convert_df_to_string(
                predictions_srs, index=True, decimals=3
            )
        )
        actual.append(
            "research_pnl=\n%s"
            % hunitest.convert_df_to_string(research_pnl, index=True, decimals=3)
        )
        #
        actual.append(portfolio)
        actual = "\n".join(map(str, actual))
        self.check_string(actual)
