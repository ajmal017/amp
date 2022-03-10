import asyncio
import datetime
import logging
import os
from typing import Tuple

import pandas as pd
import pytest

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hunit_test as hunitest
import market_data as mdata

# TODO(gp): Use import oms
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore

_LOG = logging.getLogger(__name__)


class TestDataFrameProcessForecasts1(hunitest.TestCase):
    @pytest.mark.skip("Generate manually files used by other tests")
    def test_generate_data(self) -> None:
        """
        Generate market data, predictions, and volatility files used as inputs
        by other tests.

        This test might need to be run from an `amp` container.
        """
        import core.finance_data_example as cfidaexa

        dir_ = self.get_input_dir()
        hio.create_dir(dir_, incremental=False)
        # Market data.
        start_datetime = pd.Timestamp(
            "2000-01-01 09:35:00-05:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp(
            "2000-01-01 10:30:00-05:00", tz="America/New_York"
        )
        asset_ids = [100, 200]
        market_data_df = mdata.generate_random_bars(
            start_datetime, end_datetime, asset_ids
        )
        market_data_df.to_csv(os.path.join(dir_, "market_data_df.csv"))
        # Prediction.
        forecasts = cfidaexa.get_forecast_dataframe(
            start_datetime + pd.Timedelta("5T"), end_datetime, asset_ids
        )
        prediction_df = forecasts["prediction"]
        prediction_df.to_csv(os.path.join(dir_, "prediction_df.csv"))
        # Volatility.
        volatility_df = forecasts["volatility"]
        volatility_df.to_csv(os.path.join(dir_, "volatility_df.csv"))

    # ///////////////////////////////////////////////////////////////////////////

    def get_input_filename(self, filename: str) -> str:
        dir_ = self.get_input_dir(test_method_name="test_generate_data")
        filename = os.path.join(dir_, filename)
        hdbg.dassert_file_exists(filename)
        return filename

    def get_market_data(self, event_loop) -> mdata.MarketData:
        filename = self.get_input_filename("market_data_df.csv")
        market_data_df = pd.read_csv(
            filename,
            index_col=0,
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        market_data_df = market_data_df.convert_dtypes()
        initial_replayed_delay = 5
        market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
            event_loop,
            initial_replayed_delay,
            market_data_df,
        )
        return market_data

    def get_predictions_and_volatility(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        filename = self.get_input_filename("prediction_df.csv")
        prediction_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        prediction_df = prediction_df.convert_dtypes()
        prediction_df.index = prediction_df.index.tz_convert("America/New_York")
        prediction_df.columns = prediction_df.columns.astype("int64")
        #
        filename = self.get_input_filename("volatility_df.csv")
        volatility_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        volatility_df = volatility_df.convert_dtypes()
        volatility_df.index = volatility_df.index.tz_convert("America/New_York")
        volatility_df.columns = volatility_df.columns.astype("int64")
        return prediction_df, volatility_df

    # TODO(gp): This can become an _example.
    def get_portfolio(
        self,
        event_loop,
    ) -> omportfo.DataFramePortfolio:
        market_data = self.get_market_data(event_loop)
        asset_ids = market_data._asset_ids
        strategy_id = "strategy"
        account = "account"
        timestamp_col = "end_datetime"
        mark_to_market_col = "close"
        pricing_method = "last"
        initial_holdings = pd.Series([-50, 50, 0], asset_ids + [-1])
        column_remap = {
            "bid": "bid",
            "ask": "ask",
            "price": "close",
            "midpoint": "midpoint",
        }
        portfolio = oporexam.get_DataFramePortfolio_example2(
            strategy_id,
            account,
            market_data,
            timestamp_col,
            mark_to_market_col,
            pricing_method,
            initial_holdings,
            column_remap=column_remap,
        )
        return portfolio

    # TODO(gp): This can become an _example.
    @staticmethod
    def get_process_forecasts_config() -> cconfig.Config:
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "batch_optimizer",
                "dollar_neutrality_penalty": 0.1,
                "volatility_penalty": 0.5,
                "turnover_penalty": 0.0,
                "target_gmv": 1e5,
                "target_gmv_upper_bound_multiple": 1.0,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    async def run_process_forecasts(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        predictions, volatility = self.get_predictions_and_volatility()
        portfolio = self.get_portfolio(event_loop)
        config = self.get_process_forecasts_config()
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        actual = str(portfolio)
        expected = r"""
# historical holdings=
asset_id                     100    200    -1
2000-01-01 09:40:01-05:00 -50.00  50.00    0.00
2000-01-01 09:45:01-05:00 -50.04  49.85  196.17
2000-01-01 09:50:01-05:00 -50.00  49.93   69.29
2000-01-01 09:55:01-05:00 -49.93  49.66  273.58
2000-01-01 10:00:01-05:00 -50.71  50.17  531.34
2000-01-01 10:05:01-05:00 -50.18  49.75  426.44
2000-01-01 10:10:01-05:00 -50.41  50.24  175.32
2000-01-01 10:15:01-05:00 -50.53  50.19  331.97
2000-01-01 10:20:01-05:00  50.27 -50.03  337.24
2000-01-01 10:25:01-05:00  50.74 -50.48  316.09
2000-01-01 10:30:01-05:00 -50.66  50.52  120.61
# historical holdings marked to market=
asset_id                        100       200     -1
2000-01-01 09:40:01-05:00 -49870.58  50090.60    0.00
2000-01-01 09:45:01-05:00 -50005.11  49880.90  196.17
2000-01-01 09:50:01-05:00 -49815.06  50045.95   69.29
2000-01-01 09:55:01-05:00 -49624.47  49852.98  273.58
2000-01-01 10:00:01-05:00 -50280.63  50138.68  531.34
2000-01-01 10:05:01-05:00 -49772.08  49522.56  426.44
2000-01-01 10:10:01-05:00 -49905.91  50058.52  175.32
2000-01-01 10:15:01-05:00 -50134.74  50026.61  331.97
2000-01-01 10:20:01-05:00  49853.27 -49854.22  337.24
2000-01-01 10:25:01-05:00  50339.75 -50164.13  316.09
2000-01-01 10:30:01-05:00 -50210.54  50414.70  120.61
# historical flows=
asset_id                         100        200
2000-01-01 09:45:01-05:00      42.09     154.08
2000-01-01 09:50:01-05:00     -43.43     -83.45
2000-01-01 09:55:01-05:00     -69.61     273.90
2000-01-01 10:00:01-05:00     772.15    -514.39
2000-01-01 10:05:01-05:00    -524.75     419.85
2000-01-01 10:10:01-05:00     233.90    -485.02
2000-01-01 10:15:01-05:00     111.36      45.30
2000-01-01 10:20:01-05:00  -99949.28   99954.54
2000-01-01 10:25:01-05:00    -463.45     442.30
2000-01-01 10:30:01-05:00  100485.88 -100681.36
# historical pnl=
asset_id                      100     200
2000-01-01 09:40:01-05:00     NaN     NaN
2000-01-01 09:45:01-05:00  -92.44  -55.62
2000-01-01 09:50:01-05:00  146.63   81.60
2000-01-01 09:55:01-05:00  120.97   80.93
2000-01-01 10:00:01-05:00  115.99 -228.70
2000-01-01 10:05:01-05:00  -16.19 -196.27
2000-01-01 10:10:01-05:00  100.07   50.94
2000-01-01 10:15:01-05:00 -117.48   13.39
2000-01-01 10:20:01-05:00   38.73   73.71
2000-01-01 10:25:01-05:00   23.03  132.39
2000-01-01 10:30:01-05:00  -64.41 -102.54
# historical statistics=
                              pnl  gross_volume  net_volume        gmv     nmv    cash  net_wealth  leverage
2000-01-01 09:40:01-05:00     NaN          0.00        0.00   99961.18  220.02    0.00      220.02    454.32
2000-01-01 09:45:01-05:00 -148.06        196.17     -196.17   99886.01 -124.21  196.17       71.96   1388.12
2000-01-01 09:50:01-05:00  228.22        126.88      126.88   99861.01  230.89   69.29      300.18    332.67
2000-01-01 09:55:01-05:00  201.91        343.51     -204.29   99477.46  228.51  273.58      502.09    198.13
2000-01-01 10:00:01-05:00 -112.70       1286.55     -257.76  100419.31 -141.96  531.34      389.38    257.89
2000-01-01 10:05:01-05:00 -212.46        944.60      104.90   99294.64 -249.52  426.44      176.92    561.23
2000-01-01 10:10:01-05:00  151.01        718.91      251.12   99964.43  152.61  175.32      327.93    304.83
2000-01-01 10:15:01-05:00 -104.09        156.65     -156.65  100161.35 -108.13  331.97      223.84    447.46
2000-01-01 10:20:01-05:00  112.45     199903.82       -5.27   99707.48   -0.95  337.24      336.29    296.49
2000-01-01 10:25:01-05:00  155.42        905.74       21.15  100503.87  175.62  316.09      491.71    204.40
2000-01-01 10:30:01-05:00 -166.95     201167.24      195.48  100625.24  204.16  120.61      324.76    309.84
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    # TODO(gp): -> test1
    def test_initialization1(self) -> None:
        """
        Run the process forecasts.
        """
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self.run_process_forecasts(event_loop), event_loop=event_loop
            )
