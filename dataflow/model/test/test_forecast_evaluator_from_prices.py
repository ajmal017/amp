import datetime
import logging
from typing import List

import numpy as np
import pandas as pd

import core.finance_data_example as cfidaexa
import dataflow.model.forecast_evaluator_from_prices as dtfmfefrpr
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestForecastEvaluatorFromPrices1(hunitest.TestCase):
    @staticmethod
    def get_data(
        start_datetime: pd.Timestamp,
        end_datetime: pd.Timestamp,
        asset_ids: List[int],
        *,
        bar_duration: str = "5T",
    ) -> pd.DataFrame:
        df = cfidaexa.get_forecast_price_based_dataframe(
            start_datetime,
            end_datetime,
            asset_ids,
            bar_duration=bar_duration,
        )
        return df

    def test_to_str_intraday_1_asset(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101],
        )
        forecast_evaluator = dtfmfefrpr.ForecastEvaluatorFromPrices(
            price_col="price",
            volatility_col="volatility",
            prediction_col="prediction",
        )
        actual = forecast_evaluator.to_str(
            data,
            target_gmv=1e4,
            quantization="nearest_share",
            burn_in_bars=0,
        )
        expected = r"""
# holdings=
                            101
2022-01-03 09:35:00-05:00   NaN
2022-01-03 09:40:00-05:00   NaN
2022-01-03 09:45:00-05:00  10.0
2022-01-03 09:50:00-05:00  10.0
2022-01-03 09:55:00-05:00 -10.0
2022-01-03 10:00:00-05:00 -10.0
# holdings marked to market=
                               101
2022-01-03 09:35:00-05:00      NaN
2022-01-03 09:40:00-05:00      NaN
2022-01-03 09:45:00-05:00  9973.93
2022-01-03 09:50:00-05:00  9976.60
2022-01-03 09:55:00-05:00 -9974.12
2022-01-03 10:00:00-05:00 -9975.38
# flows=
                                101
2022-01-03 09:35:00-05:00       NaN
2022-01-03 09:40:00-05:00       NaN
2022-01-03 09:45:00-05:00  -9973.93
2022-01-03 09:50:00-05:00     -0.00
2022-01-03 09:55:00-05:00  19948.23
2022-01-03 10:00:00-05:00     -0.00
# pnl=
                               101
2022-01-03 09:35:00-05:00      NaN
2022-01-03 09:40:00-05:00      NaN
2022-01-03 09:45:00-05:00 -9973.93
2022-01-03 09:50:00-05:00     2.66
2022-01-03 09:55:00-05:00    -2.48
2022-01-03 10:00:00-05:00    -1.26
# statistics=
                               pnl  gross_volume  net_volume      gmv      nmv
2022-01-03 09:35:00-05:00      NaN           NaN         NaN      NaN      NaN
2022-01-03 09:40:00-05:00      NaN           NaN         NaN      NaN      NaN
2022-01-03 09:45:00-05:00 -9973.93       9973.93     9973.93  9973.93  9973.93
2022-01-03 09:50:00-05:00     2.66          0.00        0.00  9976.60  9976.60
2022-01-03 09:55:00-05:00    -2.48      19948.23   -19948.23  9974.12 -9974.12
2022-01-03 10:00:00-05:00    -1.26          0.00        0.00  9975.38 -9975.38"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmfefrpr.ForecastEvaluatorFromPrices(
            price_col="price",
            volatility_col="volatility",
            prediction_col="prediction",
        )
        actual = forecast_evaluator.to_str(
            data,
            target_gmv=1e5,
            quantization="nearest_share",
            burn_in_bars=0,
        )
        expected = r"""
# holdings=
                            101   201   301
2022-01-03 09:35:00-05:00   NaN   NaN   NaN
2022-01-03 09:40:00-05:00   NaN   NaN   NaN
2022-01-03 09:45:00-05:00  87.0   0.0 -13.0
2022-01-03 09:50:00-05:00  48.0 -52.0   0.0
2022-01-03 09:55:00-05:00   0.0 -62.0  38.0
2022-01-03 10:00:00-05:00   0.0 -68.0  32.0
# holdings marked to market=
                                101       201       301
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN       NaN       NaN
2022-01-03 09:45:00-05:00  86773.21      0.00 -12968.54
2022-01-03 09:50:00-05:00  47887.66 -51870.06      0.00
2022-01-03 09:55:00-05:00      0.00 -61863.98  37983.85
2022-01-03 10:00:00-05:00      0.00 -67725.29  32006.56
# flows=
                                101       201       301
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN       NaN       NaN
2022-01-03 09:45:00-05:00 -86773.21     -0.00  12968.54
2022-01-03 09:50:00-05:00  38908.72  51870.06 -12981.70
2022-01-03 09:55:00-05:00  47875.76   9978.06 -37983.85
2022-01-03 10:00:00-05:00     -0.00   5975.76   6001.23
# pnl=
                                101     201       301
2022-01-03 09:35:00-05:00       NaN     NaN       NaN
2022-01-03 09:40:00-05:00       NaN     NaN       NaN
2022-01-03 09:45:00-05:00 -86773.21    0.00  12968.54
2022-01-03 09:50:00-05:00     23.17    0.00    -13.16
2022-01-03 09:55:00-05:00    -11.90  -15.86      0.00
2022-01-03 10:00:00-05:00      0.00  114.45     23.94
# statistics=
                                pnl  gross_volume  net_volume       gmv       nmv
2022-01-03 09:35:00-05:00       NaN           NaN         NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN           NaN         NaN       NaN       NaN
2022-01-03 09:45:00-05:00 -73804.67      99741.75    73804.67  99741.75  73804.67
2022-01-03 09:50:00-05:00     10.01     103760.48   -77797.08  99757.72  -3982.40
2022-01-03 09:55:00-05:00    -27.76      95837.66   -19869.97  99847.83 -23880.13
2022-01-03 10:00:00-05:00    138.39      11976.99   -11976.99  99731.85 -35718.74"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets_varying_gmv(self) -> None:
        start_timestamp = pd.Timestamp(
            "2022-01-03 09:35:00", tz="America/New_York"
        )
        end_timestamp = pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York")
        data = self.get_data(
            start_timestamp,
            end_timestamp,
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmfefrpr.ForecastEvaluatorFromPrices(
            price_col="price",
            volatility_col="volatility",
            prediction_col="prediction",
        )
        target_gmv = pd.Series(
            [0.0, 0.0, 1e3, 1e4, 1e3, 1e2],
            [
                datetime.time(9, 35),
                datetime.time(9, 40),
                datetime.time(9, 45),
                datetime.time(9, 50),
                datetime.time(9, 55),
                datetime.time(10, 0),
            ],
        )
        actual = forecast_evaluator.to_str(
            data,
            target_gmv=target_gmv,
            quantization="nearest_share",
            burn_in_bars=0,
        )
        expected = r"""
# holdings=
                           101  201  301
2022-01-03 09:35:00-05:00  NaN  NaN  NaN
2022-01-03 09:40:00-05:00  NaN  NaN  NaN
2022-01-03 09:45:00-05:00  0.0  0.0 -0.0
2022-01-03 09:50:00-05:00  0.0 -1.0  0.0
2022-01-03 09:55:00-05:00  0.0 -6.0  4.0
2022-01-03 10:00:00-05:00  0.0 -1.0  0.0
# holdings marked to market=
                           101      201     301
2022-01-03 09:35:00-05:00  NaN      NaN     NaN
2022-01-03 09:40:00-05:00  NaN      NaN     NaN
2022-01-03 09:45:00-05:00  0.0     0.00    -0.0
2022-01-03 09:50:00-05:00  0.0  -997.50     0.0
2022-01-03 09:55:00-05:00  0.0 -5986.84  3998.3
2022-01-03 10:00:00-05:00  0.0  -995.96     0.0
# flows=
                           101      201      301
2022-01-03 09:35:00-05:00  NaN      NaN      NaN
2022-01-03 09:40:00-05:00  NaN      NaN      NaN
2022-01-03 09:45:00-05:00 -0.0    -0.00     0.00
2022-01-03 09:50:00-05:00 -0.0   997.50    -0.00
2022-01-03 09:55:00-05:00 -0.0  4989.03 -3998.30
2022-01-03 10:00:00-05:00 -0.0 -4979.80  4000.82
# pnl=
                           101    201   301
2022-01-03 09:35:00-05:00  NaN    NaN   NaN
2022-01-03 09:40:00-05:00  NaN    NaN   NaN
2022-01-03 09:45:00-05:00  0.0   0.00  0.00
2022-01-03 09:50:00-05:00  0.0   0.00  0.00
2022-01-03 09:55:00-05:00  0.0  -0.30  0.00
2022-01-03 10:00:00-05:00  0.0  11.08  2.52
# statistics=
                            pnl  gross_volume  net_volume      gmv      nmv
2022-01-03 09:35:00-05:00   NaN           NaN         NaN      NaN      NaN
2022-01-03 09:40:00-05:00   NaN           NaN         NaN      NaN      NaN
2022-01-03 09:45:00-05:00   0.0          0.00        0.00     0.00     0.00
2022-01-03 09:50:00-05:00   0.0        997.50     -997.50   997.50  -997.50
2022-01-03 09:55:00-05:00  -0.3       8987.33     -990.73  9985.14 -1988.54
2022-01-03 10:00:00-05:00  13.6       8980.62      978.98   995.96  -995.96"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_log_portfolio_read_portfolio(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmfefrpr.ForecastEvaluatorFromPrices(
            price_col="price",
            volatility_col="volatility",
            prediction_col="prediction",
        )
        #
        log_dir = self.get_scratch_space()
        _ = forecast_evaluator.log_portfolio(
            data,
            log_dir,
            target_gmv=1e6,
            quantization="nearest_share",
            burn_in_bars=0,
        )
        #
        portfolio_df, stats_df = forecast_evaluator.read_portfolio(log_dir)
        # Ensure that the `int` asset id type is recovered.
        asset_id_idx = portfolio_df.columns.levels[1]
        self.assertEqual(asset_id_idx.dtype.type, np.int64)
        #
        precision = 2
        portfolio_df_str = hpandas.df_to_str(
            portfolio_df, num_rows=None, precision=precision
        )
        expected_portfolio_df_str = r"""
                            price                  volatility                     prediction                     holdings                 position                             flow                              pnl
                              101     201      301        101       201       301        101       201       301      101    201    301        101        201        301        101        201        301        101      201        301
2022-01-03 09:35:00-05:00  998.90  999.66   999.87        NaN       NaN       NaN   8.43e-04 -1.77e-04 -2.38e-04      NaN    NaN    NaN        NaN        NaN        NaN        NaN        NaN        NaN        NaN      NaN        NaN
2022-01-03 09:40:00-05:00  998.17  999.60   998.00   7.25e-04  5.14e-05  1.87e-03   8.58e-04  4.26e-04 -1.84e-03      NaN    NaN    NaN        NaN        NaN        NaN        NaN        NaN        NaN        NaN      NaN        NaN
2022-01-03 09:45:00-05:00  997.39  998.63   997.58   7.57e-04  7.29e-04  1.28e-03   4.75e-04 -9.85e-04  1.70e-04    871.0    0.0 -131.0  868729.51       0.00 -130683.00 -868729.51      -0.00  130683.00 -868729.51     0.00  130683.00
2022-01-03 09:50:00-05:00  997.66  997.50   998.59   6.02e-04  9.21e-04  1.17e-03  -4.51e-04 -1.11e-03 -1.76e-04    483.0 -519.0    0.0  481869.56 -517703.09       0.00  387091.91  517703.09 -130815.57     231.96     0.00    -132.57
2022-01-03 09:55:00-05:00  997.41  997.81   999.57   5.07e-04  7.64e-04  1.11e-03  -7.55e-04 -7.61e-04  7.68e-05      0.0 -620.0  382.0       0.00 -618639.79  381837.62  481749.79  100778.42 -381837.62    -119.77  -158.29       0.00
2022-01-03 10:00:00-05:00  997.54  995.96  1000.20   4.27e-04  1.21e-03  9.87e-04  -8.15e-04  6.48e-04  1.54e-03      0.0 -680.0  321.0       0.00 -677252.94  321065.77      -0.00   59757.61   61012.50       0.00  1144.47     240.65"""
        self.assert_equal(
            portfolio_df_str, expected_portfolio_df_str, fuzzy_match=True
        )
        #
        stats_df_str = hpandas.df_to_str(
            stats_df, num_rows=None, precision=precision
        )
        expected_stats_df_str = r"""
                                 pnl  gross_volume  net_volume       gmv        nmv
2022-01-03 09:35:00-05:00        NaN           NaN         NaN       NaN        NaN
2022-01-03 09:40:00-05:00        NaN           NaN         NaN       NaN        NaN
2022-01-03 09:45:00-05:00 -738046.51      9.99e+05   738046.51  9.99e+05  738046.51
2022-01-03 09:50:00-05:00      99.39      1.04e+06  -773979.43  1.00e+06  -35833.52
2022-01-03 09:55:00-05:00    -278.06      9.64e+05  -200690.59  1.00e+06 -236802.17
2022-01-03 10:00:00-05:00    1385.12      1.21e+05  -120770.11  9.98e+05 -356187.17"""
        self.assert_equal(stats_df_str, expected_stats_df_str, fuzzy_match=True)
