import logging
from typing import List

import pandas as pd

import core.artificial_signal_generators as carsigen
import core.signal_processing as csigproc
import dataflow.model.forecast_evaluator as dtfmofoeva
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestForecastEvaluator1(hunitest.TestCase):
    def test_to_str_intraday_1_asset_floating_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data)
        expected = r"""# holdings marked to market=
                            101
2022-01-03 09:30:00-05:00   NaN
2022-01-03 09:35:00-05:00   NaN
2022-01-03 09:40:00-05:00  1.02
2022-01-03 09:45:00-05:00 -0.92
2022-01-03 09:50:00-05:00 -1.04
2022-01-03 09:55:00-05:00  0.06
2022-01-03 10:00:00-05:00   NaN
# pnl=
                                101
2022-01-03 09:30:00-05:00       NaN
2022-01-03 09:35:00-05:00       NaN
2022-01-03 09:40:00-05:00       NaN
2022-01-03 09:45:00-05:00  2.73e-04
2022-01-03 09:50:00-05:00  2.30e-04
2022-01-03 09:55:00-05:00 -1.32e-04
2022-01-03 10:00:00-05:00  4.81e-05
# statistics=
                           net_asset_holdings  gross_exposure       pnl
2022-01-03 09:30:00-05:00                 NaN             NaN       NaN
2022-01-03 09:35:00-05:00                 NaN             NaN       NaN
2022-01-03 09:40:00-05:00                1.02            1.02       NaN
2022-01-03 09:45:00-05:00               -0.92            0.92  2.73e-04
2022-01-03 09:50:00-05:00               -1.04            1.04  2.30e-04
2022-01-03 09:55:00-05:00                0.06            0.06 -1.32e-04
2022-01-03 10:00:00-05:00                 NaN             NaN  4.81e-05"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_1_asset_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=10000)
        expected = r"""# holdings marked to market=
                               101
2022-01-03 09:30:00-05:00      NaN
2022-01-03 09:35:00-05:00      NaN
2022-01-03 09:40:00-05:00  10000.0
2022-01-03 09:45:00-05:00 -10000.0
2022-01-03 09:50:00-05:00 -10000.0
2022-01-03 09:55:00-05:00  10000.0
2022-01-03 10:00:00-05:00      NaN
# pnl=
                            101
2022-01-03 09:30:00-05:00   NaN
2022-01-03 09:35:00-05:00   NaN
2022-01-03 09:40:00-05:00   NaN
2022-01-03 09:45:00-05:00  2.67
2022-01-03 09:50:00-05:00  2.49
2022-01-03 09:55:00-05:00 -1.26
2022-01-03 10:00:00-05:00  8.43
# statistics=
                           net_asset_holdings  gross_exposure   pnl
2022-01-03 09:30:00-05:00                 NaN             NaN   NaN
2022-01-03 09:35:00-05:00                 NaN             NaN   NaN
2022-01-03 09:40:00-05:00             10000.0         10000.0   NaN
2022-01-03 09:45:00-05:00            -10000.0         10000.0  2.67
2022-01-03 09:50:00-05:00            -10000.0         10000.0  2.49
2022-01-03 09:55:00-05:00             10000.0         10000.0 -1.26
2022-01-03 10:00:00-05:00                 NaN             NaN  8.43"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets_floating_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data)
        expected = r"""# holdings marked to market=
                            101   201   301
2022-01-03 09:30:00-05:00   NaN   NaN   NaN
2022-01-03 09:35:00-05:00   NaN   NaN   NaN
2022-01-03 09:40:00-05:00  1.02 -2.30 -2.63
2022-01-03 09:45:00-05:00 -0.92  0.60  0.98
2022-01-03 09:50:00-05:00 -1.04  0.22  0.35
2022-01-03 09:55:00-05:00  0.06 -2.63 -0.18
2022-01-03 10:00:00-05:00   NaN   NaN   NaN
# pnl=
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN       NaN       NaN
2022-01-03 09:45:00-05:00  2.73e-04  4.26e-03  4.85e-03
2022-01-03 09:50:00-05:00  2.30e-04 -1.06e-04  1.67e-04
2022-01-03 09:55:00-05:00 -1.32e-04  9.42e-05 -6.08e-05
2022-01-03 10:00:00-05:00  4.81e-05  2.59e-03 -1.37e-05
# statistics=
                           net_asset_holdings  gross_exposure       pnl
2022-01-03 09:30:00-05:00                 NaN             NaN       NaN
2022-01-03 09:35:00-05:00                 NaN             NaN       NaN
2022-01-03 09:40:00-05:00               -3.91            5.96       NaN
2022-01-03 09:45:00-05:00                0.66            2.51  9.39e-03
2022-01-03 09:50:00-05:00               -0.48            1.61  2.90e-04
2022-01-03 09:55:00-05:00               -2.75            2.87 -9.85e-05
2022-01-03 10:00:00-05:00                 NaN             NaN  2.63e-03"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=100000)
        expected = r"""# holdings marked to market=
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00  17185.97 -38629.90 -44184.13
2022-01-03 09:45:00-05:00 -36831.11  23956.26  39212.62
2022-01-03 09:50:00-05:00 -64790.20  13741.93  21467.87
2022-01-03 09:55:00-05:00   1988.18 -91783.22  -6228.60
2022-01-03 10:00:00-05:00       NaN       NaN       NaN
# pnl=
                            101    201    301
2022-01-03 09:30:00-05:00   NaN    NaN    NaN
2022-01-03 09:35:00-05:00   NaN    NaN    NaN
2022-01-03 09:40:00-05:00   NaN    NaN    NaN
2022-01-03 09:45:00-05:00  4.59  71.46  81.44
2022-01-03 09:50:00-05:00  9.15  -4.24   6.65
2022-01-03 09:55:00-05:00 -8.20   5.85  -3.78
2022-01-03 10:00:00-05:00  1.68  90.39  -0.48
# statistics=
                           net_asset_holdings  gross_exposure     pnl
2022-01-03 09:30:00-05:00                 NaN             NaN     NaN
2022-01-03 09:35:00-05:00                 NaN             NaN     NaN
2022-01-03 09:40:00-05:00           -65628.06        100000.0     NaN
2022-01-03 09:45:00-05:00            26337.78        100000.0  157.49
2022-01-03 09:50:00-05:00           -29580.40        100000.0   11.56
2022-01-03 09:55:00-05:00           -96023.64        100000.0   -6.12
2022-01-03 10:00:00-05:00                 NaN             NaN   91.59"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_multiday_1_asset_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-05 10:00:00", tz="America/New_York"),
            asset_ids=[101],
            bar_duration="1H",
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=100000)
        expected = r"""# holdings marked to market=
                                101
2022-01-03 09:30:00-05:00       NaN
2022-01-03 10:30:00-05:00       NaN
2022-01-03 11:30:00-05:00  100000.0
2022-01-03 12:30:00-05:00 -100000.0
2022-01-03 13:30:00-05:00 -100000.0
2022-01-03 14:30:00-05:00  100000.0
2022-01-03 15:30:00-05:00 -100000.0
2022-01-04 09:30:00-05:00 -100000.0
2022-01-04 10:30:00-05:00 -100000.0
2022-01-04 11:30:00-05:00 -100000.0
2022-01-04 12:30:00-05:00 -100000.0
2022-01-04 13:30:00-05:00 -100000.0
2022-01-04 14:30:00-05:00 -100000.0
2022-01-04 15:30:00-05:00  100000.0
2022-01-05 09:30:00-05:00       NaN
# pnl=
                             101
2022-01-03 09:30:00-05:00    NaN
2022-01-03 10:30:00-05:00    NaN
2022-01-03 11:30:00-05:00    NaN
2022-01-03 12:30:00-05:00  26.70
2022-01-03 13:30:00-05:00  24.85
2022-01-03 14:30:00-05:00 -12.65
2022-01-03 15:30:00-05:00  84.34
2022-01-04 09:30:00-05:00 -85.83
2022-01-04 10:30:00-05:00 -47.53
2022-01-04 11:30:00-05:00  45.07
2022-01-04 12:30:00-05:00  75.46
2022-01-04 13:30:00-05:00  81.45
2022-01-04 14:30:00-05:00  34.38
2022-01-04 15:30:00-05:00   5.14
2022-01-05 09:30:00-05:00 -97.18
# statistics=
                           net_asset_holdings  gross_exposure    pnl
2022-01-03 09:30:00-05:00                 NaN             NaN    NaN
2022-01-03 10:30:00-05:00                 NaN             NaN    NaN
2022-01-03 11:30:00-05:00            100000.0        100000.0    NaN
2022-01-03 12:30:00-05:00           -100000.0        100000.0  26.70
2022-01-03 13:30:00-05:00           -100000.0        100000.0  24.85
2022-01-03 14:30:00-05:00            100000.0        100000.0 -12.65
2022-01-03 15:30:00-05:00           -100000.0        100000.0  84.34
2022-01-04 09:30:00-05:00           -100000.0        100000.0 -85.83
2022-01-04 10:30:00-05:00           -100000.0        100000.0 -47.53
2022-01-04 11:30:00-05:00           -100000.0        100000.0  45.07
2022-01-04 12:30:00-05:00           -100000.0        100000.0  75.46
2022-01-04 13:30:00-05:00           -100000.0        100000.0  81.45
2022-01-04 14:30:00-05:00           -100000.0        100000.0  34.38
2022-01-04 15:30:00-05:00            100000.0        100000.0   5.14
2022-01-05 09:30:00-05:00                 NaN             NaN -97.18"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @staticmethod
    def get_data(
        start_datetime: pd.Timestamp,
        end_datetime: pd.Timestamp,
        asset_ids: List[int],
        *,
        bar_duration: str = "5T",
    ) -> pd.DataFrame:
        price_process = carsigen.PriceProcess(seed=10)
        dfs = {}
        for asset_id in asset_ids:
            price = price_process.generate_price_series_from_normal_log_returns(
                start_datetime,
                end_datetime,
                asset_id,
                bar_duration=bar_duration,
            )
            vol = csigproc.compute_rolling_norm(price.pct_change(), tau=4)
            rets = price.pct_change()
            noise_integral = (
                price_process.generate_price_series_from_normal_log_returns(
                    start_datetime,
                    end_datetime,
                    asset_id,
                    bar_duration=bar_duration,
                )
            )
            noise = noise_integral.pct_change()
            pred = rets.shift(-2) + noise
            df = pd.DataFrame(
                {
                    "rets": rets,
                    "vol": vol,
                    "pred": pred,
                }
            )
            dfs[asset_id] = df
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        return df
