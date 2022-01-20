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
2022-01-03 09:30:00-05:00       NaN
2022-01-03 09:35:00-05:00       NaN
2022-01-03 09:40:00-05:00  7.43e-07
2022-01-03 09:45:00-05:00 -7.01e-07
2022-01-03 09:50:00-05:00 -6.29e-07
2022-01-03 09:55:00-05:00  2.90e-08
2022-01-03 10:00:00-05:00       NaN
# pnl=
                                101
2022-01-03 09:30:00-05:00       NaN
2022-01-03 09:35:00-05:00       NaN
2022-01-03 09:40:00-05:00       NaN
2022-01-03 09:45:00-05:00  1.99e-10
2022-01-03 09:50:00-05:00  1.74e-10
2022-01-03 09:55:00-05:00 -7.96e-11
2022-01-03 10:00:00-05:00  2.44e-11
# statistics=
                           net_asset_holdings  gross_exposure       pnl
2022-01-03 09:30:00-05:00                 NaN             NaN       NaN
2022-01-03 09:35:00-05:00                 NaN             NaN       NaN
2022-01-03 09:40:00-05:00            7.43e-07        7.43e-07       NaN
2022-01-03 09:45:00-05:00           -7.01e-07        7.01e-07  1.99e-10
2022-01-03 09:50:00-05:00           -6.29e-07        6.29e-07  1.74e-10
2022-01-03 09:55:00-05:00            2.90e-08        2.90e-08 -7.96e-11
2022-01-03 10:00:00-05:00                 NaN             NaN  2.44e-11"""
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
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00  7.43e-07 -2.61e-06 -1.66e-06
2022-01-03 09:45:00-05:00 -7.01e-07  4.72e-07  4.45e-07
2022-01-03 09:50:00-05:00 -6.29e-07  2.97e-07  4.29e-07
2022-01-03 09:55:00-05:00  2.90e-08 -2.86e-06 -1.80e-07
2022-01-03 10:00:00-05:00       NaN       NaN       NaN
# pnl=
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN       NaN       NaN
2022-01-03 09:45:00-05:00  1.99e-10  4.84e-09  3.05e-09
2022-01-03 09:50:00-05:00  1.74e-10 -8.36e-11  7.55e-11
2022-01-03 09:55:00-05:00 -7.96e-11  1.26e-10 -7.55e-11
2022-01-03 10:00:00-05:00  2.44e-11  2.82e-09 -1.38e-11
# statistics=
                           net_asset_holdings  gross_exposure       pnl
2022-01-03 09:30:00-05:00                 NaN             NaN       NaN
2022-01-03 09:35:00-05:00                 NaN             NaN       NaN
2022-01-03 09:40:00-05:00           -1.18e-06        5.02e-06       NaN
2022-01-03 09:45:00-05:00            7.23e-08        1.62e-06  8.09e-09
2022-01-03 09:50:00-05:00            3.21e-08        1.36e-06  1.66e-10
2022-01-03 09:55:00-05:00           -1.00e-06        3.07e-06 -2.87e-11
2022-01-03 10:00:00-05:00                 NaN             NaN  2.83e-09"""
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
2022-01-03 09:40:00-05:00  14823.36 -52131.13 -33045.51
2022-01-03 09:45:00-05:00 -43301.47  29172.31  27526.22
2022-01-03 09:50:00-05:00 -46446.11  21899.27  31654.62
2022-01-03 09:55:00-05:00    944.18 -93202.60  -5853.22
2022-01-03 10:00:00-05:00       NaN       NaN       NaN
# pnl=
                             101    201    301
2022-01-03 09:30:00-05:00    NaN    NaN    NaN
2022-01-03 09:35:00-05:00    NaN    NaN    NaN
2022-01-03 09:40:00-05:00    NaN    NaN    NaN
2022-01-03 09:45:00-05:00   3.96  96.44  60.91
2022-01-03 09:50:00-05:00  10.76  -5.16   4.67
2022-01-03 09:55:00-05:00  -5.88   9.33  -5.57
2022-01-03 10:00:00-05:00   0.80  91.79  -0.45
# statistics=
                           net_asset_holdings  gross_exposure     pnl
2022-01-03 09:30:00-05:00                 NaN             NaN     NaN
2022-01-03 09:35:00-05:00                 NaN             NaN     NaN
2022-01-03 09:40:00-05:00           -23451.10        100000.0     NaN
2022-01-03 09:45:00-05:00             4465.69        100000.0  161.31
2022-01-03 09:50:00-05:00             2369.26        100000.0   10.27
2022-01-03 09:55:00-05:00           -32703.88        100000.0   -2.12
2022-01-03 10:00:00-05:00                 NaN             NaN   92.14"""
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
            vol = csigproc.compute_rolling_norm(price, tau=4)
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
