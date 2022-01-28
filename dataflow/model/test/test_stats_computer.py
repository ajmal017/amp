import logging

import pandas as pd

import core.artificial_signal_generators as carsigen
import dataflow.model.stats_computer as dtfmostcom
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestStatsComputer1(hunitest.TestCase):
    def test_compute_pnl_stats1(self) -> None:
        srs = self._get_pnl_srs()
        sc = dtfmostcom.StatsComputer()
        actual = hpandas.dataframe_to_str(sc._compute_pnl_stats(srs), precision=5)
        expected = r"""
ratios        sharpe_ratio                                                -18.62702
              sharpe_ratio_standard_error                                   7.08762
              sr.tval                                                      -2.63041
              sr.pval                                                       0.00857
              kratio                                                       -9.86355
sampling      start_time                                  2001-01-03 09:31:00-05:00
              end_time                                    2001-01-10 16:00:00-05:00
              n_sampling_points                                                3127
              frequency                                                    <Minute>
              sampling_points_per_year                                   525780.125
              time_span_in_years                                            0.01991
              n_rows                                                          10471
              frac_zero                                                         0.0
              frac_nan                                                      0.70137
              frac_inf                                                          0.0
              frac_constant                                                     0.0
              num_finite_samples                                               3127
              num_finite_samples_inv                                        0.00032
              num_finite_samples_inv_dyadic_scale                               -12
              num_finite_samples_sqrt                                      55.91959
              num_finite_samples_sqrt_inv                                   0.01788
              num_finite_samples_sqrt_inv_dyadic_scale                           -6
              num_unique_values                                                3127
summary       scipy.mean                                                   -0.00005
              scipy.std                                                       0.001
              scipy.skew                                                    0.01345
              scipy.kurtosis                                                 0.0721
              null_mean_zero.tval                                          -2.63041
              null_mean_zero.pval                                           0.00857
              jensen_ratio                                                   0.7922
              count                                                          3127.0
              mean                                                         -0.00005
              std                                                             0.001
              min                                                          -0.00342
              25%                                                          -0.00072
              50%                                                          -0.00005
              75%                                                            0.0006
              max                                                            0.0035
stationarity  adf.stat                                                    -56.29862
              adf.pval                                                          0.0
              adf.used_lag                                                      0.0
              adf.nobs                                                       3126.0
              adf.critical_values_1%                                       -3.43244
              adf.critical_values_5%                                       -2.86247
              adf.critical_values_10%                                      -2.56726
              adf.ic_best                                              -34153.27372
              kpss.stat                                                     0.16171
              kpss.pval                                                         0.1
              kpss.lags                                                        16.0
              kpss.critical_values_1%                                         0.739
              kpss.critical_values_5%                                         0.463
              kpss.critical_values_10%                                        0.347
normality     omnibus_null_normal.stat                                        0.839
              omnibus_null_normal.pval                                      0.65737
              centered_gaussian.log_likelihood                          17172.00517
              centered_gaussian.centered_var                                    0.0
spectral      forecastability                                               0.00513
portfolio     annualized_mean_return                                       -7.35895
              annualized_volatility                                         0.39507
              max_drawdown                                                  0.15224
              hit_rate_point_est_(%)                                       47.71346
              hit_rate_97.50%CI_lower_bound_(%)                             45.9656
              hit_rate_97.50%CI_upper_bound_(%)                            49.46555
correlation   prediction_corr_implied_by_pnl                               -0.04706
dtype: object"""

        self.assert_equal(actual, expected, fuzzy_match=True)

    def _get_pnl_srs(self, seed=10, asset_id=100) -> pd.Series:
        price_process = carsigen.PriceProcess(seed)
        price = price_process.generate_price_series_from_normal_log_returns(
            pd.Timestamp("2001-01-03 09:30:00-05:00", tz="America/New_York"),
            pd.Timestamp("2001-01-10 16:00:00-05:00", tz="America/New_York"),
            asset_id,
        )
        rets = price.pct_change()
        rets = rets.resample("T").sum(min_count=1)
        return rets
