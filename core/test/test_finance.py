import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import core.signal_processing as sigp
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_set_weekends_to_nan(hut.TestCase):
    def test1(self) -> None:
        """
        Test for a daily frequency input.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=1)
        df = mn_process.generate_sample(
            {"start": "2020-01-01", "periods": 40, "freq": "D"}, seed=1
        )
        actual = fin.set_weekends_to_nan(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for a minutely frequency input.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=1)
        df = mn_process.generate_sample(
            {"start": "2020-01-05 23:00:00", "periods": 100, "freq": "T"}, seed=1
        )
        actual = fin.set_weekends_to_nan(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_inverse_volatility_weights(hut.TestCase):
    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        sample.rename(columns={0: "srs1", 1: "srs2"}, inplace=True)
        return sample

    @staticmethod
    def _get_output_txt(sample: pd.DataFrame, weights: pd.Series) -> str:
        sample_string = hut.convert_df_to_string(sample, index=True)
        weights_string = hut.convert_df_to_string(weights, index=True)
        txt = (
            f"Input sample:\n{sample_string}\n\n"
            f"Output weights:\n{weights_string}\n"
        )
        return txt

    def test1(self) -> None:
        """
        Test for a clean input.
        """
        sample = self._get_sample(seed=1)
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test2(self) -> None:
        """
        Test for an input with NaNs.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[1, 1] = np.nan
        sample.iloc[0:5, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test3(self) -> None:
        """
        Test for an input with all-NaN column.

        Results are not intended.
        `weights` are `0` for all-NaN columns in the input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test4(self) -> None:
        """
        Test for an all-NaN input.

        Results are not intended.
        `weights` are `0` for all-NaN columns in the input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, :] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)


class Test_compute_prices_from_rets(hut.TestCase):
    @staticmethod
    def _get_sample() -> pd.DataFrame:
        date_range = pd.date_range(start="2010-01-01", periods=40, freq="B")
        sample = pd.DataFrame(index=date_range)
        sample["price"] = np.random.uniform(low=0, high=1, size=40)
        return sample
    
    def test1(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="pct_change")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "pct_change"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)
        
    def test2(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="log_rets")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "log_rets"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)
        
    def test3(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="diff")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "diff"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)
        
    def test4(self) -> None:
        """
        Check forward returns.
        """
        sample = pd.DataFrame({"price": [1, 2, 3], "fwd_ret": [1, 0.5, np.nan]})
        sample["ret_0"] = sample.fwd_ret.shift(1)
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.ret_0, "pct_change"
        ).shift(1)
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)
        
class Test_aggregate_log_rets(hut.TestCase):
    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        sample.rename(columns={0: "srs1", 1: "srs2"}, inplace=True)
        return sample

    @staticmethod
    def _get_output_txt(
        sample: pd.DataFrame, weights: pd.Series, aggregate_log_rets: pd.Series
    ) -> str:
        sample_string = hut.convert_df_to_string(sample, index=True)
        weights_string = hut.convert_df_to_string(weights, index=True)
        aggregate_log_rets_string = hut.convert_df_to_string(
            aggregate_log_rets, index=True
        )
        txt = (
            f"Input sample:\n{sample_string}\n\n"
            f"Input weights:\n{weights_string}\n\n"
            f"Output aggregate log returns:\n{aggregate_log_rets_string}\n"
        )
        return txt

    def test1(self) -> None:
        """
        Test for a clean input.
        """
        sample = self._get_sample(seed=1)
        weights = fin.compute_inverse_volatility_weights(sample)
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test2(self) -> None:
        """
        Test for an input with NaNs.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[1, 1] = np.nan
        sample.iloc[0:5, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test3(self) -> None:
        """
        Test for an input with all-NaN column.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, 0] = np.nan
        weights = pd.Series([0.5, 0.5], index=["srs1", "srs2"], name="weights")
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test4(self) -> None:
        """
        Test for an all-NaN input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, :] = np.nan
        weights = pd.Series([0.5, 0.5], index=["srs1", "srs2"], name="weights")
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)


class Test_compute_kratio(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        """
        Test for an clean input series.
        """
        series = self._get_series(seed=1)
        actual = fin.compute_kratio(series)
        expected = -0.84551
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        """
        Test for an input with NaN values.
        """
        series = self._get_series(seed=1)
        series[:3] = np.nan
        series[7:10] = np.nan
        actual = fin.compute_kratio(series)
        expected = -0.85089
        np.testing.assert_almost_equal(actual, expected, decimal=3)


class Test_compute_drawdown(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(1)
        actual = fin.compute_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_time_under_water(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = Test_compute_time_under_water._get_series(42)
        drawdown = fin.compute_drawdown(series).rename("drawdown")
        time_under_water = fin.compute_time_under_water(series).rename(
            "time_under_water"
        )
        output = pd.concat([series, drawdown, time_under_water], axis=1)
        self.check_string(hut.convert_df_to_string(output, index=True))

    def test2(self) -> None:
        series = Test_compute_time_under_water._get_series(42)
        series.iloc[:4] = np.nan
        series.iloc[10:15] = np.nan
        series.iloc[-4:] = np.nan
        drawdown = fin.compute_drawdown(series).rename("drawdown")
        time_under_water = fin.compute_time_under_water(series).rename(
            "time_under_water"
        )
        output = pd.concat([series, drawdown, time_under_water], axis=1)
        self.check_string(hut.convert_df_to_string(output, index=True))


class Test_compute_turnover(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "D"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        ).rename("input")
        return series

    def test1(self) -> None:
        """
        Test for default arguments.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test2(self) -> None:
        """
        Test for only positive input.
        """
        positive_series = self._get_series(seed=1).abs()
        actual = fin.compute_turnover(positive_series).rename("output")
        output_df = pd.concat([positive_series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test3(self) -> None:
        """
        Test for nan_mode.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, nan_mode="ffill").rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test4(self) -> None:
        """
        Test for unit.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, unit="B").rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)


class Test_compute_average_holding_period(hut.TestCase):
    @staticmethod
    def _get_series_in_unit(seed: int, freq: str = "D") -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": freq}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series_in_unit(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_average_holding_period(series)
        expected = 1.23458
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        positive_series = self._get_series_in_unit(seed=1).abs()
        actual = fin.compute_average_holding_period(positive_series)
        expected = 1.23620
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test3(self) -> None:
        series = self._get_series_in_unit(seed=1)
        actual = fin.compute_average_holding_period(series, unit="M")
        expected = 0.05001
        np.testing.assert_almost_equal(actual, expected, decimal=3)


class Test_compute_bet_runs(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        positions = Test_compute_bet_runs._get_series(42)
        actual = fin.compute_bet_runs(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_runs')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_runs._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_runs(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_runs')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test leading zeros.
        """
        idx = pd.date_range(start="2010-01-01", periods=4, freq="D")
        positions = pd.Series([0, 0, 0, 1], index=idx)
        expected = pd.Series([0, 0, 0, 1], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test all zeros.
        """
        idx = pd.date_range(start="2010-01-01", periods=2, freq="D")
        positions = pd.Series([0, 0], index=idx)
        expected = pd.Series([0, 0], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test leading NaNs.
        """
        idx = pd.date_range(start="2010-01-01", periods=4, freq="D")
        positions = pd.Series([np.nan, np.nan, 0, 1], index=idx)
        expected = pd.Series([np.nan, np.nan, 0, 1], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test6(self) -> None:
        """
        Test series with a single value.
        """
        positions = pd.Series([10], index=[pd.Timestamp("2010-01-01")])
        expected = pd.Series([1], index=[pd.Timestamp("2010-01-01")], dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)


class Test_compute_bet_starts(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        positions = Test_compute_bet_starts._get_series(42)
        actual = fin.compute_bet_starts(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_starts._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_starts(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test zeros.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test `NaN`s.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test consecutive zeroes.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 0,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): np.nan,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)


class Test_compute_bet_ends(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        positions = Test_compute_bet_ends._get_series(42)
        actual = fin.compute_bet_ends(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_ends._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_ends(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test zeros.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test `NaN`s.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test consecutive zeroes.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): -1,
                pd.Timestamp("2010-01-02"): 0,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): 0,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): -1,
                pd.Timestamp("2010-01-02"): np.nan,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): np.nan,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)


class Test_compute_signed_bet_lengths(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        positions = pd.Series(
            [1, 1, 1, 2, -1, -4, -0.5, 0, 0, -1, 0, 1],
            index=pd.date_range(start="2010-01-01", periods=12, freq="D"),
        )
        expected_bet_ends = pd.to_datetime(
            ["2010-01-04", "2010-01-07", "2010-01-10", "2010-01-12"]
        )
        expected = pd.Series([4, -3, -1, 1], index=expected_bet_ends, dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test a single value.
        """
        positions = pd.Series([1], index=[pd.Timestamp("2010-01-01")])
        # Notice the int to float data type change.
        expected = pd.Series([1], index=[pd.Timestamp("2010-01-01")], dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([np.nan, np.nan], index=idx)
        expected = pd.Series(index=idx).dropna()
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test6(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, np.nan, 0], index=idx)
        expected = pd.Series(
            [2], index=pd.to_datetime(["2010-01-02"]), dtype=float
        )
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test7(self) -> None:
        """
        Test NaNs
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, np.nan, np.nan], index=idx)
        expected = pd.Series(
            [3], index=pd.to_datetime(["2010-01-03"]), dtype=float
        )
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test8(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([0, 0], index=idx)
        expected = pd.Series(index=idx).dropna()
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test9(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([0, 1, 0], index=idx)
        expected = pd.Series([1.0], index=[pd.Timestamp("2010-01-02")])
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test10(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, 0, 0], index=idx)
        expected = pd.Series([1.0], index=[pd.Timestamp("2010-01-01")])
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test11(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = 0
        positions.iloc[-4:] = 0
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test12(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = 0
        positions.iloc[-4:] = 0
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)


class Test_compute_returns_per_bet(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        """
        Test for clean input series.
        """
        log_rets = self._get_series(42)
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        actual = fin.compute_returns_per_bet(positions, log_rets)
        rets_pos = pd.concat({"pos": positions, "rets": log_rets}, axis=1)
        output_str = (
            f"{prnt.frame('rets_pos')}\n"
            f"{hut.convert_df_to_string(rets_pos, index=True)}\n"
            f"{prnt.frame('rets_per_bet')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        """
        Test for input series with NaNs and zeros.
        """
        log_rets = self._get_series(42)
        log_rets.iloc[6:12] = np.nan
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = 0
        actual = fin.compute_returns_per_bet(positions, log_rets)
        rets_pos = pd.concat({"pos": positions, "rets": log_rets}, axis=1)
        output_str = (
            f"{prnt.frame('rets_pos')}\n"
            f"{hut.convert_df_to_string(rets_pos, index=True)}\n"
            f"{prnt.frame('rets_per_bet')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test for short input series.
        """
        idx = pd.to_datetime(
            [
                "2010-01-01",
                "2010-01-03",
                "2010-01-05",
                "2010-01-06",
                "2010-01-10",
                "2010-01-12",
            ]
        )
        log_rets = pd.Series([1.0, 2.0, 3.0, 5.0, 7.0, 11.0], index=idx)
        positions = pd.Series([1.0, 2.0, 0.0, 1.0, -3.0, -2.0], index=idx)
        actual = fin.compute_returns_per_bet(positions, log_rets)
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-03"): 5.0,
                pd.Timestamp("2010-01-06"): 5.0,
                pd.Timestamp("2010-01-12"): -43.0,
            }
        )
        pd.testing.assert_series_equal(actual, expected)
