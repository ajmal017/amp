import io
import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as casgen
import core.config as cconfig
import core.dataflow.nodes.test.helpers as cdnth
import core.dataflow.nodes.transformers as cdnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestSeriesToSeriesTransformer(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = cdnt.SeriesToSeriesTransformer("sklearn", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` call.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = cdnt.SeriesToSeriesTransformer("sklearn", **config.to_dict())
        expected, actual = cdnth.get_fit_predict_outputs(data, node)
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=342)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=134
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        realization = np.exp(0.1 * realization.cumsum())
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["close", "volume"])
        return data


# class TestGroupedColDfToDfColTransformer(hut.TestCase):
#    def test1(self) -> None:
#        pass


class TestSeriesToDfTransformer(hut.TestCase):
    def test1(self) -> None:
        def add_lags(srs: pd.Series, num_lags: int) -> pd.DataFrame:
            lags = []
            for lag in range(0, num_lags):
                lags.append(srs.shift(lag).rename("lag_" + str(lag)))
            out_df = pd.concat(lags, axis=1)
            return out_df

        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": (),
                "transformer_func": add_lags,
                "transformer_kwargs": {"num_lags": 3, },
            }
        )
        node = cdnt.SeriesToDfTransformer("add_lags", **config.to_dict())
        actual = node.fit(data)["df_out"]
        expected_txt = """
,lag_0,lag_0,lag_1,lag_1,lag_2,lag_2,close,close,volume,volume
,MN0,MN1,MN0,MN1,MN0,MN1,MN0,MN1,MN0,MN1
2016-01-04 09:30:00,94.7,100.2,,,,,94.7,100.2,30000,40000
2016-01-04 09:31:00,94.9,100.25,94.7,100.2,,,94.9,100.25,35000,44000
2016-01-04 09:32:00,95.35,100.23,94.9,100.25,94.7,100.2,95.35,100.23,40000,45000
"""
        expected = pd.read_csv(io.StringIO(expected_txt), index_col=0, parse_dates=True,
                               header=[0, 1])
        hut.compare_df(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        txt = """
,close,close,volume,volume
datetime,MN0,MN1,MN0,MN1
2016-01-04 09:30:00,94.70,100.20,30000,40000
2016-01-04 09:31:00,94.90,100.25,35000,44000
2016-01-04 09:32:00,95.35,100.23,40000,45000
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True, header=[0, 1])
        return df


class TestSeriesToSeriesTransformer(hut.TestCase):
    def test1(self) -> None:
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = cdnt.SeriesToSeriesTransformer("add_lags", **config.to_dict())
        actual = node.fit(data)["df_out"]
        expected_txt = """
,ret_0,ret_0,close,close,volume,volume
,MN0,MN1,MN0,MN1,MN0,MN1
2016-01-04 09:30:00,,,100.0,100.0,30000,40000
2016-01-04 09:31:00,0.05,-0.02,105.0,98.0,35000,44000
2016-01-04 09:32:00,-0.5,-0.5,52.5,49.0,40000,45000
"""
        expected = pd.read_csv(io.StringIO(expected_txt), index_col=0, parse_dates=True,
                               header=[0, 1])
        # NOTE: `hut.compare_df()` is unstable due to round-off errors.
        np.testing.assert_allclose(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        txt = """
,close,close,volume,volume
datetime,MN0,MN1,MN0,MN1
2016-01-04 09:30:00,100.00,100.00,30000,40000
2016-01-04 09:31:00,105.00,98.00,35000,44000
2016-01-04 09:32:00,52.50,49.00,40000,45000
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True, header=[0, 1])
        return df


class TestFunctionWrapper(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        data = self._get_df()

        def multiply(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
            product = (df[col1] * df[col2]).rename("pv")
            return product.to_frame()

        config = cconfig.get_config_from_nested_dict(
            {
                "func": multiply,
                "func_kwargs": {
                    "col1": "close",
                    "col2": "volume",
                },
            }
        )
        node = cdnt.FunctionWrapper("sklearn", **config.to_dict())
        actual = node.fit(data)["df_out"]
        txt = """
datetime,pv
2016-01-04 09:30:00,1.769e+08
2016-01-04 09:31:00,3.316e+07
2016-01-04 09:32:00,3.999e+07
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        pd.testing.assert_frame_equal(actual, expected, rtol=1e-2)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        """
        Return a df without NaNs.
        """
        txt = """
datetime,close,volume
2016-01-04 09:30:00,94.7,1867590
2016-01-04 09:31:00,94.98,349119
2016-01-04 09:32:00,95.33,419479
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class TestTwapVwapComputer(hut.TestCase):
    def test1(self) -> None:
        """
        Test building 5-min TWAP/VWAP bars from 1-min close/volume bars.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "rule": "5T",
                "price_col": "close",
                "volume_col": "volume",
            }
        )
        node = cdnt.TwapVwapComputer("twapvwap", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` call.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "rule": "5T",
                "price_col": "close",
                "volume_col": "volume",
            }
        )
        node = cdnt.TwapVwapComputer("twapvwap", **config.to_dict())
        expected, actual = cdnth.get_fit_predict_outputs(data, node)
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate AR(1) returns and Poisson volume.
        """
        date_range_kwargs = {
            "start": "2001-01-04 09:30:00",
            "end": "2001-01-04 10:00:00",
            "freq": "T",
        }
        ar_params = [0.5]
        arma_process = casgen.ArmaProcess(ar_params, [])
        rets = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs,
            scale=1,
            burnin=0,
            seed=100,
        )
        prices = np.exp(0.25 * rets.cumsum())
        prices.name = "close"
        poisson_process = casgen.PoissonProcess(mu=100)
        volume = poisson_process.generate_sample(
            date_range_kwargs=date_range_kwargs,
            seed=100,
        )
        volume.name = "volume"
        df = pd.concat([prices, volume], axis=1)
        return df


class TestMultiindexTwapVwapComputer(hut.TestCase):
    def test1(self) -> None:
        """
        Test building 5-min TWAP/VWAP bars from 1-min close/volume bars.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "rule": "5T",
                "price_col_group": ("close",),
                "volume_col_group": ("volume",),
                "out_col_group": (),
            }
        )
        node = cdnt.MultiindexTwapVwapComputer("twapvwap", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` call.
        """
        data = self._get_data()
        config = cconfig.get_config_from_nested_dict(
            {
                "rule": "5T",
                "price_col_group": ("close",),
                "volume_col_group": ("volume",),
                "out_col_group": (),
            }
        )
        node = cdnt.MultiindexTwapVwapComputer("twapvwap", **config.to_dict())
        expected, actual = cdnth.get_fit_predict_outputs(data, node)
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate AR(1) returns and Poisson volume.
        """
        date_range_kwargs = {
            "start": "2001-01-04 09:30:00",
            "end": "2001-01-04 10:00:00",
            "freq": "T",
        }
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=402)
        rets = mn_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=343
        )
        rets = rets.rename(columns=lambda x: "MN" + str(x))
        prices = np.exp(0.1 * rets.cumsum())
        poisson_process = casgen.PoissonProcess(mu=100)
        volume_srs = poisson_process.generate_sample(
            date_range_kwargs=date_range_kwargs,
            seed=100,
        )
        volume = pd.DataFrame(index=volume_srs.index, columns=rets.columns)
        for col in volume.columns:
            volume[col] = volume_srs
        df = pd.concat([prices, volume], axis=1, keys=["close", "volume"])
        return df
