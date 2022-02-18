import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import core.finance.kratio as cfinkrat
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestComputeKratio(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test for an clean input series.
        """
        series = self._get_series(seed=1)
        actual = cfinkrat.compute_kratio(series)
        expected = -0.84551
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        """
        Test for an input with NaN values.
        """
        series = self._get_series(seed=1)
        series[:3] = np.nan
        series[7:10] = np.nan
        actual = cfinkrat.compute_kratio(series)
        expected = -0.85089
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = carsigen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series
