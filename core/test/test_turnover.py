import logging

import numpy as np
import pandas as pd

import core.turnover as coturnov
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestComputeTurn(hunitest.TestCase):
    def test1(self) -> None:
        df = pd.DataFrame(
            [
                [0.035800, 1.7276, -1],
                [0.019700, 1.2265, -1],
                [0.011828, 0.8651, -1],
                [0.007924, 0.5893, -1],
                [0.005678, 0.4399, -1],
                [0.002616, 0.3795, -1],
                [0.000883, 0.3581, -1],
            ],
            [1, 2, 3, 4, 5, 6, 7],
            ["var", "turn", "weight"],
        )
        turn = coturnov.compute_turnover(df)
        np.testing.assert_almost_equal(turn, 1.33146, decimal=3)
