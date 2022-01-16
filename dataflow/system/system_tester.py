"""
Import as:

import dataflow.system.system_tester as dtfsysytes
"""

import logging
from typing import List, Union

import pandas as pd

import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class SystemTester:
    """
    Test a System.
    """

    def get_events_signature(self, events) -> List[str]:
        # TODO(gp): Use events.to_str()
        actual = ["# event signature=\n"]
        events_as_str = "\n".join(
            [
                event.to_str(
                    include_tenths_of_secs=False,
                    include_wall_clock_time=False,
                )
                for event in events
            ]
        )
        actual.append("events_as_str=\n%s" % events_as_str)
        actual = "\n".join(actual)
        return actual

    def get_portfolio_signature(self, portfolio) -> List[str]:
        actual = ["\n# portfolio signature=\n"]
        actual.append(str(portfolio))
        actual = "\n".join(actual)
        return actual

    def get_research_pnl_signature(
        self,
        result_bundle,
        *,
        price_col: str,
        returns_col: str,
        volatility_col: str,
        volatility_adjusted_returns_col: str,
        prediction_col: str,
    ):
        actual = ["\n# result_bundle.result_df signature=\n"]
        #
        result_df = result_bundle.result_df
        # Price.
        price = result_df[price_col]
        self._append(actual, "price", price)
        # Returns.
        returns = result_df[returns_col]
        self._append(actual, "returns", returns)
        # Volatility.
        volatility = result_df[volatility_col]
        self._append(actual, "volatility", volatility)
        # Volatility-adjusted returns.
        volatility_adjusted_returns = result_df[volatility_adjusted_returns_col]
        self._append(
            actual, "volatility adjusted returns", volatility_adjusted_returns
        )
        # Prediction.
        predictions = result_df[prediction_col]
        self._append(actual, "predictions", predictions)
        # Research PnL.
        research_pnl = (
            predictions.shift(2)
            .multiply(volatility_adjusted_returns)
            .sum(axis=1, min_count=1)
        )
        self._append(actual, "research pnl", research_pnl)
        actual = "\n".join(actual)
        return actual

    def compute_run_signature(
        self,
        dag_runner,
        portfolio,
        result_bundle,
        *,
        price_col: str,
        returns_col: str,
        volatility_col: str,
        volatility_adjusted_returns_col: str,
        prediction_col: str,
    ) -> str:
        # Check output.
        actual = []
        #
        events = dag_runner.events
        actual.append(self.get_events_signature(events))
        actual.append(self.get_portfolio_signature(portfolio))
        actual.append(
            self.get_research_pnl_signature(
                result_bundle,
                price_col=price_col,
                returns_col=returns_col,
                volatility_col=volatility_col,
                volatility_adjusted_returns_col=volatility_adjusted_returns_col,
                prediction_col=prediction_col,
            )
        )
        actual = "\n".join(map(str, actual))
        return actual

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        list_.append(f"{label}=\n{data_str}")
