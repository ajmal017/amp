import logging

import pandas as pd

_LOG = logging.getLogger(__name__)

from typing import Optional, Union


import dataflow.model.forecast_mixer as dtfmofomix
import dataflow.model.utils as dtfmodutil
import helpers.hdbg as hdbg
import helpers.hparquet as hparque


def load_and_mix_by_year(
    file_name: str,
    start_year: int,
    end_year: int,
    asset_id_col: str,
    returns_col: str,
    volatility_col: str,
    feature_cols: list,
    weights: pd.DataFrame,
    target_gmv: float,
    dollar_neutrality: str,
) -> pd.DataFrame:
    """
    Wrap `load_mix_evaluate()`. Loads tiles by year and concats metrics.
    """
    years = list(range(start_year, end_year + 1))
    dfs = []
    for year in years:
        filters = []
        and_condition = ("year", "==", year)
        filters.append(and_condition)
        df = load_mix_evaluate(
            file_name,
            asset_id_col=asset_id_col,
            returns_col=returns_col,
            volatility_col=volatility_col,
            feature_cols=feature_cols,
            filters=filters,
            target_gmv=target_gmv,
            dollar_neutrality=dollar_neutrality,
            weights=weights,
        )
        dfs.append(df)
    bar_metrics = pd.concat(dfs)
    return bar_metrics


def load_mix_evaluate(
    file_name: str,
    filters: list,
    asset_id_col: str,
    returns_col: str,
    volatility_col: str,
    feature_cols: list,
    weights: pd.DataFrame,
    target_gmv: Optional[float] = None,
    dollar_neutrality: str = "no_constraint",
) -> pd.DataFrame:
    """
    Load a tiled backtest, mix features, and evaluate the portfolio.

    :param file_name: parquet file containing tiled backtest results
    :param filters: parquet file filters
    :param asset_id_col: name of asset id column
    :param returns_col: name of realize returns column
    :param volatility_col: name of volatility forecast column
    :param feature_cols: names of predictive feature columns
    :param weights: feature weights, indexed by feature column name; one
        set of weights per column
    :param target_gmv: target gmv for forecast evaluation
    :param dollar_neutrality: dollar neutrality constraint for forecast
        evaluation, e.g.,
    :return: a portfolio bar metrics dataframe (see
        dtfmofomix.get_portfolio_bar_metrics_dataframe() for an example).
    """
    hdbg.dassert_isinstance(weights, pd.DataFrame)
    # Int col names may be written as strings in parquet. This interprets
    # col names as ints if possible.
    casted_feature_cols = [_maybe_cast_to_int(x) for x in feature_cols]
    hdbg.dassert_set_eq(weights.index, casted_feature_cols)
    # Load parquet tile.
    columns = [asset_id_col, returns_col, volatility_col] + feature_cols
    parquet_df = hparque.from_parquet(
        file_name,
        columns=columns,
        filters=filters,
    )
    # Convert the `from_parquet()` dataframe to a dataflow-style dataframe.
    df = dtfmodutil.process_parquet_read_df(
        parquet_df,
        asset_id_col,
    )
    # For each weight column, mix the features using those weights into a
    # single forecast and generate the corresponding portfolio.
    fm =  dtfmofomix.ForecastMixer(
        returns_col=returns_col,
        volatility_col=volatility_col,
        prediction_cols=casted_feature_cols,
    )
    bar_metrics = fm.generate_portfolio_bar_metrics_df(
        df,
        weights,
        target_gmv=target_gmv,
        dollar_neutrality=dollar_neutrality,
    )
    return bar_metrics


# TODO(Paul): Factor this helper out.
def _maybe_cast_to_int(string: str) -> Union[str, int]:
    hdbg.dassert_isinstance(string, str)
    try:
        val = int(string)
    except ValueError:
        val = string
    return val
