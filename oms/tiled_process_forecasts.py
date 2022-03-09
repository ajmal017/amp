"""
Import as:

import oms.tiled_process_forecasts as otiprfor
"""

import datetime
import logging
from typing import Union

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import oms.portfolio as omportfo
import oms.process_forecasts as oprofore

_LOG = logging.getLogger(__name__)


# market_data: mdata.MarketData,
# strategy_id: str,
# account: str,
# timestamp_col: str,

# Build broker and portfolio objects.
# broker = ombroker.SimulatedBroker(
#     strategy_id, account, market_data, timestamp_col=timestamp_col
# )
# mark_to_market_col = "price"
# pricing_method = "twap"
# portfolio = omportfo.DataFramePortfolio(
#     broker,
#     mark_to_market_col,
#     pricing_method,
# )


async def run_tiled_process_forecasts(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
    asset_id_col: str,
    returns_col: str,
    volatility_col: str,
    prediction_col: str,
    process_forecasts_config: cconfig.Config,
    portfolio: omportfo.AbstractPortfolio,
) -> None:
    columns = [asset_id_col, returns_col, volatility_col, prediction_col]
    tiles = hparque.yield_parquet_tiles_by_year(
        file_name,
        start_date,
        end_date,
        columns,
    )
    num_years = end_date.year - start_date.year + 1
    for tile in tqdm(tiles, total=num_years):
        # Convert the `from_parquet()` dataframe to a dataflow-style dataframe.
        df = process_parquet_read_df(
            tile,
            asset_id_col,
        )
        prediction_df = df[prediction_col]
        volatility_df = df[volatility_col]

        await oprofore.process_forecasts(
            prediction_df,
            volatility_df,
            portfolio,
            process_forecasts_config,
        )
    return portfolio


def process_parquet_read_df(
    df: pd.DataFrame,
    asset_id_col: str,
) -> pd.DataFrame:
    """"""
    # TODO(Paul): Maybe wrap `hparque.from_parquet()`.
    hdbg.dassert_isinstance(asset_id_col, str)
    hdbg.dassert_in(asset_id_col, df.columns)
    # Parquet uses categoricals; cast the asset ids to their native integers.
    df[asset_id_col] = df[asset_id_col].astype("int64")
    # Check that the asset id column is now an integer column.
    hpandas.dassert_series_type_is(df[asset_id_col], np.int64)
    # If a (non-asset id) column can be represented as an int, then do so.
    df = df.rename(columns=_maybe_cast_to_int)
    # Convert from long format to column-multiindexed format.
    df = df.pivot(columns=asset_id_col)
    # NOTE: the asset ids may already be sorted and so this may not be needed.
    df.sort_index(axis=1, level=-2, inplace=True)
    return df


def _maybe_cast_to_int(string: str) -> Union[str, int]:
    hdbg.dassert_isinstance(string, str)
    try:
        val = int(string)
    except ValueError:
        val = string
    return val
