"""
Import as:

import oms.tiled_process_forecasts as otiprfor
"""

import datetime
import logging

import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import market_data as mdata
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
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


def get_portfolio(market_data: mdata.MarketData) -> omportfo.AbstractPortfolio:
    strategy_id = "strategy"
    account = "account"
    timestamp_col = "end_datetime"
    mark_to_market_col = "close"
    pricing_method = "twap"
    initial_holdings = pd.Series([0], [-1])
    column_remap = {
        "bid": "bid",
        "ask": "ask",
        "price": "close",
        "midpoint": "midpoint",
    }
    portfolio = oporexam.get_DataFramePortfolio_example2(
        strategy_id,
        account,
        market_data,
        timestamp_col,
        mark_to_market_col,
        pricing_method,
        initial_holdings,
        column_remap=column_remap,
    )
    return portfolio


async def run_tiled_process_forecasts(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
    asset_id_col: str,
    returns_col: str,
    prediction_col: str,
    volatility_col: str,
    portfolio: omportfo.AbstractPortfolio,
    process_forecasts_config: cconfig.Config,
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
        # Parquet reads asset_ids as categoricals; convert to ints.
        tile = hpandas.convert_col_to_int(tile, asset_id_col)
        # Convert any dataframe columns to ints if possible.
        tile = tile.rename(columns=hparque.maybe_cast_to_int)
        # Extract the prediction and volatility data as dataframes with columns
        # equal to asset ids.
        prediction_df = tile[prediction_col].pivot(columns=asset_id_col)
        volatility_df = tile[volatility_col].pivot(columns=asset_id_col)
        await oprofore.process_forecasts(
            prediction_df,
            volatility_df,
            portfolio,
            process_forecasts_config,
        )
    return portfolio
