"""
Import as:

import oms.portfolio_example as oporexam
"""

import logging

import pandas as pd

import core.dataflow.price_interface as cdtfprint
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def get_portfolio_example1(
    price_interface: cdtfprint.AbstractPriceInterface,
    initial_timestamp: pd.Timestamp,
) -> omportfo.Portfolio:
    strategy_id = "st1"
    account = "paper"
    asset_id_column = "asset_id"
    # price_column = "midpoint"
    mark_to_market_col = "price"
    timestamp_col = "end_datetime"
    #
    initial_cash = 1e6
    portfolio = omportfo.Portfolio.from_cash(
        strategy_id,
        account,
        #
        price_interface,
        asset_id_column,
        mark_to_market_col,
        timestamp_col,
        #
        initial_cash,
        initial_timestamp,
    )
    return portfolio
