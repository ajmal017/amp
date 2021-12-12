"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio
from typing import Optional

import helpers.datetime_ as hdateti
import helpers.sql as hsql
import market_data.market_data_interface as mdmadain
import market_data.market_data_interface_example as mdmdinex
import oms.broker as ombroker
import oms.oms_db as oomsdb


def get_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data_interface: Optional[mdmadain.AbstractMarketDataInterface] = None,
) -> ombroker.Broker:
    """
    Build a simulated broker example.
    """
    # Build the market data interface.
    if market_data_interface is None:
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(event_loop)
    # Build the broker.
    strategy_id = "SAU1"
    account = "candidate"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    broker = ombroker.Broker(
        strategy_id, account, market_data_interface, get_wall_clock_time
    )
    return broker


def get_mocked_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    *,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
) -> ombroker.Broker:
    """
    Build a mocked broker.
    """
    # Build the market data interface.
    (
        market_data_interface,
        _,
    ) = mdmdinex.get_replayed_time_market_data_interface_example2(event_loop)
    # Build the broker.
    strategy_id = "SAU1"
    account = "candidate"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    broker = ombroker.MockedBroker(
        strategy_id,
        account,
        market_data_interface,
        get_wall_clock_time,
        db_connection=db_connection,
        submitted_orders_table_name=submitted_orders_table_name,
        accepted_orders_table_name=accepted_orders_table_name,
    )
    return broker
