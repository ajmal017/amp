"""
Import as:

import oms.mr_market as omrmark
"""

import asyncio
import logging
from typing import Any, Dict

import helpers.dbg as hdbg
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)
import oms.broker as ombroker
import oms.oms_db as oomsdb

# #############################################################################
# Order processor
# #############################################################################

# This mocks the behavior of the actual broker + market.


async def order_processor(
    db_connection: hsql.DbConnection,
    poll_kwargs: Dict[str, Any],
    delay_to_accept_in_secs: float,
    delay_to_fill_in_secs: float,
    broker: ombroker.AbstractBroker,
    *,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
    current_positions_table_name: str = oomsdb.CURRENT_POSITIONS_TABLE_NAME,
) -> None:
    """
    A coroutine that:

    - polls for submitted orders
    - updates the accepted orders table
    - updates the current positions table

    :param delay_to_accept_in_secs: how long to wait after the order is submitted
        to update the accepted orders table
    """
    # Wait for orders to be written in `submitted_orders_table_name`.
    await hsql.wait_for_change_in_number_of_rows(
        db_connection, submitted_orders_table_name, poll_kwargs
    )
    hdbg.dassert_lt(0, delay_to_accept_in_secs)
    await asyncio.sleep(delay_to_accept_in_secs)
    # Extract the latest file_name after order submission is complete.
    _LOG.debug("Executing query for submitted orders filename...")
    query = (
        f"SELECT t.filename, t.timestamp_db "
        f"from {submitted_orders_table_name} t "
        f"inner join ("
        f"    select filename, max(timestamp_db) as MaxDate "
        f"    from {submitted_orders_table_name} "
        f"    group by filename "
        f") tm on t.filename = tm.filename and t.timestamp_db = tm.MaxDate"
    )
    df = hsql.execute_query_to_df(db_connection, query)
    _LOG.debug("df=\n%s", df)
    hdbg.dassert_eq(len(df), 1)
    file_name = df.squeeze()["filename"]
    _LOG.debug("file_name=%s", file_name)
    # Write in `accepted_orders_table_name` to acknowledge the orders.
    # NOTE: This is where we use `file_name`.
    timestamp_db = broker.market_data_interface.get_wall_clock_time()
    trade_date = timestamp_db.date()
    success = True
    txt = f"""
    strategyid,SAU1
    targetlistid,1
    tradedate,{trade_date}
    instanceid,1
    filename,{file_name}
    timestamp_processed,{timestamp_db}
    timestamp_db,{timestamp_db}
    target_count,1
    changed_count,0
    unchanged_count,0
    cancel_count,0
    success,{success}
    reason,Foobar
    """
    row = hsql.csv_to_series(txt, sep=",")
    hsql.execute_insert_query(db_connection, row, accepted_orders_table_name)
    # Wait.
    hdbg.dassert_lt(0, delay_to_fill_in_secs)
    await asyncio.sleep(delay_to_fill_in_secs)
    # Get the fills.
    _LOG.debug("Getting philz.")
    fills = broker.get_fills(timestamp_db)
    _LOG.debug("Received %i fills", len(fills))
    # Update current positions based on fills.
    for fill in fills:
        id_ = fill.order.order_id
        trade_date = fill.timestamp.date()
        timestamp_db = broker.market_data_interface.get_wall_clock_time()
        asset_id = fill.order.asset_id
        # TODO: query current positions table, then modify based on fills
        txt = f"""
        strategyid,SAU1
        account,paper
        id,{id_}
        tradedate,{trade_date}
        timestamp_db,{timestamp_db}
        asset_id,{asset_id}
        target_position,0
        current_position,0
        open_quantity,0
        net_cost,0
        bod_position,0
        bod_price,0
        """
        row = hsql.csv_to_series(txt, sep=",")
        hsql.execute_insert_query(
            db_connection, row, current_positions_table_name
        )
