"""
Import as:

import oms.mr_market as omrmark
"""

import asyncio
import logging
from typing import Any, Dict

import pandas as pd

import helpers.dbg as hdbg
import helpers.printing as hprint
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
    end_timestamp: pd.Timestamp,
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
    get_wall_clock_time = broker.market_data_interface.get_wall_clock_time
    target_list_id = 0
    while True:
        if get_wall_clock_time() > end_timestamp:
            break
        # Wait for orders to be written in `submitted_orders_table_name`.
        diff_num_rows = await hsql.wait_for_change_in_number_of_rows(
            db_connection, submitted_orders_table_name, poll_kwargs
        )
        _LOG.debug("diff_num_rows=%s", diff_num_rows)
        # Extract the latest file_name after order submission is complete.
        _LOG.debug("Executing query for submitted orders filename...")
        query = f"""
            SELECT filename, timestamp_db
                FROM {submitted_orders_table_name}
                ORDER BY timestamp_db"""
        df = hsql.execute_query_to_df(db_connection, query)
        _LOG.debug("df=\n%s", hprint.dataframe_to_str(df))
        hdbg.dassert_lte(
            diff_num_rows,
            len(df),
            1,
            "There are not enough new rows in df=\n%s",
            hprint.dataframe_to_str(df),
        )
        # TODO(gp): For now we accept only one orderlist.
        hdbg.dassert_eq(diff_num_rows, 1)
        file_name = df.tail(1).squeeze()["filename"]
        hdbg.dassert_lt(0, delay_to_accept_in_secs)
        await asyncio.sleep(delay_to_accept_in_secs)
        _LOG.debug("file_name=%s", file_name)
        # Write in `accepted_orders_table_name` to acknowledge the orders.
        # NOTE: This is where we use `file_name`.
        timestamp_db = get_wall_clock_time()
        trade_date = timestamp_db.date()
        success = True
        txt = f"""
        strategyid,SAU1
        targetlistid,{target_list_id}
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
        target_list_id += 1
        row = hsql.csv_to_series(txt, sep=",")
        hsql.execute_insert_query(db_connection, row, accepted_orders_table_name)
        # Wait.
        hdbg.dassert_lt(0, delay_to_fill_in_secs)
        await asyncio.sleep(delay_to_fill_in_secs)
        # Get the fills.
        _LOG.debug("Getting fills.")
        fills = broker.get_fills(timestamp_db)
        _LOG.debug("Received %i fills", len(fills))
        # Update current positions based on fills.
        for fill in fills:
            id_ = fill.order.order_id
            trade_date = fill.timestamp.date()
            timestamp_db = get_wall_clock_time()
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
