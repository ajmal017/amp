"""
Import as:

import oms.mr_market as omrmark
"""

# TODO(gp): Not sure I get the joke of mr_market.py. Mean-reversion market?
# map-reduce market?
# https://en.wikipedia.org/wiki/Mr._Market
# -> order_processor.py

import logging
from typing import Any, Dict, Optional, Union

import pandas as pd

import helpers.dbg as hdbg
import helpers.hasyncio as hasynci
import helpers.printing as hprint
import helpers.sql as hsql
import oms.broker as ombroker
import oms.oms_db as oomsdb
import oms.order as omorder

_LOG = logging.getLogger(__name__)

# #############################################################################
# Order processor
# #############################################################################


async def order_processor(
    db_connection: hsql.DbConnection,
    delay_to_accept_in_secs: float,
    delay_to_fill_in_secs: float,
    broker: ombroker.AbstractBroker,
    termination_condition: Union[pd.Timestamp, int],
    *,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
    current_positions_table_name: str = oomsdb.CURRENT_POSITIONS_TABLE_NAME,
    poll_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Mock the behavior of part of the actual implemented broker and of the
    market.

    This coroutine:
    - polls a table of the DB for submitted orders
    - updates the accepted orders DB table
    - updates the current positions DB table

    :param delay_to_accept_in_secs: delay after the order is submitted to update
        the accepted orders table
    :param delay_to_fill_in_secs: delay after the order is accepted to update the
        position table with the filled positions
    :param termination_condition: when to terminate polling the table of submitted
        order.
        - pd.timestamp: when this object should stop checking for orders. Be
          careful since this can create deadlocks if this timestamp is set after
          the broker stops submitting orders
        - int: number of orders to accept before shut down
    """
    get_wall_clock_time = broker.market_data_interface.get_wall_clock_time
    if poll_kwargs is None:
        poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
    #
    target_list_id = 0
    while True:
        wall_clock_time = get_wall_clock_time()
        # Check whether we should exit or continue.
        if isinstance(termination_condition, pd.Timestamp):
            exit = wall_clock_time >= termination_condition
        elif isinstance(termination_condition, int):
            exit = target_list_id >= termination_condition
        else:
            raise ValueError(
                "Invalid termination_condition=%s type=%s"
                % (termination_condition, str(type(termination_condition)))
            )
        if exit:
            _LOG.debug(
                "Reached the end: "
                + hprint.to_str(
                    "target_list_id wall_clock_time termination_condition"
                )
            )
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
        # TODO(gp): For now we accept only one order list.
        hdbg.dassert_eq(diff_num_rows, 1)
        file_name = df.tail(1).squeeze()["filename"]
        _LOG.debug("file_name=%s", file_name)
        # Wait after the submission was parsed.
        hdbg.dassert_lt(0, delay_to_accept_in_secs)
        await hasynci.sleep(delay_to_accept_in_secs, get_wall_clock_time)
        wall_clock_time = get_wall_clock_time()
        # Write in `accepted_orders_table_name` to acknowledge the orders.
        trade_date = wall_clock_time.date()
        success = True
        txt = f"""
        strategyid,SAU1
        targetlistid,{target_list_id}
        tradedate,{trade_date}
        instanceid,1
        filename,{file_name}
        timestamp_processed,{wall_clock_time}
        timestamp_db,{wall_clock_time}
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
        # Wait after the orders were accepted.
        hdbg.dassert_lt(0, delay_to_fill_in_secs)
        await hasynci.sleep(delay_to_fill_in_secs, get_wall_clock_time)
        # Wait until the max order deadline to return fills.
        _LOG.debug("Executing query for unfilled submitted orders...")
        query = f"""
            SELECT filename, timestamp_db, orders_as_txt
                FROM {submitted_orders_table_name}
                ORDER BY timestamp_db"""
        df = hsql.execute_query_to_df(db_connection, query)
        _LOG.debug("df=\n%s", hprint.dataframe_to_str(df))
        hdbg.dassert_eq(file_name, df.tail(1).squeeze()["filename"])
        orders_as_txt = df.tail(1).squeeze()["orders_as_txt"]
        orders = omorder.orders_from_string(orders_as_txt)
        fulfillment_deadline = max([order.end_timestamp for order in orders])
        _LOG.debug("Order fulfillment deadline=%s", fulfillment_deadline)
        # TODO(Paul): Don't wait past the deadline.
        await hasynci.wait_until(fulfillment_deadline, get_wall_clock_time)
        # Get the fills.
        _LOG.debug("Getting fills.")
        fills = broker.get_fills()
        _LOG.debug("Received %i fills", len(fills))
        # Update current positions based on fills.
        for fill in fills:
            id_ = fill.order.order_id
            trade_date = fill.timestamp.date()
            wall_clock_time = get_wall_clock_time()
            asset_id = fill.order.asset_id
            num_shares = fill.num_shares
            cost = fill.price * fill.num_shares
            # #################################################################
            # Get the current positions for `asset_id`.
            query = []
            query.append(f"SELECT * FROM {current_positions_table_name}")
            query.append(
                f"WHERE account='candidate' AND tradedate='{trade_date}' AND asset_id={asset_id}"
            )
            query = "\n".join(query)
            _LOG.debug("query=%s", query)
            positions_df = hsql.execute_query_to_df(db_connection, query)
            hdbg.dassert_lte(positions_df.shape[0], 1)
            _LOG.debug("positions_df=%s", hprint.dataframe_to_str(positions_df))
            # Delete the row from the positions table.
            query = []
            query.append(f"DELETE FROM {current_positions_table_name}")
            query.append(
                f"WHERE account='candidate' AND tradedate='{trade_date}' AND asset_id={asset_id}"
            )
            query = "\n".join(query)
            _LOG.debug("query=%s", query)
            deletions = hsql.execute_query(db_connection, query)
            deletions = deletions or 0
            _LOG.debug("Num deletions=%d", deletions)
            # Update the row and insert into the positions table.
            # TODO(Paul): Need to handle BOD.
            if not positions_df.empty:
                row = positions_df.squeeze()
                hdbg.dassert_isinstance(row, pd.Series)
                row["id"] = int(id_)
                row["tradedate"] = trade_date
                row["timestamp_db"] = wall_clock_time
                row["current_position"] += num_shares
                row["net_cost"] += cost
                row["asset_id"] = int(row["asset_id"])
            else:
                txt = f"""
                strategyid,SAU1
                account,candidate
                id,{id_}
                tradedate,{trade_date}
                timestamp_db,{wall_clock_time}
                asset_id,{asset_id}
                target_position,0
                current_position,{num_shares}
                open_quantity,0
                net_cost,{cost}
                bod_position,0
                bod_price,0
                """
                row = hsql.csv_to_series(txt, sep=",")
            row = row.convert_dtypes()
            _LOG.debug("Insert row is=%s", hprint.dataframe_to_str(row))
            hsql.execute_insert_query(
                db_connection, row, current_positions_table_name
            )
