"""
Import as:

import oms.broker as ombroker
"""

import collections
import logging
from typing import Any, Dict, List

import pandas as pd

import core.dataflow.price_interface as cdtfprint
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import oms.order as omorder

_LOG = logging.getLogger(__name__)


# def set_wall_clock_time():
#
# def get_wall_clock_time()


class Fill:
    """
    Represent an order fill.

    An order can be filled partially or completely. Each fill can happen at
    different prices.

    The simplest case is for an order to be completely filled (e.g., at the end of
    its VWAP execution window) at a single price. In this case a single `Fill`
    object can represent the execution.
    """

    _fill_id = 0

    def __init__(
        self,
        order: omorder.Order,
        timestamp: pd.Timestamp,
        num_shares: float,
        price: float,
    ):
        # TODO(Paul): decide how to id these.
        self._fill_id = Fill._fill_id
        # Pointer to the order.
        self.order = order
        # TODO(gp): An Order should contain a list of pointers to its fills for
        #  accounting purposes.
        #  We can verify the invariant that no more than the desired quantity
        #  was filled.
        # Timestamp of when it was completed.
        self.timestamp = timestamp
        # Number of shares executed. This has the same meaning as in Order, i.e., it
        # can be positive and negative depending on long / short.
        hdbg.dassert_ne(num_shares, 0)
        self.num_shares = num_shares
        # Price executed for the given shares.
        hdbg.dassert_lt(0, price)
        self.price = price

    def __str__(self) -> str:
        txt: List[str] = []
        txt.append("Fill:")
        dict_ = self.to_dict()
        for k, v in dict_.items():
            txt.append(f"{k}={v}")
        return " ".join(txt)

    def to_dict(self) -> Dict[str, Any]:
        dict_: Dict[str, Any] = collections.OrderedDict()
        dict_["asset_id"] = self.order.asset_id
        dict_["fill_id"] = self.order.order_id
        dict_["timestamp"] = self.timestamp
        dict_["num_shares"] = self.num_shares
        dict_["price"] = self.price
        return dict_


# TODO(Paul): -> SimulatedBroker
# TODO(*): At some point separate AbstractBroker from SimulatedBroker.
# TODO(Paul): Add unit tests
class Broker:
    """
    Represent a broker to which we can place orders and receive fills back.
    """

    def __init__(
        self,
        price_interface: cdtfprint.AbstractPriceInterface,
        get_wall_clock_time: hdateti.GetWallClockTime,
    ) -> None:
        hdbg.dassert_issubclass(price_interface, cdtfprint.AbstractPriceInterface)
        self._price_interface = price_interface
        # Map a timestamp to the orders with that execution time deadline.
        self._deadline_timestamp_to_orders: Dict[
            pd.Timestamp, List[omorder.Order]
        ] = collections.defaultdict(list)
        # Last seen timestamp to enforce that time is only moving ahead.
        self._last_timestamp = None
        # Track the fills for internal accounting.
        self._fills: List[Fill] = []
        self._get_wall_clock_time = get_wall_clock_time

    def submit_orders(
        self, orders: List[omorder.Order],
    ) -> None:
        """
        Submit a list of orders to the broker at `curr_timestamp`.
        """
        curr_timestamp = self._get_wall_clock_time()
        self._update_last_timestamp(curr_timestamp)
        # Enqueue the orders based on their completion deadline time.
        for order in orders:
            # if self._
            # TODO(gp): curr_timestamp <= order.start_timestamp
            self._deadline_timestamp_to_orders[order.end_timestamp].append(order)

    def get_fills(self, curr_timestamp: pd.Timestamp) -> List[Fill]:
        """
        Get fills for the orders that should have been executed by
        `curr_timestamp`.

        Note that this function can be called only once since it passes
        the ownership of the...
        """
        wall_clock_time = self._get_wall_clock_time()
        if curr_timestamp > wall_clock_time:
            raise ValueError("You are asking about the future")
        self._update_last_timestamp(curr_timestamp)
        # We should always get the "next" orders, for this reason one should use
        # a priority queue.
        timestamps = self._deadline_timestamp_to_orders.keys()
        if not timestamps:
            return []
        hdbg.dassert_eq(min(timestamps), curr_timestamp)
        orders_to_execute = self._deadline_timestamp_to_orders[curr_timestamp]
        _LOG.debug("Executing %d orders", len(orders_to_execute))
        # We can execute this function only once.
        hdbg.dassert_is_not(orders_to_execute, None)
        # `timestamp` should match the end time of the orders.
        for order in orders_to_execute:
            hdbg.dassert_eq(curr_timestamp, order.end_timestamp)
        # "Execute" the orders.
        fills = []
        for order in orders_to_execute:
            # TODO(gp): Here there should be a programmable logic that decides
            #  how many shares are filled.
            fills.extend(self._fully_fill(curr_timestamp, order))
        # Remove the orders that have been executed.
        self._deadline_timestamp_to_orders[curr_timestamp] = None
        _LOG.debug("Returning %d fills", len(fills))
        return fills

    def _fully_fill(
        self, curr_timestamp: pd.Timestamp, order: omorder.Order
    ) -> List[Fill]:
        num_shares = order.num_shares
        # TODO(gp): We should move the logic here.
        price = order.get_execution_price()
        fill = Fill(order, curr_timestamp, num_shares, price)
        return [fill]

    def _update_last_timestamp(self, curr_timestamp: pd.Timestamp) -> None:
        if self._last_timestamp is not None:
            hdbg.dassert_lte(self._last_timestamp, curr_timestamp)
        # Update.
        self._last_timestamp = curr_timestamp
