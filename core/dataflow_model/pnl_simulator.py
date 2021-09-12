import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

_LOG.debug = _LOG.info


def compute_data(num_samples: int, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    date_range = pd.date_range("09:30", periods=num_samples, freq="1T")
    # Random walk.
    diff = np.random.normal(0, 1, size=len(date_range))
    diff = diff.cumsum()
    price = 100.0 + diff
    #
    df = pd.DataFrame(price, index=date_range, columns=["price"])
    # Add ask, bid, where price is not the midpoint.
    df["ask"] = price + np.abs(np.random.normal(0, 1, size=len(date_range)))
    df["bid"] = price - np.abs(np.random.normal(0, 1, size=len(date_range)))
    return df


#def get_example_data1() -> pd.DataFrame:


def resample_data(df: pd.DataFrame, mode: str, seed: int = 42) -> pd.DataFrame:
    # Sample on 5 minute bars, labeling and close interval on the right.
    df_5mins = df.resample("5T", closed="right", label="right")
    if mode == "instantaneous":
        df_5mins = df_5mins.last()
    elif mode == "twap":
        df_5mins = df_5mins.mean()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    # Compute ret_0.
    df_5mins["ret_0"] = df_5mins["price"].pct_change()
    # Compute random predictions.
    np.random.seed(seed)
    vals = (np.random.random(df_5mins.shape[0]) >= 0.5) * 2.0 - 1.0
    # Zero out the last two predictions since we need two lags to realize (enter /
    # exit) a prediction.
    # vals[-2:] = np.nan
    vals[-2:] = 0
    df_5mins["preds"] = vals
    return df_5mins

# TODO(gp): Extend for multiple stocks.

def compute_pnl_for_instantaneous_no_cost_case(
    w0: float, df: pd.DataFrame, df_5mins: pd.DataFrame
):
    num_shares_history = []
    w_history = []
    pnl_history = []
    # Initial balance.
    w = w0
    # Skip the last two rows since we need two rows to enter / exit the position.
    for ts, row in df_5mins[:-2].iterrows():
        _LOG.debug("# ts=%s", ts)
        pred = row["preds"]
        #
        ts_5 = ts + pd.DateOffset(minutes=5)
        dbg.dassert_in(ts_5, df.index)
        price_5 = df.loc[ts_5]["price"]
        #
        ts_10 = ts + pd.DateOffset(minutes=10)
        dbg.dassert_in(ts_10, df.index)
        price_10 = df.loc[ts_10]["price"]
        _LOG.debug("  pred=%s price_5=%s price_10=%s", pred, price_5, price_10)
        #
        num_shares = w / price_5
        # The magnitude of the prediction is interpreted as amount of leverage.
        num_shares *= abs(pred)
        if pred > 0:
            # Go long.
            buy_pnl = num_shares * price_5
            sell_pnl = num_shares * price_10
            diff = -buy_pnl + sell_pnl
        elif pred < 0:
            # Short sell.
            sell_pnl = num_shares * price_5
            buy_pnl = num_shares * price_10
            diff = sell_pnl - buy_pnl
        elif pred == 0:
            # Stay flat.
            diff = 0.0
        else:
            raise ValueError
        _LOG.debug("  w=%s num_shares=%s", w, num_shares)
        w += diff
        num_shares_history.append(num_shares)
        w_history.append(w)
        pnl_history.append(diff)
        _LOG.debug("  diff=%s -> w=%s", diff, w)
    # Update the df with intermediate results.
    buffer = [np.nan] * 2
    df_5mins["num_shares"] = num_shares_history + buffer
    df_5mins["w"] = w_history + buffer
    df_5mins["pnl_sim"] = pnl_history + buffer
    # Compute total return.
    total_ret = (w - w0) / w0
    return w, total_ret, df_5mins


def compute_lag_pnl(df_5mins: pd.DataFrame) -> pd.DataFrame:
    df_5mins["pnl_lag"] = df_5mins["preds"] * df_5mins["ret_0"].shift(-2)
    tot_ret_lag = (1 + df_5mins["pnl_lag"]).prod() - 1
    return tot_ret_lag, df_5mins


def get_instantaneous_price(df: pd.DataFrame, ts: pd.Timestamp, column: str) -> float:
    dbg.dassert_in(ts, df.index)
    dbg.dassert_in(column, df.columns)
    price = df.loc[ts][column]
    return price


def get_twap_price(df: pd.DataFrame, ts_start: pd.Timestamp, ts_end: pd.Timestamp, column: str) -> float:
    """
    Compute TWAP of the column `column` in (ts_start, ts_end].
    """
    dbg.dassert_in(ts_start, df.index)
    dbg.dassert_in(ts_end, df.index)
    dbg.dassert_lt(ts_start, ts_end)
    dbg.dassert_in(column, df.columns)
    prices = df[ts_start:ts_end][column]
    # Remove the first row to represent `(ts_start, ...`.
    prices = prices.iloc[1:]
    dbg.dassert_lte(1, prices.shape[0])
    price = prices.mean()
    return price


class Order:

    def __init__(self, df: pd.DataFrame, type_: str, ts_start: pd.Timestamp, ts_end: pd.Timestamp,
                 num_shares: float):
        self._df = df
        # An order has 2 characteristics:
        # 1) what price is executed
        #    - price: the (historical) realized price
        #    - midpoint: the midpoint
        #    - full_spread: always cross the spread to hit ask or lift bid
        #    - partial_spread: pay a percentage of spread
        # 2) timing semantic
        #    - at beginning of interval
        #    - at end of interval
        #    - twap
        #    - vwap
        dbg.dassert_in(type_, ["price.start", "price.end",
                               "price.twap", "price.vwap"])
        self.type_ = type_
        dbg.dassert_lte(ts_start, ts_end)
        self.ts_start = ts_start
        self.ts_end = ts_end
        self.num_shares = num_shares

    def __str__(self) -> str:
        return (f"Order: type={self.type_} " +
                f"ts=[{self.ts_start}, {self.ts_end}] " +
                f"num_shares={self.num_shares}")

    @staticmethod
    def get_price(df: pd.DataFrame, type_: str, ts_start: pd.Timestamp, ts_end: pd.Timestamp) -> float:
        if type_ == "price.start":
            price = get_instantaneous_price(df, ts_start, "price")
        elif type_ == "price.end":
            price = get_instantaneous_price(df, ts_end, "price")
        elif type_ == "price.twap":
            # Price is the average of price in (ts_start, ts_end].
            #price =
            price = np.nan
        else:
            raise ValueError("Invalid type='%s'", type_)
        _LOG.debug("type=%s, ts_start=%s, ts_end=%s -> execution_price=%s", type_,
                   ts_start, ts_end, price)
        return price

    def get_execution_price(self) -> float:
        price = self.get_price(self._df, self.type_, self.ts_start, self.ts_end)
        return price

    def is_mergeable(self, rhs: "Order") -> bool:
        return ((self.type_ == rhs.type_) and (self.ts_start == rhs.ts_start) and
                (self.ts_end == rhs.ts_end))

    def merge(self, rhs: "Order") -> "Order":
        """
        Accumulate current order with `rhs`, if compatible, and return the merged order.
        """
        # Only orders for the same type / interval, with different num_shares can
        # be merged.
        dbg.dassert(self.is_mergeable(rhs))
        num_shares = self.num_shares + rhs.num_shares
        order = Order(self._df, self.type_, self.ts_start, self.ts_end, num_shares)
        return order

    def copy(self) -> "Order":
        return copy.copy(self)


def get_orders_to_execute(orders: List[Order], ts: pd.Timestamp) -> List[Order]:
    orders.sort(key=lambda x: x.ts_start, reverse=False)
    dbg.dassert_lte(orders[0].ts_start, ts)
    # TODO(gp): This is inefficient. Use binary search.
    curr_orders = []
    for order in orders:
        if order.ts_start == ts:
            curr_orders.append(order)
    return curr_orders


def orders_to_string(orders: List[Order]) -> str:
    return str(list(map(str, orders)))


def get_total_wealth(df: pd.DataFrame, ts: pd.Timestamp, cash: float, holdings: float, column: str) -> float:
    """
    Return the value of the portfolio at time ts.
    """
    price = get_instantaneous_price(df, ts, column)
    holdings_value = holdings * price
    _LOG.debug("Marking at ts=%s holdings=%s at %s -> value=%s", ts, holdings, price, holdings_value)
    wealth = cash + holdings_value
    return wealth

import collections

def simulate(df: pd.DataFrame, df_5mins: pd.DataFrame, initial_wealth: float,
             config: Dict[str, Any]) -> List[Order]:
    columns = [
        "target_n_shares",
        "cash.before",
        "holdings.before",
        "wealth.before",
        "diff_n_shares",
        #
        "filled_n_shares",
        "cash.after",
        "holdings.after",
        "wealth.after",
    ]
    accounting = collections.OrderedDict()
    for column in columns:
        accounting[column] = []
    def _update(key: str, value: float) -> None:
        prev_value = accounting[key][-1] if accounting[key] else None
        _LOG.debug("%s=%s -> %s", key, prev_value, value)
        accounting[key].append(value)

    orders: List[Order] = []
    # Initial balance.
    holdings = 0.0
    cash = initial_wealth
    for ts, row in df_5mins[:-1].iterrows():
        _LOG.debug(hprint.frame("# ts=%s" % ts))
        # 1) Place orders based on the predictions, if needed.
        pred = row["preds"]
        _LOG.debug("pred=%s" % pred)
        # Mark the portfolio to market.
        _LOG.debug("# Mark portfolio to market")
        wealth = get_total_wealth(df, ts, cash, holdings, "price")
        _update("wealth.before", wealth)
        # Use current price to convert forecasts in position intents.
        _LOG.debug("# Decide how much to trade")
        # Enter position between [0, 5].
        ts_start = ts + pd.DateOffset(minutes=0)
        ts_end = ts + pd.DateOffset(minutes=5)
        # NOTE:
        # - in the vectorized PnL case we assume we work in terms of wealth and not
        #   shares, so we assume we can buy the entire amount of wealth at the
        #   execution price.
        # - in the real set-up, we need to place order for a certain number of shares
        #   before we know what price we will get. Thus we use the price at the
        #   decision time to estimate the number of shares, which means that we
        #   can't always invest exactly the whole available wealth.
        order_type = config["order_type"]
        if config.get("use_current_price_for_target_n_shares", True):
            price_0 = get_instantaneous_price(df, ts, config["price_column"])
        else:
            price_0 = Order.get_price(df, order_type, ts_start, ts_end)
        _LOG.debug("price_0=%s", price_0)
        target_num_shares = wealth / price_0
        target_num_shares *= pred
        _update("target_n_shares", target_num_shares)
        _update("cash.before", cash)
        _update("holdings.before", holdings)
        _LOG.debug("# Place orders")
        diff = target_num_shares - holdings
        _update("diff_n_shares", diff)
        # Create order.
        order = Order(df, order_type, ts_start, ts_end, diff)
        _LOG.debug("order=%s", order)
        orders.append(order)
        # 2) Execute the orders.
        # INV: When we get here all the orders for the current timestamp `ts` have
        # been placed since we acted on the predictions for `ts` and we can't place
        # orders in the past.
        # Find all the orders with the current timestamp.
        _LOG.debug("# Get orders to execute")
        orders_to_execute = get_orders_to_execute(orders, ts)
        _LOG.debug("orders_to_execute=%s", orders_to_string(orders_to_execute))
        # Merge the orders.
        merged_orders = []
        while orders_to_execute:
            order = orders_to_execute.pop()
            orders_to_execute_tmp = orders_to_execute[:]
            for next_order in orders_to_execute_tmp:
                if order.is_mergeable(next_order):
                    order = order.merge(next_order)
                    orders_to_execute_tmp.remove(next_order)
            merged_orders.append(order)
            orders_to_execute = orders_to_execute_tmp
        _LOG.debug("After merging:\n  merged_orders=%s\n  orders_to_execute=%s",
                   orders_to_string(merged_orders),
                   orders_to_string(orders_to_execute))
        # Execute the merged orders.
        _LOG.debug("# Execute orders")
        # TODO(gp): We rely on the assumption that order span only one time step.
        #  so we can evaluate an order starting now and ending in the next time step.
        #  A more accurate simulation requires to attach "callbacks" representing actions
        #  to timestamp.
        # TODO(gp): For now there should be at most one order.
        dbg.dassert_lte(len(merged_orders), 1)
        order = merged_orders[0]
        _LOG.debug("order=%s", order)
        num_shares = order.num_shares
        _update("filled_n_shares", num_shares)
        holdings += num_shares
        _update("holdings.after", holdings)
        executed_price = order.get_execution_price()
        cash -= executed_price * num_shares
        _update("cash.after", cash)
        wealth = get_total_wealth(df, ts, cash, holdings, config["price_column"])
        _update("wealth.after", wealth)
    # Update the df with intermediate results.
    buffer = [np.nan] * (df_5mins.shape[0] - len(accounting["wealth.before"]))
    for key, value in accounting.items():
        df_5mins[key] = value + buffer
    df_5mins["pnl.sim"] = (df_5mins["wealth.after"] - df_5mins["wealth.before"]) / df_5mins["wealth.before"]
    return df_5mins
