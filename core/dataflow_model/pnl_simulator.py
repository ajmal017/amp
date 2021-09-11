import logging
from typing import List

import numpy as np
import pandas as pd

import helpers.dbg as dbg

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



def get_instantaneous_price(ts, df) -> float:
    dbg.dassert_in(ts, df.index)
    price = df.loc[ts]["price"]
    return price


def get_twap_price(ts_start, ts_end, df) -> float:
    dbg.dassert_in(ts_start, df.index)
    dbg.dassert_in(ts_end, df.index)
    #price = df.loc[ts]["price"]
    #return price


class Order:

    def __init__(self, df, type_: str, ts_start: pd.Timestamp, ts_end: pd.Timestamp,
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
        dbg.dassert_in(type_, ["trade.start", "trade.end",
                               "trade.twap", "trade.vwap"])
        self.type_ = type_
        dbg.dassert_lte(ts_start, ts_end)
        self.ts_start = ts_start
        self.ts_end = ts_end
        self.num_shares = num_shares

    # def __str__

    def get_execution_price(self):
        if self.type_ == "market_at_end":
            price = get_price(self.ts_end, df)
        elif self.type_ == "market_at_start":
            price = get_price(self.ts_start, df)
        elif self.type_ == "twap":
            # Price is the average of price in (ts_start, ts_end].
            #price =
            price = np.nan
        else:
            raise ValueError("Invalid type='%s'", self._type)

    def merge(self, rhs: "Order") -> "Order":
        # Only orders for the same type / interval, with different num_shares can
        # be merged.
        dbg.dassert_eq(self.type_, rhs.type_)



def get_active_orders(orders, ts: pd.Timestamp) -> List[Order]:
    orders.sort(lambda x: x.ts_start)
    dbg.dassert_lte(ts
    # TODO(gp): This is inefficient. Use binary search.
    curr_orders = []
    for i i




def get_total_wealth(df, ts, cash, holdings) -> float:
    """
    Return the value of the portfolio at time ts.
    """
    pass


def place_orders_from_predictions(df: pd.DataFrame, df_5mins: pd.DataFrame) -> List[Order]:
    num_shares_history = []
    w_history = []
    pnl_history = []
    orders: List[Order] = []
    # Initial balance.
    holdings = 0.0
    cash = w0
    for ts, row in df_5mins[:-2].iterrows():
        _LOG.debug("# ts=%s", ts)
        # Place orders based on the predictions, if needed.
        pred = row["preds"]
        # Mark the portfolio to market.
        w = get_total_wealth(df, ts, cash, holdings)
        # Use current price to convert forecasts in position intents.
        price_0 = get_price(ts, df)
        num_shares = w / price_0
        num_shares *= abs(pred)
        if pred > 0:
            # Go long.
            num_shares_5 = num_shares
            num_shares_10 = -num_shares
        elif pred < 0:
            # Short sell.
            num_shares_5 = -num_shares
            num_shares_10 = num_shares
        elif pred == 0:
            num_shares_5 = num_shares_10 = 0
        else:
            raise ValueError
        # Enter position between [0, 5].
        ts_start = ts + pd.DateOffset(minutes=0)
        ts_end = ts + pd.DateOffset(minutes=5)
        type_ = "price.end"
        order = Order(df, type_, ts_start, ts_end, num_shares_5)
        orders.append(order)
        # Exit position between [5, 10].
        ts_start = ts + pd.DateOffset(minutes=5)
        ts_end = ts + pd.DateOffset(minutes=10)
        type_ = "price.end"
        order = Order(df, type_, ts_start, ts_end, num_shares_10)
        orders.append(order)
        # Execute the orders.
        # INV: When we get here all the orders for the current timestamp `ts` have
        # been placed since we acted on the predictions for ts and we can't place
        # orders in the past.
        # Find all the orders with the current timestamp.

        # Merge the orders.

    return orders


def internal_cross_orders(orders: List[Order]) -> List[Order]:
    pass


# w = 100
# t = 9:30
# pred = 1.0
# price_0 = 10
# Buy w / price_0 shares
#