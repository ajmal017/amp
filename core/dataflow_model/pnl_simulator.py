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
    # exit) a prediciton.
    # vals[-2:] = np.nan
    vals[-2:] = 0
    df_5mins["preds"] = vals
    return df_5mins


def compute_pnl_for_instantaneous_no_cost_case(
    w0: float, df: pd.DataFrame, df_5mins: pd.DataFrame
):
    num_shares_history = []
    w_history = []
    pnl_history = []
    #
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


import dataclasses

@dataclasses.dataclass
class Order:
    ts: pd.Timestamp
    action: str


def place_orders_from_predictions(df_5mins: pd.DataFrame) -> List[Order]:
    orders: List[Order] = []
    for ts, row in df_5mins[:-2].iterrows():
        _LOG.debug("# ts=%s", ts)
        pred = row["preds"]
        if pred == 1:
            # Go long.
            action_5 = "buy"
            action_10 = "sell"
        elif pred == -1:
            # Short sell.
            action_5 = "sell"
            action_10 = "buy"
        else:
            raise ValueError
        # Create two orders to enter / exit the position.
        order = Order(ts + pd.DateOffset(minutes=5), action_5)
        orders.append(order)
        order = Order(ts + pd.DateOffset(minutes=10), action_10)
        orders.append(order)
    return orders


def internal_cross_orders(orders: List[Order]) -> List[Order]:
    pass