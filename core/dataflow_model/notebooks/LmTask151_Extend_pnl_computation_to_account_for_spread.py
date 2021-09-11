# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import helpers.dbg as dbg
import helpers.printing as prnt

prnt.config_notebook()

# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()
_LOG = logging.getLogger(__name__)

# %%
_LOG.debug = _LOG.info

# %% [markdown]
# # Generate random data

# %%
import numpy as np
import pandas as pd

import core.dataflow_model.pnl_simulator as pnlsim

df = pnlsim.compute_data(21)

display(df.head(3))
display(df.tail(3))

# %% [markdown]
# ## Case 1: instantaneous, no costs

# %%
mode = "instantaneous"
df_5mins = pnlsim.resample_data(df, mode)
display(df_5mins)

# %%
df.plot()

# %%
# Compute pnl using simulation.
w0 = 100.0
final_w, tot_ret = pnlsim.compute_pnl_for_instantaneous_no_cost_case(
    w0, df, df_5mins
)

print(final_w, tot_ret)

# %%
# Compute pnl using lags.
df_5mins["pnl"] = df_5mins["preds"] * df_5mins["ret_0"].shift(-2)
tot_ret2 = (1 + df_5mins["pnl"]).prod() - 1
display(df_5mins[:-1])

# Check that the results are the same.
print("tot_ret=", tot_ret)
print("tot_ret2=", tot_ret2)

np.testing.assert_almost_equal(tot_ret, tot_ret2)

# %%
orders = pnl.place_orders_from_predictions(df_5mins)

orders

# %%
# Merge the orders.
orders.sort(key=lambda x: x.ts, reverse=False)


def to_sign(order):
    return 

def merge_order(order1, order2):
    dbg.dassert_eq(order1.ts, order2.ts)
    
    

current_ts = None
next_order = None
for order in orders:
    if current_ts is None or 

# %%
# Show that the previous approach (which trades two times per interval) is equivalent to trading once with the
# sum of the position.

# # Place the orders.
# orders = []
# for ts, row in df_5mins[:-2].iterrows():
#     _LOG.debug("# ts=%s", ts)
#     pred = row["preds"]
#     if pred == 1:
#         # Go long.
#         action_5 = "buy"
#         action_10 = "sell"
#     elif pred == -1:
#         # Short sell.
#         action_5 = "sell"
#         action_10 = "buy"
#     else:
#         raise ValueError
#     # Create two orders to enter / exit the position.
#     order = (ts + pd.DateOffset(minutes=5), action_5)
#     print(order)
#     orders.append(order)
#     order = (ts + pd.DateOffset(minutes=10), action_10)
#     print(order)
#     orders.append(order)
    
    
# Merge the orders.
orders = orders.sort(lambda x: x[0])


def compute_pnl_from_orders(w0, orders):
    # Assume the orders are in chronological order.
    holdings = 0.0
    cash = w0
    for order in orders:
        ts, action = order
        _LOG.debug("# ts=%s -> action=%s", ts, action)
        price = df.loc[ts]["price"]
        _LOG.debug("  price=%s", price)
        #
        wealth = holdings * price + cash
        _LOG.debug(
            "  before: cash=%s holdings=%s wealth=%s", cash, holdings, wealth
        )
        # Assume that we invest always all the wealth.
        num_shares = wealth / price
        if action == "buy":
            cash -= num_shares * price
            holdings += num_shares
        elif action == "sell":
            cash += num_shares * price
            holdings -= num_shares
        else:
            raise ValueError
        _LOG.debug(
            "  after: cash=%s holdings=%s wealth=%s", cash, holdings, wealth
        )
    # We don't necessary liquidate the portfolio.
    return holdings * price + cash


w = compute_pnl_from_orders(w0, orders)
print((w - w0) / w0)


# %% [markdown]
# ## Case 2: interval trading, no costs

# %%
df_5mins = df.resample("5T", closed="right", label="right").mean()

if True:
    a = df.iloc[1:6]["price"].mean()
    b = df_5mins.iloc[1]["price"]
    # print(a, b)
    assert a == b

df_5mins["ret_0"] = df_5mins["price"].pct_change()

np.random.seed(42)
df_5mins["preds"] = (np.random.random(df_5mins.shape[0]) >= 0.5) * 2.0 - 1.0
display(df_5mins)
