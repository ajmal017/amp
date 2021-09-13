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

# %% [markdown]
# # Generate random data

# %%
import numpy as np

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
df_5mins = pnlsim.get_example_data1()
df = df_5mins

display(df_5mins)

# %%
# Compute pnl using simulation.
w0 = 100.0
final_w, tot_ret, df_5mins = pnlsim.compute_pnl_level1(w0, df, df_5mins)

print(final_w, tot_ret)

# %%
# Compute pnl using lags.
# df_5mins["pnl"] = df_5mins["preds"] * df_5mins["ret_0"].shift(-2)
# tot_ret2 = (1 + df_5mins["pnl"]).prod() - 1
# display(df_5mins[:-1])

tot_ret2, df_5mins = pnlsim.compute_lag_pnl(df_5mins)

# Check that the results are the same.
print("tot_ret=", tot_ret)
print("tot_ret2=", tot_ret2)
np.testing.assert_almost_equal(tot_ret, tot_ret2)

# %%
mode = "instantaneous"
df = df_5mins = pnlsim.get_example_data1()
# df_5mins = pnlsim.resample_data(df, mode)

initial_wealth = 1000
final_w, tot_ret, df_5mins = pnlsim.compute_pnl_level1(
    initial_wealth, df, df_5mins
)
tot_ret2, df_5mins = pnlsim.compute_lag_pnl(df_5mins)
# display(df_5mins)

config = {
    "price_column": "price",
    "future_snoop_allocation": True,
    "order_type": "price.end",
}

df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)

df_5mins

# %%
_, df_5mins = pnlsim.compute_lag_pnl(df_5mins)
display(df_5mins)

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
