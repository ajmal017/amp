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

#dbg.init_logger(verbosity=logging.DEBUG)
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

np.random.seed(42)

#date_range = pd.date_range("09:30", "15:00", freq="1T")
date_range = pd.date_range("09:30", "10:00", freq="1T")

diff = np.random.normal(0, 1, size=len(date_range))
diff = diff.cumsum()
price = 100.0 + diff
df = pd.DataFrame(price, index=date_range, columns=["price"])

# ask, bid
df["ask"] = price + np.abs(np.random.normal(0, 1, size=len(date_range)))
df["bid"] = price - np.abs(np.random.normal(0, 1, size=len(date_range)))
display(df.head(5))

# %% [markdown]
# ## Case 1: instantaneous, no costs

# %%
# Sample on 5 minute bars labeling and close on the right

#df_5mins = df.resample("5T", closed="right", label="right").mean()
df_5mins = df.resample("5T", closed="right", label="right").last()

if False:
    a = df.iloc[1:6]["price"].mean()
    b = df_5mins.iloc[1]["price"]
    #print(a, b)
    assert a == b
    
df_5mins["ret_0"] = df_5mins["price"].pct_change()

df_5mins["preds"] = (np.random.random(df_5mins.shape[0]) >= 0.5) * 2.0 - 1.0
display(df_5mins)

# %%
df.plot()

# %%
# Naive pnl

w0 = 100.0
w = w0
for ts, row in df_5mins[:-2].iterrows():
    _LOG.debug("ts=%s", ts)
    pred = row["preds"]
    price_5 = df.loc[ts + pd.DateOffset(minutes=5)]["price"]
    price_10 = df.loc[ts + pd.DateOffset(minutes=10)]["price"]
    _LOG.debug("# pred=%s price_5=%s price_10=%s", pred, price_5, price_10)
    # 
    num_shares = w / price_5
    if pred == 1:
        # Go long.
        buy_pnl = num_shares * price_5
        sell_pnl = num_shares * price_10
        diff = -buy_pnl + sell_pnl
    elif pred == -1:
        # Short sell.
        sell_pnl = num_shares * price_5
        buy_pnl = num_shares * price_10
        diff = sell_pnl - buy_pnl
    else:
        raise ValueError
    _LOG.debug("  w=%s num_shares=%s", w, num_shares)
    w += diff
    _LOG.debug("  diff=%s -> w=%s", diff, w)
        
print(w)
print((w - w0) / w0)

# Use lags.
df_5mins["pnl"] = df_5mins["preds"] * df_5mins["ret_0"].shift(-2)
pnls = df_5mins["pnl"][:-1]

print((1 + pnls).prod() - 1)

display(df_5mins[:-1])

# %%

# %%
