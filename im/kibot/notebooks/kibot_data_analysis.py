# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2


import pandas as pd

# %%
file = "s3://alphamatic-data/data/kibot/sp_500_1min/AAPL.csv.gz"

df = pd.read_csv(file)
df.head(5)

# %%
file = "s3://alphamatic-data/data/kibot/pq/sp_500_1min/AAPL.pq"

df = pd.read_parquet(file)

# %%
mask = (df.index >= "2019-01-01") & (df.index <= "2019-01-31")
df[mask].to_csv("aapl.csv")
