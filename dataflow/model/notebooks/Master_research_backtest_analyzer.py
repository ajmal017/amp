# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import datetime
import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import core.statistics as costatis
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hparquet as hparque
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load tiled backtest

# %%
dict_ = {
    "file_name": "",
    "start_date": datetime.date(2010, 1, 1),
    "end_date": datetime.date(2020, 12, 31),
    "asset_id_col": "",
    "returns_col": "",
    "volatility_col": "",
    "prediction_col": "",
    "feature_cols": None,
    "target_gmv": 1e6,
    "dollar_neutrality": "no_constraint",
    "freq": "5T",
}
config = cconfig.get_config_from_nested_dict(dict_)

# %%
parquet_tile_analyzer = dtfmod.ParquetTileAnalyzer()
parquet_tile_metadata = parquet_tile_analyzer.collate_parquet_tile_metadata(
    config["file_name"]
)

# %%
parquet_tile_analyzer.compute_metadata_stats_by_asset_id(parquet_tile_metadata)

# %%
parquet_tile_analyzer.compute_universe_size_by_time(parquet_tile_metadata)

# %% [markdown]
# # Compute portfolio bar metrics

# %%
bar_metrics = dtfmod.generate_bar_metrics(
    config["file_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    config["returns_col"],
    config["volatility_col"],
    config["prediction_col"],
    config["target_gmv"],
    config["dollar_neutrality"],
)

# %%
coplotti.plot_portfolio_stats(bar_metrics, freq="B")

# %% [markdown]
# # Compute aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
portfolio_stats, daily_metrics = stats_computer.compute_portfolio_stats(
    bar_metrics,
    "B",
)
display(portfolio_stats)

# %% [markdown]
# # Regression analysis

# %%
hdbg.dassert(config["target_col"])
hdbg.dassert(config["feature_cols"])

# %%
coefficients = dtfmod.regress(
    config["file_name"],
    config["asset_id_col"],
    config["target_col"],
    config["feature_cols"],
    2,
    20,
)

# %%
coefficients.head(3)

# %% [markdown]
# # Predictor mixing

# %%
hdbg.dassert(config["feature_cols"])

# %%
features = config["feature_cols"]
weights = pd.DataFrame(np.identity(len(features)), features, features)
weights["sum"] = 1
display(weights)

# %%
mix_bar_metrics = dtfmod.load_mix_evaluate(
    config["file_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    config["returns_col"],
    config["volatility_col"],
    config["feature_cols"],
    weights,
    config["target_gmv"],
    config["dollar_neutrality"],
)

# %%
mix_portfolio_stats, mix_daily_metrics = stats_computer.compute_portfolio_stats(
    mix_bar_metrics,
    "B",
)
display(mix_portfolio_stats)

# %%
coplotti.plot_portfolio_stats(mix_bar_metrics, freq="B")
