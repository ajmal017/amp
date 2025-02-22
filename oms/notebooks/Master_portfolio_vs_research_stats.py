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

import logging

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms as oms

# %% run_control={"marked": true}
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
date = "YYYY-MM-DD"
start_timestamp = pd.Timestamp(date + " 09:30:00", tz="America/New_York")
end_timestamp = pd.Timestamp(date + " 16:00:00", tz="America/New_York")

# %%
dict_ = {
    "portfolio_data_dir": f"{date}/portfolio",
    "research_data_dir": f"{date}/evaluate_forecasts",
    "freq": "5T",
    "portfolio_file_name": None,
    "research_file_name": None,
    "start_timestamp": start_timestamp,
    "end_timestamp": end_timestamp,
}

# %%
config = cconfig.get_config_from_nested_dict(dict_)
#
start_timestamp = config["start_timestamp"]
end_timestamp = config["end_timestamp"]

# %%
# Load and time-localize Portfolio logged data.
paper_df, paper_stats_df = oms.AbstractPortfolio.read_state(
    config["portfolio_data_dir"],
    file_name=config["portfolio_file_name"],
)
paper_df = paper_df.loc[start_timestamp:end_timestamp]
paper_stats_df = paper_stats_df.loc[start_timestamp:end_timestamp]

# %%
# Load and time localize ForecastEvaluator logged data.
(
    research_df,
    research_stats_df,
) = dtfmod.ForecastEvaluatorFromReturns.read_portfolio(
    config["research_data_dir"],
    file_name=config["research_file_name"],
)
research_df = research_df.loc[start_timestamp:end_timestamp]
research_stats_df = research_stats_df.loc[start_timestamp:end_timestamp]


# %%
def compute_delay(df: pd.DataFrame, freq: str) -> pd.Series:
    diff = df.index - df.index.round(freq)
    srs = pd.Series(
        [
            diff.mean(),
            diff.std(),
        ],
        [
            "mean",
            "stdev",
        ],
        name="delay",
    )
    return srs


# Compute delay stats.
delay_stats = compute_delay(paper_stats_df, config["freq"])
display(delay_stats)

# Round paper_stats_df to bar
paper_stats_df.index = paper_stats_df.index.round(config["freq"])

# %%
bar_stats_df = pd.concat(
    [research_stats_df, paper_stats_df], axis=1, keys=["research", "paper"]
)
display(bar_stats_df.head(3))

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    bar_stats_df, config["freq"]
)
display(stats_sxs)


# %%
def per_asset_pnl_corr(
    research_df: pd.DataFrame, paper_df: pd.DataFrame, freq: str
) -> pd.Series:
    research_pnl = research_df["pnl"]
    paper_pnl = paper_df["pnl"]
    corrs = {}
    for asset_id in research_pnl.columns:
        pnl1 = research_pnl[asset_id].resample(freq).sum(min_count=1)
        pnl2 = paper_pnl[asset_id].resample(freq).sum(min_count=1)
        corr = pnl1.corr(pnl2)
        corrs[asset_id] = corr
    corr_srs = pd.Series(corrs).rename("pnl_correlation")
    return corr_srs


# %%
# Display per-asset PnL correlations.
pnl_corrs = per_asset_pnl_corr(research_df, paper_df, config["freq"])
pnl_corrs.hist(bins=101)

# %%
pnl_corrs.sort_values().head()

# %%
pnl = bar_stats_df.T.xs("pnl", level=1).T
display(pnl.head())

# %%
pnl.corr()

# %%
coplotti.plot_portfolio_stats(bar_stats_df)
