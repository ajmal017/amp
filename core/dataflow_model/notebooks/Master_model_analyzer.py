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

# %% [markdown]
# # Description
#
# - Initialize with returns, predictions, target volatility, and oos start date
# - Evaluate portfolios generated from the predictions

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.utils as cdmu
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Notebook config

# %%
# exp_dir = "s3://eglp-spm-sasm/experiments/experiment.RH2Ef.v1_9-all.5T.20210831-004747.run1.tgz"
exp_dir = "./experiment.RH2Ef.v1_9-all.5T.20210831-004747.run1.tgz"  # exp_dir = "s3://alphamatic-data/experiments/..."

eval_config = cconfig.get_config_from_nested_dict(
    {
        "exp_dir": exp_dir,
        "model_evaluator_kwargs": {
            "returns_col": "vwap_ret_0_vol_adj_clipped_2",
            "predictions_col": "vwap_ret_0_vol_adj_clipped_2_hat",
            # "oos_start": "2017-01-01",
        },
        "bh_adj_threshold": 0.1,
        "resample_rule": "W",
        "mode": "ins",
        "target_volatility": 0.1,
    }
)

# %% [markdown]
# # Initialize ModelEvaluator and ModelPlotter

# %%
# Load the data.
selected_idxs = list(range(4))
result_bundles = cdmu.yield_experiment_artifacts(
    eval_config["exp_dir"],
    "result_bundle.pkl",
    selected_idxs=selected_idxs,
)

# %%
print("before:", dbg.get_memory_usage(None))
df = next(result_bundles)
print("after:", dbg.get_memory_usage(None))

# %%
i = 0
dfs = []

# %%
rss_before = dbg.get_memory_usage()[0]
print("before", dbg.get_memory_usage_as_str(None))

df_copy = result_df.copy(deep=True)
print("mem usage=", df_copy.memory_usage().sum() / 1024 ** 3)
dfs.append(df_copy)

rss_after = dbg.get_memory_usage()[0]
print("after", dbg.get_memory_usage_as_str(None))

print("mem_increase=", rss_after - rss_before)

# %%
result_df = df[1]["result_df"]

# %%
result_df.memory_usage().sum()

# %%
hintro.format_size(
    result_df[
        ["vwap_ret_0_vol_adj_clipped_2", "vwap_ret_0_vol_adj_clipped_2_hat"]
    ]
    .memory_usage()
    .sum()
)

# %%

# %%
print(df[1].keys())

# hintro.get_size_in_bytes(df[1]["result_df"])
df[1]["result_df"].memory_usage(index=True, deep=True).sum()
# df[1]["result_df"].info()

# %%

# %%
# Build the ModelEvaluator.
evaluator = modeval.build_model_evaluator_from_result_bundles(
    result_bundles,
    abort_on_error=False,
    **eval_config["model_evaluator_kwargs"].to_dict(),
)
# Build the ModelPlotter.
plotter = modplot.ModelPlotter(evaluator)

# %% [markdown]
# # Analysis

# %%
pnl_stats = evaluator.calculate_stats(
    mode=eval_config["mode"], target_volatility=eval_config["target_volatility"]
)
display(pnl_stats)

# %% [markdown]
# ## Model selection

# %%
plotter.plot_multiple_tests_adjustment(
    threshold=eval_config["bh_adj_threshold"], mode=eval_config["mode"]
)

# %%
# TODO(gp): Move this chunk of code in a function.
col_mask = (
    pnl_stats.loc["signal_quality"].loc["sr.adj_pval"]
    < eval_config["bh_adj_threshold"]
)
selected = pnl_stats.loc[:, col_mask].columns.to_list()
not_selected = pnl_stats.loc[:, ~col_mask].columns.to_list()

print("num model selected=%s" % hprint.perc(len(selected), pnl_stats.shape[1]))
print("model selected=%s" % selected)
print("model not selected=%s" % not_selected)

# Use `selected = None` to show all the models.

# %%
plotter.plot_multiple_pnls(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Return correlation

# %%
plotter.plot_correlation_matrix(
    series="returns",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
plotter.plot_effective_correlation_rank(
    series="returns",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Model correlation

# %%
plotter.plot_correlation_matrix(
    series="pnl",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
plotter.plot_effective_correlation_rank(
    series="pnl",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Aggregate model

# %%
pnl_srs, pos_srs, aggregate_stats = evaluator.aggregate_models(
    keys=selected,
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)
display(aggregate_stats)

# %%
plotter.plot_sharpe_ratio_panel(keys=selected, mode=eval_config["mode"])

# %%
plotter.plot_rets_signal_analysis(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_performance(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_rets_and_vol(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
assert 0

# %%
plotter.plot_positions(
    keys=selected,
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
# Plot the returns and prediction for one or more models.
model_key = selected[:1]
plotter.plot_returns_and_predictions(
    keys=model_key,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)
