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

# %% [markdown]
# - Initialize with returns, alpha, and spread
# - Evaluate portfolios generated from the alpha

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
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
# config = cconfig.Config.from_env_var("AM_CONFIG_CODE")
config = None

if config is None:
    experiment_dir = "/cache/experiments/oos_experiment.RH2Eg.v2_0-all.5T.run2.hacked"
    aws_profile = None
    #selected_idxs = range(200)
    selected_idxs = None

    eval_config = cconfig.get_config_from_nested_dict(
        {
            "load_experiment_kwargs": {
                "src_dir": experiment_dir,
                "file_name": "result_bundle.v2_0.pkl",
                "experiment_type": "ins_oos",
                "selected_idxs": selected_idxs,
                "aws_profile": aws_profile,
            },
            "strategy_evaluator_kwargs": {
                "returns_col": "mid_ret_0",
                "position_intent_col": "position_intent_1",
                "spread_col": "spread",
            },
            "bh_adj_threshold": 0.1,
            "resample_rule": "W",
        }
    )

print(str(eval_config))

# %%
#result_bundle_dict

# %%
load_config = eval_config["load_experiment_kwargs"].to_dict()

# Load only the columns needed by the StrategyEvaluator.
load_config["load_rb_kwargs"] = {
    "columns": [
        eval_config["strategy_evaluator_kwargs"]["returns_col"],
        eval_config["strategy_evaluator_kwargs"]["position_intent_col"],
        eval_config["strategy_evaluator_kwargs"]["spread_col"],
    ]
}
result_bundle_dict = cdmu.load_experiment_artifacts(**load_config)

# Build the StrategyEvaluator.
evaluator = modeval.StrategyEvaluator.from_result_bundle_dict(
    result_bundle_dict,
    # abort_on_error=False,
    abort_on_error=True,
    **eval_config["strategy_evaluator_kwargs"].to_dict(),
)

# %%
spread_fraction_paid = 0
#keys = range(3)
keys = None
#result = evaluator.compute_pnl(key_type="attribute", keys=keys)
pnl_dict = evaluator.compute_pnl(spread_fraction_paid, keys=keys, key_type="instrument")

#pnl_dict[0]

# %%
#spread_fraction_paid = 0
#evaluator.calculate_stats(spread_fraction_paid)

# %%
import pandas as pd

# %%
df.shape

# %%
print(dbg.get_memory_usage_as_str(None))

#del pnl_dict

import gc

gc.collect()

print(dbg.get_memory_usage_as_str(None))

# %%
dfs = []
for key in list(pnl_dict.keys()):
    srs = pnl_dict[key]["pnl_0"] + pnl_dict[key]["spread_cost_0"]
    srs.name = key
    dfs.append(srs)
df = pd.concat(dfs, axis=1)

print(df.shape)
df.head()

# %%
pnl_ = df.resample("1B").sum().diff()

pos = abs(pnl_).max()
pos
#mask = pnl_.tail(1) < 0
#pnl_.tail(1)[mask]

# %%
#pos.iloc[0].sort_values()
pos.sort_values().tail(10)

# %%
df.resample("1B").sum().sum(axis=0).argmin()

# %%
dbg.get_memory_usage_as_str(None)

# %%
#df.sum(axis=1).resample("1B").sum().cumsum().plot(color="k")
df.resample("1B").sum().sum(axis=1).cumsum().plot(color="k")

# %%
aggr_pnl = df.resample("1B").sum().drop([224, 554, 311, 384, 589, 404], axis=1).sum(axis=1).cumsum()

aggr_pnl.plot(color="k")

# %%
import numpy as np

def sr(srs):
    return srs.mean() / srs.std() * np.sqrt(252)
    
print("ins", sr(aggr_pnl[:"2017-01-01"].diff()))
print("oos", sr(aggr_pnl["2017-01-01":].diff()))

# %%
aggr_pnl["2018-06-06":].plot()
