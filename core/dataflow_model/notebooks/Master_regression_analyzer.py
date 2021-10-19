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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import pandas as pd

import core.config as cconfig
import core.dataflow_model.incremental_single_name_model_evaluator as ime
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.stats_computer as csc
import core.dataflow_model.utils as cdmu
import core.plotting as cplot
import core.statistics as cstati
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load regression dataframes

# %%
fit_iter = cdmu.yield_experiment_artifacts(
    src_dir="",
    file_name="result_bundle.v2_0.pkl",
    load_rb_kwargs={},
)

pred_iter = cdmu.yield_experiment_artifacts(
    src_dir="",
    file_name="result_bundle.v2_0.pkl",
    load_rb_kwargs={},
)

# %%
fit_coeffs = {k: v.info["ml"]["predict"]["fit_coefficients"] for k, v in fit_iter}
fit_coeffs = pd.concat(fit_coeffs)

# %%
pred_coeffs = {k: v.info["ml"]["predict"]["predict_coefficients"] for k, v in pred_iter}
pred_coeffs = pd.concat(pred_coeffs)
