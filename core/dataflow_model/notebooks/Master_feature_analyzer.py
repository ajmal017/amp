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
# # Load features

# %%
feat_iter = cdmu.yield_experiment_artifacts(
    src_dir="",
    file_name="result_bundle.v2_0.pkl",
    load_rb_kwargs={},
)

# %%
key, artifact = next(feat_iter)
display("key=%s", key)
features = artifact.result_df

# %%
features.head()

# %% [markdown]
# # Cross-sectional feature analysis

# %%
cplot.plot_heatmap(
    features.corr(),
    mode="clustermap",
    figsize=(20, 20)
)

# %%
cplot.plot_effective_correlation_rank(features)

# %%
cplot.plot_projection(features.resample("B").sum(min_count=1))

# %%
sc = csc.StatsComputer()

# %%
features.apply(sc.compute_summary_stats).round(3)

# %% [markdown]
# # Single feature analysis

# %%
feature = ""

# %%
cplot.plot_qq(features[feature])

# %%
cplot.plot_histograms_and_lagged_scatterplot(
    features[feature],
    lag=1,
    figsize=(20, 20)
)

# %%
cplot.plot_time_series_by_period(
    features[feature],
    "hour",
)

# %%
import numpy as np
import pandas as pd

import core.features as cfeat

# %%

# %%

# %%
m = [[1, 0.9, 0, -1], [-1, -1, 0, 0], [0, 0.1, 1, 0], [0, 0, -1, 1]]

# %%
mdf = pd.DataFrame(m, columns=["f1", "f2", "f3", "f4"])

# %%
mdf.std()

# %%
mdf = mdf / np.sqrt((mdf ** 2).sum())

# %%
mdf.std()

# %%
mdf["f1"].std()

# %%
cfeat.compute_correlations(mdf).round(2)

# %%
cfeat.compute_grassmann_distance(mdf, 1, ["f1", "f2"])

# %%
cfeat.compute_grassmann_distance(mdf, 1, ["f1"])

# %%
cfeat.compute_grassmann_distance(mdf, 1, ["f1", "f2", "f3"]).round(3)

# %%
np.linalg.det(m)

# %%
df = mdf

# %%
import core.artificial_signal_generators as casg

# %%
mvn = casg.MultivariateNormalProcess()

# %%
mvn.set_cov_from_inv_wishart_draw(dim=10, seed=343)

# %%
df = mvn.generate_sample(
    date_range_kwargs={
        "start": "2001-01-01",
        "freq": "T",
        "periods": 1000
    },
    seed=708
)

# %%
df

# %%
cfeat.compute_normalized_statistical_leverage_scores(df, demean_cols=True, normalize_cols=True).round(3)

# %%
cfeat.compute_normalized_principal_loadings(df, normalize_cols=True).round(3)

# %%
cfeat.compute_effective_rank(df / df.std(), np.inf)

# %%
cplot.plot_effective_correlation_rank(df / df.std())

# %%
edf = cfeat.evaluate_col_selection(df, [8], normalize_cols=False)
display(edf)

# %%
edf = cfeat.evaluate_col_selection(df, [4], normalize_cols=False)
display(edf)

# %%
cfeat.select_cols_by_greedy_grassmann(df, 5, normalize_cols=False)

# %%
cfeat.select_cols_by_greedy_volume(df, 5, normalize_cols=False)

# %%
cfeat.compute_principal_grassmannian(df, 1)

# %%
