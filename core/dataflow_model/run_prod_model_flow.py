"""
Import as:

import core.dataflow_model.run_prod_model_flow as rpmf
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.config as cconfig
import core.dataflow as cdataf
import core.dataflow_model.stats_computer as cstats
import core.dataflow_model.utils as cdmu
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dbg as dbg
import helpers.introspection as hintro

_LOG = logging.getLogger(__name__)


def run_prod_model_flow(bm_config: str,
                   config_builder: str,
                   experiment_builder: str,
                    run_model_opts: str,
                   model_eval_config: Optional[cconfig.Config],
                   strategy_eval_config: Optional[cconfig.Config],
                    dst_dir: str):
    # 1) Run the model.
    _LOG.debug("\n%s", prnt.frame("Run model", char1="<"))
    #bm_config = "v2_0-top1.5T"
    #bm_config = "kibot_v1-top1.5T"
    run_model_dst_dir = os.path.join(dst_dir, "run_model")
    # We abort on error since we don't expect failures.
    #extra_opts = ""
    run_model_dir = self._run_model(bm_config, config_builder, experiment_builder, run_model_opts, run_model_dst_dir)
    # 2) Run the ModelEvaluator notebook.
    if model_eval_config is not None:
        _LOG.debug("\n%s", prnt.frame("Run model analyzer notebook", char1="<"))
        amp_dir = git.get_amp_abs_path()
        # TODO(gp): Rename -> Master_model_evaluator
        file_name = os.path.join(amp_dir, "core/dataflow_model/notebooks/Master_model_analyzer.ipynb")
        #
        run_notebook_dir = os.path.join(dst_dir, "run_model_analyzer")
        #
        eval_config = self._get_eval_config(run_model_dir)
        python_code = eval_config.to_python(check=True)
        env_var = "AM_CONFIG_CODE"
        pre_cmd = f'export {env_var}="{python_code}"'
        #
        hjupyter.run_notebook(file_name, run_notebook_dir, pre_cmd=pre_cmd)
    # 3) Run the StrategyEvaluator notebook.
    if strategy_eval_config is not None:
        _LOG.debug("\n%s", prnt.frame("Run strategy analyzer notebook", char1="<"))
        amp_dir = git.get_amp_abs_path()
        # TODO(gp): Rename -> Master_strategy_evaluator
        file_name = os.path.join(amp_dir, "core/dataflow_model/notebooks/Master_strategy_analyzer.ipynb")
        #
        run_notebook_dir = os.path.join(dst_dir, "run_strategy_analyzer")
        #
        eval_config = self._get_eval_config(run_model_dir)
        python_code = eval_config.to_python(check=True)
        env_var = "AM_CONFIG_CODE"
        pre_cmd = f'export {env_var}="{python_code}"'
        #
        hjupyter.run_notebook(file_name, run_notebook_dir, pre_cmd=pre_cmd)
    # 4) Freeze PnL.
    actual_outcome = []
    # Build the ModelEvaluator from the eval config.
    evaluator = modeval.ModelEvaluator.from_eval_config(eval_config)
    pnl_stats = evaluator.calculate_stats(
        mode=eval_config["mode"], target_volatility=eval_config["target_volatility"]
    )
    actual_outcome.append(p)
    actual_outcome.append(hut.convert_df_to_string(pnl_stats, index=True))
    # TODO(gp): Add the corresponding info for Strategy PnL.
    actual_outcome = "\n".join(actual_outcome)
    self.check_string(act)


def _run_model(bm_config: str, config_builder: str, experiment_builder: str, extra_opts: str, dst_dir: str) -> None:
    # Execute a command line like:
    #   /app/amp/core/dataflow_model/run_experiment.py \
    #       --experiment_builder \
    #           amp.core.dataflow_model.master_experiment.run_experiment \
    #       --config_builder \
    #           'dataflow_lemonade.RH1E.RH1E_configs.build_model_configs("kibot_v1-top1.5T", 1)'
    #       --dst_dir .../run_model/oos_experiment.RH1E.kibot_v1-top1.5T \
    #       --clean_dst_dir \
    #       --no_confirm \
    #       --num_threads serial
    tag = "RH1E"
    dst_dir = f"{dst_dir}/oos_experiment.{tag}.{bm_config}"
    if os.path.exists(dst_dir):
        _LOG.warning("Dir with experiment already exists: skipping...")
        return dst_dir
    #
    opts = []
    opts.append("--clean_dst_dir --no_confirm")
    opts.append("--num_threads serial")
    opts.append(extra_opts)
    opts = " ".join(opts)
    #
    exec = git.get_client_root(super_module=False)
    exec = os.path.join(exec, "amp/core/dataflow_model/run_experiment.py")
    dbg.dassert_exists(exec)
    #
    cmd = []
    cmd.append(exec)
    # Experiment builder.
    #experiment_builder = "amp.core.dataflow_model.master_experiment.run_experiment"
    cmd.append(f"--experiment_builder {experiment_builder}")
    # Config builder.
    #builder = f'build_oos_model_configs("{bm_config}")'
    #builder = f'build_model_configs("{bm_config}", 1)'
    #config_builder = f'research.RH1E.RH1E_configs.{builder}'
    #config_builder = f'dataflow_lemonade.RH1E.RH1E_configs.{builder}'
    cmd.append(f"--config_builder '{config_builder}'")
    #
    cmd.append(f"--dst_dir {dst_dir}")
    cmd.append(opts)
    cmd = " ".join(cmd)
    hsinte.system(cmd)
    return dst_dir