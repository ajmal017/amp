#!/usr/bin/env python
r"""
Run a pipeline given a list of configs.

# Run an RH1E pipeline using 2 threads:
> run_pipeline.py \
    --dst_dir experiment1 \
    --pipeline ./core/dataflow_model/notebooks/Master_pipeline_runner.py \
    --function "dataflow_lemonade.RH1E.task89_config_builder.build_15min_ar_model_configs()" \
    --num_threads 2
"""
import argparse
import logging
import os
import sys
from typing import List, Optional

import joblib
import tqdm

import core.config as cfg
import core.config_builders as cfgb
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.pickle_ as hpickle
import helpers.printing as printing
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################

import helpers.system_interaction as hsinte

def _run_pipeline(
        i: int,
        pipeline_builder: str,
        dst_dir: str,
        config: cfg.Config,
        config_builder: str,
        num_attempts: int,
        abort_on_error: bool,
) -> Optional[int]:
    """
    Run a pipeline for a specific `Config`.

    The `config_builder` is passed inside the notebook to generate a list of all
    configs to be run as part of a series of experiments, but only the `i`-th config
    is run inside a particular notebook.

    :param i: index of config to select in a list of configs
    :param notebook_file: path to file with experiment template
    :param dst_dir: path to directory to store results
    :param config: config for the experiment
    :param config_builder: function used to generate all the configs
    :param num_attempts: maximum number of times to attempt running the
        notebook
    :param abort_on_error: if `True`, raise an error
    :return: if notebook is skipped ("success.txt" file already exists), return
        `None`; otherwise, return `rc`
    """
    dbg.dassert_eq(1, num_attempts, "Multiple attempts not supported yet")
    cdtfut.setup_experiment_dir(config)
    # Execute experiment.
    _LOG.info("Executing experiment %d", i)
    # Prepare the log file.
    log_file = os.path.join(experiment_result_dir, "run_pipeline.%s.log" % i)
    log_file = os.path.abspath(os.path.abspath(log_file))
    # Prepare command line.
    cmd = ["run_pipeline_stub.py",
       f"--pipeline_builder '{pipeline_builder}'",
        f"--config_builder '{config_builder}'",
        f"--idx {i}",
        f"--dst_dir {dst_dir}",
        "-v INFO",
        ]
    cmd = " ".join(cmd)
    # Execute.
    rc = hsinte.system(cmd, output_file=log_file, abort_on_error=False)
    if rc != 0:
        # The notebook run wasn't successful.
        _LOG.error("Execution failed for experiment %d", i)
        if abort_on_error:
            dbg.dfatal("Aborting")
        else:
            _LOG.error("Continuing execution for next experiments")
    else:
        # Mark as success.
        cdtfut.mark_config_as_success(experiment_result_dir)
    return rc


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Add common experiment options.
    parser = cdtfut.add_experiment_arg(parser)
    # Add pipeline options.
    parser.add_argument(
        "--pipeline",
        action="store",
        required=True,
        help="File storing the pipeline to iterate over",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.no_incremental)

    configs = cdtfut.get_configs_from_command_line(args)

    # Get the file with the pipeline to run.
    pipeline_file = os.path.abspath(args.pipeline)
    dbg.dassert_exists(pipeline_file)
    # Parse command-line options.
    num_attempts = args.num_attempts
    abort_on_error = not args.skip_on_error
    publish = args.publish_notebook
    num_threads = args.num_threads
    # Execute.
    if num_threads == "serial":
        rcs = []
        for config in tqdm.tqdm(configs):
            _LOG.debug("\n%s", printing.frame("Config %s" % i))
            #
            rc = _run_pipeline(
                int(config[("meta", "id")]),
                notebook_file,
                dst_dir,
                config,
                config_builder,
                num_attempts,
                abort_on_error,
            )
            rcs.append(rc)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        rcs = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_run_pipeline)(
                int(config[("meta", "id")]),
                notebook_file,
                dst_dir,
                config,
                config_builder,
                num_attempts,
                abort_on_error,
                publish,
            )
            for config in configs
        )
    # Report failing experiments.
    experiment_ids = [int(config[("meta", "id")]) for config in configs]
    failed_experiment_ids = [
        i for i, rc in zip(experiment_ids, rcs) if rc is not None and rc != 0
    ]
    if failed_experiment_ids:
        _LOG.error("Failed experiments are: %s", failed_experiment_ids)


if __name__ == "__main__":
    _main(_parse())
