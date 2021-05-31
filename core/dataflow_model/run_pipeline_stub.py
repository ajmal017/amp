#!/usr/bin/env python
r"""
Run a single DAG model wrapping

# Use example:
> run_notebook_stub.py \
    --dst_dir nlp/test_results \
    --function "nlp.build_configs.build_PTask1088_configs()" \
    --num_threads 2
"""
import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Add notebook options.
    parser.add_argument(
        "--pipeline_builder",
        action="store",
        required=True,
        help="",
    )
    parser.add_argument(
        "--config_builder",
        action="store",
        required=True,
        help="",
    )
    parser.add_argument(
        "--index",
        action="store",
        required=True,
        help="",
    )
    parser.add_argument(
        "--experiment_result_dir",
        action="store",
        required=True,
        help="",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    # Create the dst dir.
    experiment_result_dir = os.path.abspath(args.experiment_result_dir)
    io_.create_dir(experiment_result_dir, incremental=True)
    #
    params = {
        "config_builder": args.config_builder,
        "experiment_result_dir": args.experiment_result_dir,
        "pipeline_builder": args.pipeline_builder,
    }
    config = ccbuild.get_config_from_params(config_idx, params)
    # TODO(gp): Generalize this in terms of `pipeline_builder`.
    master_pipeline.run_pipeline(config)


if __name__ == "__main__":
    _main(_parse())
