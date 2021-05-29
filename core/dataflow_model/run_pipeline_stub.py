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
import sys
from typing import List, Optional

import joblib
import tqdm

import core.config as cfg
import core.config_builders as cfgb
import core.dataflow_model.utils as cdtfut
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.pickle_ as hpickle
import helpers.printing as printing
import helpers.system_interaction as si


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
        "--dst_dir",
        action="store",
        required=True,
        help="",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # TODO(gp): Save log.
    dbg.init_logger(verbosity=args.log_level)
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=True)


if __name__ == "__main__":
    _main(_parse())
