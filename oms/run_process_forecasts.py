#!/usr/bin/env python

"""
Execute `process_forecasts.py` over a tiled backtest.
"""

import argparse
import datetime
import logging

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import oms.tiled_process_forecasts as otiprfor

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def get_market_data_tile_config() -> cconfig.Config:
    dict_ = {
        "file_name": "/cache/tiled.bar_data.all.2010_2022.20220204",
        "price_col": "close",
        "knowledge_datetime_col": "end_time",
        "start_time_col": "start_time",
        "end_time_col": "end_time",
    }
    config = cconfig.get_config_from_nested_dict(dict_)
    return config


def get_backtest_tile_config() -> cconfig.Config:
    dict_ = {
        "file_name": "",
        "asset_id_col": "",
        "start_date": datetime.date(2020, 12, 1),
        "end_date": datetime.date(2020, 12, 31),
        "prediction_col": "prediction",
        "volatility_col": "vwap.ret_0.vol",
    }
    config = cconfig.get_config_from_nested_dict(dict_)
    return config


async def _run_coro(event_loop):
    market_data_tile_config = get_market_data_tile_config()
    backtest_tile_config = get_backtest_tile_config()
    process_forecasts_config = otiprfor.get_process_forecasts_config()
    await otiprfor.process_forecasts(
        event_loop,
        market_data_tile_config,
        backtest_tile_config,
        process_forecasts_config,
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    with hasynci.solipsism_context() as event_loop:
        hasynci.run(_run_coro(event_loop), event_loop)


if __name__ == "__main__":
    _main(_parse())
