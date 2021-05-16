#!/usr/bin/env python
import argparse

import helpers.cache as hcac
import helpers.dbg as dbg
import helpers.parser as prsr


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, choices=["reset_cache"])
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    action = args.positional[0]
    if action == "reset_cache":
        hcac.reset_cache("disk", tag=None)
        hcac.reset_cache("mem", tag=None)
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
