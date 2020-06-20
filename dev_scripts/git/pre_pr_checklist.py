#!/usr/bin/env python

"""
You are in a branch for one of git repo, which can include submodules.

Qualify a branch for submitting a PR to master by:
- making sure git client is empty
- chekcking that master has been merged
- running linter in all repos
- running tests in the outermost repo

> pre_pr_checklist.py
"""

import argparse
import logging
import os
from typing import List, Tuple

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _run_linter_on_dir(actions, cd_cmd, dir_name, debug, abort_on_error, output):
    action = "linter"
    to_execute, actions = prsr.mark_action(action, actions)
    if to_execute:
        output.append(prnt.frame("%s: linter log" % dir_name))
        #
        linter_log = "linter_log.txt"
        if dir_name != ".":
            linter_log = "%s.%s" % (dir_name, linter_log)
        linter_log = os.path.abspath(linter_log)
        if debug:
            file_name = git.find_file_in_git_tree("test_dbg.py")
            _LOG.warning("Running a quick lint")
            cmd = "linter.py -f %s --linter_log %s" % (file_name, linter_log)
        else:
            cmd = "linter.py -b --linter_log %s" % linter_log
        rc = si.system(
            cd_cmd + cmd,
            suppress_output=False,
            abort_on_error=False,
            log_level="echo",
        )
        _LOG.info("linter output=\n%s", linter_log)
        if rc != 0:
            _LOG.warning("There are lints. Please take time to fix them")
        # Read output from the linter.
        txt = io_.from_file(linter_log)
        output.append(txt)
    # def _run_linter_check() -> None:
    #    modified_files = _get_modified_files()
    #    dbg.dassert(
    #        len(modified_files) == 0,
    #        msg=f"Commit changes or stash them.\n{modified_files}",
    #    )
    #    amp_path = os.environ["AMP"]
    #    cmd = f"{amp_path}/dev_scripts/linter_master_report.py"
    #    _, output = si.system_to_string(cmd, abort_on_error=False)
    #    print(output.strip())
    return actions, output


def _run_tests_for_dir(
    actions, cd_cmd, dir_name, test_list, debug, abort_on_error, output,
):
    action = "run_tests"
    to_execute, actions = prsr.mark_action(action, actions)
    is_ok = True
    if to_execute:
        output.append(prnt.frame("%s: unit tests" % dir_name))
        if debug:
            _LOG.warning("Running a quick unit test")
            file_name = git.find_file_in_git_tree("test_dbg.py")
            cmd = "pytest %s::Test_dassert1" % file_name
        else:
            # Delete pytest.
            si.pytest_clean_artifacts(".")
            # Run the tests.
            cmd = "run_tests.py --test %s --num_cpus -1" % test_list
        output.append("cmd line='%s'" % cmd)
        rc = si.system(
            cd_cmd + cmd, suppress_output=False, abort_on_error=abort_on_error,
            log_level="echo"
        )
        output.append("  rc=%s" % rc)
        if not abort_on_error and rc != 0:
            output.append(
                "WARNING: unit tests failed: skipping as per user request"
            )
        # Update the function rc.
        is_ok = rc == 0
    return actions, is_ok, output


# #############################################################################

_VALID_ACTIONS = [
    "linter",
    "run_tests",
]


_DEFAULT_ACTIONS = [
    "linter",
    "run_tests",
]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    output = []
    output.append("cmd=%s" % dbg.get_command_line())
    # Print actions.
    actions = prsr.select_actions(args, _VALID_ACTIONS, _DEFAULT_ACTIONS)
    output.append("actions=%s" % actions)
    add_frame = True
    actions_as_str = prsr.actions_to_string(actions, _VALID_ACTIONS, add_frame)
    _LOG.info("\n%s", actions_as_str)
    #
    dir_name = "."
    dbg.dassert_exists(dir_name)
    _LOG.debug("\n%s", prnt.frame("Processing: %s" % dir_name, char1=">"))
    cd_cmd = "cd %s && " % dir_name
    debug = args.debug
    abort_on_error = not args.continue_on_error
    # Check the hash.
    # actions = _merge_master_into_dir(actions, cd_cmd, dir_name, dst_branch)
    # Run linter.
    actions, output = _run_linter_on_dir(actions, cd_cmd, dir_name, debug, abort_on_error, output)
    # Run unit tests.
    actions, is_ok, output = _run_tests_for_dir(
        actions, cd_cmd, dir_name, args.test_list, debug, abort_on_error, output,
    )
    # Report the output.
    _LOG.info("Summary file saved into '%s'", args.summary_file)
    output_as_txt = "\n".join(output)
    io_.to_file(args.summary_file, output_as_txt)
    # Print output.
    txt = io_.from_file(args.summary_file)
    msg = "--> Please attach this to your PR <--"
    print(prnt.frame(msg, char1="-").rstrip("\n"))
    print(txt + "\n")
    print(prnt.line(char="/").rstrip("\n"))
    # Merge.
    if not is_ok:
        _LOG.error(
            "Can't merge since some repo didn't pass the qualification process"
        )
    # Check that everything was executed.
    if actions:
        _LOG.error("actions=%s were not processed", str(actions))


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_branch",
        action="store",
        default="master",
        help="Branch to merge into, typically " "master",
    )
    prsr.add_action_arg(parser, _VALID_ACTIONS, _DEFAULT_ACTIONS)
    parser.add_argument("--test_list", action="store", default="slow")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--continue_on_error",
        action="store_true",
        help="Do not abort on the first error",
    )
    parser.add_argument(
        "--summary_file",
        action="store",
        default="./pr_summary.txt",
        help="File with the summary of the merge",
    )
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
