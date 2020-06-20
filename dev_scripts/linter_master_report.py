#!/usr/bin/env python

"""
Compute the change of lints of a branch with respect to the point where it was
branched from.
"""

import argparse
import logging
import os
import sys
from typing import List, Optional, Tuple

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _clean_up_git_client() -> None:
    cmd = "git reset --hard"
    si.system(cmd)


def _lint_branch(base_commit_sha: str) -> Tuple[int, bool, str]:
    _LOG.info("Linting branch after changes")
    msg: List[str] = []
    # Clean up the client from all linter artifacts.
    _clean_up_git_client()
    # Sync at the HEAD of the branch.
    linter_out = "./tmp_lint_branch.txt"
    cmd = f"linter.py -t {base_commit_sha} --linter_log {linter_out}"
    branch_lints = si.system(
        cmd, suppress_output=False, abort_on_error=False, log_level="echo"
    )
    # Remote the lints.
    _LOG.info("Branch lints: %s", branch_lints)
    # Check if the Git client is dirty.
    changed_files = git.get_modified_files()
    branch_dirty = len(changed_files) != 0
    _LOG.info("Branch dirty: %s", branch_dirty)
    tmp = "%d files not linted:\n%s" + prnt.space("\n".join(changed_files))
    tmp = "```\n" + tmp + "\n```\n"
    msg.append(tmp)
    # Read the lints reported from the linter.
    tmp = io_.from_file(linter_out)
    tmp = "```\n" + tmp + "\n```\n"
    msg.append(tmp)
    #
    msg = "\n".join(msg)
    # Clean up the client from all linter artifacts.
    _clean_up_git_client()
    return branch_lints, branch_dirty, msg


def _lint_master(base_commit_sha: str, mod_files: List[str]) -> Tuple[int, int]:
    _LOG.info("Linting branch before changes")
    # Clean up the client from all linter artifacts.
    _clean_up_git_client()
    # Check out master at the requested hash.
    cmd = f"git checkout {base_commit_sha} --recurse-submodules"
    si.system(cmd)
    mod_files_as_str = " ".join(mod_files)
    linter_out = "./tmp_lint_master.txt"
    cmd = f"linter.py --files {mod_files_as_str} --linter_log {linter_out}"
    master_lints = si.system(
        cmd, suppress_output=False, abort_on_error=False, log_level="echo"
    )
    # Report the lints.
    _LOG.info("Master lints: %s", master_lints)
    # Check if the Git client is dirty.
    changed_files = git.get_modified_files()
    master_dirty = len(changed_files) != 0
    _LOG.info("Master dirty: %s", master_dirty)
    #
    _clean_up_git_client()
    return master_lints, master_dirty


def _calculate_exit_status(
    branch_dirty_status: bool, master_lints: int, branch_lints: int,
) -> Tuple[int, str]:
    """
    Calculate status and error message.
    """
    exit_status = 0
    errors = []
    if branch_dirty_status:
        errors.append("**ERROR**: Run `linter.py. -b` locally before merging.")
        exit_status = 1
    if master_lints > 0:
        errors.append("**WARNING**: Your branch has lints. Please fix them.")
    if branch_lints > master_lints:
        exit_status = 1
        errors.append("**ERROR**: You introduced more lints. Please fix them.")
    return exit_status, "\n".join(errors)


def _calculate_stats(
    base_commit_sha: str,
    head_commit_sha: str,
    head_branch_name: str,
    build_url: Optional[str] = None,
) -> Tuple[int, str]:
    """
    Compute the statistics from the linter when run on a branch vs master.

    :param base_commit_sha: hash of the branch to compare
    :param: head_branch_name: name of the branch to be compared
    :param: branch_name: branch name for the report
    :param: build_url: jenkins build url
    :return: an integer representing the exit status and an error message.
    """
    dir_name = "."
    # Find the files that are modified in the branch.
    # TODO(gp): Not sure what to do with files that are not present in the
    #  branch.
    remove_files_non_present = False
    mod_files = git.get_modified_files_in_branch(
        dir_name,
        base_commit_sha,
        remove_files_non_present=remove_files_non_present,
    )
    #
    branch_lints, branch_dirty, linter_message = _lint_branch(base_commit_sha)
    master_lints, master_dirty = _lint_master(base_commit_sha, mod_files)
    # Prepare a message and exit status.
    master_dirty_status = master_dirty > 0
    branch_dirty_status = branch_dirty > 0
    exit_status, errors = _calculate_exit_status(
        branch_dirty_status, master_lints, branch_lints
    )
    # Package the message.
    # pylint: disable=line-too-long,pointless-string-statement
    """
    # Results of the linter build

    Console output: http://52.15.239.182:8080/job/p1.ci_tests/5408/consoleFull

    - Master (sha: caadd4b)
      - Number of lints: 0
      - Dirty (i.e., linter was not run): False
    - Branch (PartTask3000_Look_for_copper_price_data_in_WIND: f3ade60)
      - Number of lints: 0
      - Dirty (i.e., linter was not run): True
    The number of lints introduced with this change: 0

    ERROR: Run linter.py. -b locally before merging.

    cmd line='/var/lib/jenkins/workspace/p1.ci_tests/5408/linter_tests/amp/dev_scripts/linter.py -t caadd4bc1c0998eb754c39ec675bd885a4376171'
    actions=14 ['check_file_property', 'basic_hygiene', 'compile_python', 'autoflake', 'isort', 'black', 'flake8', 'pydocstyle', 'pylint', 'mypy', 'sync_jupytext', 'test_jupytext', 'custom_python_checks', 'lint_markdown']
    file_names=1 ['./automl/notebooks/PartTask3000_Look_for_copper_price_data_in_WIND.py']
    �[0m [sync_jupytext]
    �[0m [test_jupytext]
    ./automl/notebooks/PartTask3000_Look_for_copper_price_data_in_WIND.py: Your code has been rated at 10.00/10 (previous run: 10.00/10, +0.00) [pylint]
    Fixing /var/lib/jenkins/workspace/p1.ci_tests/5408/linter_tests/automl/notebooks/PartTask3000_Look_for_copper_price_data_in_WIND.py [isort]
    num_lints=0
    """
    message: List[str] = []
    # Report title.
    message.append("# Results of the linter build")
    # Console url for Jenkins.
    console_url = os.path.join(str(build_url), "consoleFull")
    console_message = "Console output: "
    if build_url is not None:
        console_message += console_url
    else:
        console_message += "no console output"
    message.append(console_message)
    # Statuses and additional info.
    message.append(f"- Master (sha: {base_commit_sha})")
    message.append(f"  - Number of lints: {master_lints}")
    message.append(f"  - Dirty (i.e., linter was not run): {master_dirty_status}")
    message.append(f"- Branch ({head_branch_name}: {head_commit_sha})")
    message.append(f"  - Number of lints: {branch_lints}")
    message.append(f"  - Dirty (i.e., linter was not run): {branch_dirty_status}")
    diff_lints = branch_lints - master_lints
    message.append(
        f"\nThe number of lints introduced with this change: {diff_lints}"
    )
    # Format report in order.
    message = "\n".join(message)
    message += "\n\n" + errors
    message += "\n" + linter_message
    return exit_status, message


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--jenkins", action="store_true", help="Run as Jenkins",
    )
    parser.add_argument("--base_commit_sha", type=str, required=False, help="")
    parser.add_argument("--head_branch_name", type=str, required=False, help="")
    parser.add_argument("--head_commit_sha", type=str, required=False, help="")
    parser.add_argument("--debug", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(args: argparse.Namespace) -> int:
    dbg.init_logger(args.log_level)
    # Test that the Git client is clean.
    git.verify_client_clean(abort_on_error=not args.debug)
    #
    build_url = None
    if args.jenkins:
        # Fetch the environment variable as passed by Jenkins from Git web-hook.
        base_commit_sha = os.environ["data_pull_request_base_sha"]
        head_branch_name = os.environ["data_pull_request_head_ref"]
        head_commit_sha = os.environ["data_pull_request_head_sha"]
        build_url = os.environ["BUILD_URL"]
    else:
        # Use passed parameters from command line or infer some defaults from
        # the current git client.
        base_commit_sha = args.base_commit_sha or "master"
        head_branch_name = args.head_branch_name or git.get_branch_name()
        head_commit_sha = args.head_commit_sha or git.get_current_commit_hash()
    #
    rc, message = _calculate_stats(
        base_commit_sha, head_commit_sha, head_branch_name, build_url
    )
    # Save the result or print it to the screen.
    if args.jenkins:
        io_.to_file("./tmp_message.txt", message)
        io_.to_file("./tmp_exit_status.txt", str(rc))
    else:
        print(message)
    #
    _clean_up_git_client()
    # Clean up the branch bringing to the original status.
    cmd = f"git checkout {head_branch_name} --recurse-submodules"
    si.system(cmd)
    return rc


if __name__ == "__main__":
    parser_ = _parse()
    args_ = parser_.parse_args()
    rc_ = _main(args_)
    sys.exit(rc_)
