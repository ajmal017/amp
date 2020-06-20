#!/usr/bin/env python
"""
Reformat python docstring.
"""

import argparse
import logging
import os
import re
import sys
from typing import List, Tuple, Type

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# Use the current dir and not the dir of the executable.
_TMP_DIR = os.path.abspath(os.getcwd() + "/tmp.linter")


# #############################################################################
# Utils.
# #############################################################################


def _write_file_back(file_name: str, txt: List[str], txt_new: List[str]) -> None:
    _dassert_list_of_strings(txt)
    txt_as_str = "\n".join(txt)
    #
    _dassert_list_of_strings(txt_new)
    txt_new_as_str = "\n".join(txt_new)
    #
    if txt_as_str != txt_new_as_str:
        io_.to_file(file_name, txt_new_as_str)


# There are some lints that
#   a) we disagree with (e.g., too many functions in a class)
#       - they are ignored all the times
#   b) are too hard to respect (e.g., each function has a docstring)
#       - they are ignored unless we want to see them
#
# pedantic=2 -> all lints, including a) and b)
# pedantic=1 -> discard lints from a), include b)
# pedantic=0 -> discard lints from a) and b)

# - The default is to run with (-> pedantic=0)
# - Sometimes we want to take a look at the lints that we would like to enforce
#   (-> pedantic=1)
# - In rare occasions we want to see all the lints (-> pedantic=2)


# TODO(gp): joblib asserts when using abstract classes:
#   AttributeError: '_BasicHygiene' object has no attribute '_executable'
# class _Action(abc.ABC):
class _Action:
    """
    Implemented as a Strategy pattern.
    """

    def __init__(self, executable: str = "") -> None:
        self._executable = executable

    # @abc.abstractmethod
    def check_if_possible(self) -> bool:
        """
        Check if the action can be executed.
        """
        raise NotImplementedError

    def execute(self, file_name: str, pedantic: int) -> List[str]:
        """
        Execute the action.

        :param file_name: name of the file to process
        :param pendantic: True if it needs to be run in angry mode
        :return: list of strings representing the output
        """
        dbg.dassert(file_name)
        dbg.dassert_exists(file_name)
        output = self._execute(file_name, pedantic)
        _dassert_list_of_strings(output)
        return output

    # @abc.abstractmethod
    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        raise NotImplementedError


class _Pydocstyle(_Action):
    def __init__(self) -> None:
        executable = "pydocstyle"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        ignore = []
        # http://www.pydocstyle.org/en/2.1.1/error_codes.html
        if pedantic < 2:
            # TODO(gp): Review all of these.
            ignore.extend(
                [
                    # D105: Missing docstring in magic method
                    "D105",
                    # D200: One-line docstring should fit on one line with quotes
                    "D200",
                    # D202: No blank lines allowed after function docstring
                    "D202",
                    # D212: Multi-line docstring summary should start at the first line
                    "D212",
                    # D203: 1 blank line required before class docstring (found 0)
                    "D203",
                    # D205: 1 blank line required between summary line and description
                    "D205",
                    # D400: First line should end with a period (not ':')
                    "D400",
                    # D402: First line should not be the function's "signature"
                    "D402",
                    # D407: Missing dashed underline after section
                    "D407",
                    # D413: Missing dashed underline after section
                    "D413",
                    # D415: First line should end with a period, question mark, or
                    # exclamation point
                    "D415",
                ]
            )
        if pedantic < 1:
            # Disable some lints that are hard to respect.
            ignore.extend(
                [
                    # D100: Missing docstring in public module
                    "D100",
                    # D101: Missing docstring in public class
                    "D101",
                    # D102: Missing docstring in public method
                    "D102",
                    # D103: Missing docstring in public function
                    "D103",
                    # D104: Missing docstring in public package
                    "D104",
                    # D107: Missing docstring in __init__
                    "D107",
                ]
            )
        cmd_opts = ""
        if ignore:
            cmd_opts += "--ignore " + ",".join(ignore)
        #
        cmd = []
        cmd.append(self._executable)
        cmd.append(cmd_opts)
        cmd.append(file_name)
        cmd_as_str = " ".join(cmd)
        # We don't abort on error on pydocstyle, since it returns error if there
        # is any violation.
        _, file_lines_as_str = si.system_to_string(
            cmd_as_str, abort_on_error=False
        )
        # Process lint_log transforming:
        #   linter_v2.py:1 at module level:
        #       D400: First line should end with a period (not ':')
        # into:
        #   linter_v2.py:1: at module level: D400: First line should end with a
        #   period (not ':')
        #
        output: List[str] = []
        #
        file_lines = file_lines_as_str.split("\n")
        lines = ["", ""]
        for cnt, line in enumerate(file_lines):
            line = line.rstrip("\n")
            # _LOG.debug("line=%s", line)
            if cnt % 2 == 0:
                regex = r"(\s(at|in)\s)"
                subst = r":\1"
                line = re.sub(regex, subst, line)
            else:
                line = line.lstrip()
            # _LOG.debug("-> line=%s", line)
            lines[cnt % 2] = line
            if cnt % 2 == 1:
                line = "".join(lines)
                output.append(line)
        return output


# #############################################################################


class _Pyment(_Action):
    def __init__(self) -> None:
        executable = "pyment"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        opts = "-w --first-line False -o reST"
        cmd = self._executable + " %s %s" % (opts, file_name)
        _, output = _tee(cmd, self._executable, abort_on_error=False)
        return output


# #############################################################################
# Actions.
# #############################################################################

# We use the command line instead of API because:
# - some tools don't have a public API
# - this make easier to reproduce / test commands using the command lines and
#   then incorporate in the code
# - it allows to have clear control over options


# Actions and if they read / write files.
# The order of this list implies the order in which they are executed.

# TODO(GP,Sergey): I think this info should be encapsulated in classes.
#  There are mapping that we have to maintain. DRY.
_VALID_ACTIONS_META: List[Tuple[str, str, str, Type[_Action]]] = [
    (
        "check_file_property",
        "r",
        "Check that generic files are valid",
        _CheckFileProperty,
    ),
    (
        "basic_hygiene",
        "w",
        "Clean up (e.g., tabs, trailing spaces)",
        _BasicHygiene,
    ),
    ("compile_python", "r", "Check that python code is valid", _CompilePython),
    ("autoflake", "w", "Removes unused imports and variables", _Autoflake),
    ("isort", "w", "Sort Python import definitions alphabetically", _Isort),
    # Superseded by black.
    # ("yapf", "w", "Formatter for Python code", _Yapf),
    ("black", "w", "The uncompromising code formatter", _Black),
    ("flake8", "r", "Tool For Style Guide Enforcement", _Flake8),
    ("pydocstyle", "r", "Docstring style checker", _Pydocstyle),
    # TODO(gp): Fix this.
    # Not installable through conda.
    # ("pyment", "w", "Create, update or convert docstring", _Pyment),
    ("pylint", "w", "Check that module(s) satisfy a coding standard", _Pylint),
    ("mypy", "r", "Static code analyzer using the hint types", _Mypy),
    ("sync_jupytext", "w", "Create / sync jupytext files", _SyncJupytext),
    ("test_jupytext", "r", "Test jupytext files", _TestJupytext),
    # Superseded by "sync_jupytext".
    # ("ipynb_format", "w", "Format jupyter code using yapf", _IpynbFormat),
    (
        "custom_python_checks",
        "w",
        "Apply some custom python checks",
        _CustomPythonChecks,
    ),
    ("lint_markdown", "w", "Lint txt/md markdown files", _LintMarkdown),
]


# joblib and caching with lru_cache don't get along, so we cache explicitly.
_VALID_ACTIONS = None


def _get_valid_actions() -> List[str]:
    global _VALID_ACTIONS
    if _VALID_ACTIONS is None:
        _VALID_ACTIONS = list(zip(*_VALID_ACTIONS_META))[0]
    return _VALID_ACTIONS  # type: ignore


def _get_default_actions() -> List[str]:
    return _get_valid_actions()


def _get_action_class(action: str) -> _Action:
    """
    Return the function corresponding to the passed string.
    """
    res = None
    for action_meta in _VALID_ACTIONS_META:
        name, rw, comment, class_ = action_meta
        _ = rw, comment
        if name == action:
            dbg.dassert_is(res, None)
            res = class_
    dbg.dassert_is_not(res, None)
    # mypy gets confused since we are returning a class.
    obj = res()  # type: ignore
    return obj


def _remove_not_possible_actions(actions: List[str]) -> List[str]:
    """
    Check whether each action in "actions" can be executed and return a list of
    the actions that can be executed.

    :return: list of strings representing actions
    """
    actions_tmp: List[str] = []
    for action in actions:
        class_ = _get_action_class(action)
        is_possible = class_.check_if_possible()
        if not is_possible:
            _LOG.warning("Can't execute action '%s': skipping", action)
        else:
            actions_tmp.append(action)
    return actions_tmp


def _select_actions(args: argparse.Namespace) -> List[str]:
    valid_actions = _get_valid_actions()
    default_actions = _get_default_actions()
    actions = prsr.select_actions(args, valid_actions, default_actions)
    # Find the tools that are available.
    actions = _remove_not_possible_actions(actions)
    #
    add_frame = True
    actions_as_str = prsr.actions_to_string(
        actions, _get_valid_actions(), add_frame
    )
    _LOG.info("\n%s", actions_as_str)
    return actions


# #############################################################################
# Main.
# #############################################################################


def _main(args: argparse.Namespace) -> int:
    dbg.init_logger(args.log_level)
    #
    if args.test_actions:
        _LOG.warning("Testing actions...")
        _test_actions()
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    if args.no_print:
        global NO_PRINT
        NO_PRINT = True
    if not args.no_cleanup:
        io_.delete_dir(_TMP_DIR)
    else:
        _LOG.warning("Leaving tmp files in '%s'", _TMP_DIR)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Select files.
    parser.add_argument(
        "-f", "--files", nargs="+", type=str, help="Files to process"
    )
    # Debug.
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Generate one file per transformation",
    )
    parser.add_argument(
        "--no_cleanup", action="store_true", help="Do not clean up tmp files"
    )
    # parser.add_argument("--jenkins", action="store_true", help="Run as jenkins")
    # Test.
    parser.add_argument(
        "--test_actions", action="store_true", help="Print the possible actions"
    )
    #
    parser.add_argument(
        "--pedantic",
        action="store",
        type=int,
        default=0,
        help="Pedantic level. 0 = min, 2 = max (all the lints)",
    )
    #
    parser.add_argument(
        "--linter_log",
        default="./docstring_linter_warnings.txt",
        help="File storing the warnings",
    )
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    parser_ = _parse()
    args_ = parser_.parse_args()
    _main(args_)
