import logging
from typing import List, Match, Optional, Tuple

import helpers.traceback_helper as htrace
import helpers.unit_test as hut

import helpers.dbg as dbg
import helpers.system_interaction as hsinte



_LOG = logging.getLogger(__name__)


class Test_Traceback1(hut.TestCase):
    def test_parse1(self) -> None:
        txt = """

        TEST
Traceback
    TEST
Traceback (most recent call last):
  File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "/app/amp/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
NameError: name 'repo_short_name' is not defined
    TEST TEST TEST
"""
        act_cfile, act_traceback = htrace.parse_traceback(txt)
        _LOG.debug("act_cfile=\n%s", act_cfile)
        # pylint: disable=line-too-long
        exp_cfile = [
            (
                "test/test_lib_tasks.py",
                27,
                "test_get_gh_issue_title2:act = ltasks._get_gh_issue_title(issue_id, repo)",
            ),
            (
                "lib_tasks.py",
                1265,
                "_get_gh_issue_title:task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)",
            ),
            (
                "helpers/git.py",
                397,
                'get_task_prefix_from_repo_short_name:if repo_short_name == "amp":',
            ),
        ]
        # pylint: enable=line-too-long
        self.assert_equal(
            htrace.cfile_to_str(act_cfile), htrace.cfile_to_str(exp_cfile)
        )
        #
        exp_traceback = """
Traceback (most recent call last):
  File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "/app/amp/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
        """.rstrip().lstrip()
        self.assert_equal(act_traceback, exp_traceback)

    def test_parse2(self) -> None:
        """
        Parse an empty traceback file.
        """
        txt = """

        TEST
Traceback
    TEST TEST TEST
"""
        #
        act_cfile, act_traceback = htrace.parse_traceback(txt)
        _LOG.debug("act_cfile=\n%s", act_cfile)
        exp_cfile = []
        self.assert_equal(
            htrace.cfile_to_str(act_cfile), htrace.cfile_to_str(exp_cfile)
        )
        #
        exp_traceback = None
        self.assertIs(act_traceback, exp_traceback)

    def test_parse3(self) -> None:
        """
        Parse a traceback file with / without purifying files.
        """
        purify_from_client = True
        # pylint: disable=line-too-long
        exp_cfile = []
        # pylint: enable=line-too-long
        exp_traceback = None
        self._test_parse_helper(purify_from_client, exp_cfile, exp_traceback)

    def _test_parse_helper(self, purify_from_client: bool,
                           exp_cfile: List[htrace.CFILE_ROW],
                           exp_traceback: str) -> None:
        """
        Parse a traceback file with / without purifying files.
        """
        #
        txt = """
Traceback (most recent call last):
  File "./amp/core/dataflow_model/run_pipeline.py", line 146, in <module>
    _main(_parse())
  File "./amp/core/dataflow_model/run_pipeline.py", line 105, in _main
    configs = cdtfut.get_configs_from_command_line(args)
  File "/app/amp/core/dataflow_model/utils.py", line 228, in get_configs_from_command_line
    "config_builder": args.config_builder,
"""
        act_cfile, act_traceback = htrace.parse_traceback(txt, purify_from_client=purify_from_client)
        _LOG.debug("act_cfile=\n%s", act_cfile)
        self.assert_equal(
            htrace.cfile_to_str(act_cfile), htrace.cfile_to_str(exp_cfile)
        )
        #
        self.assertIs(act_traceback, exp_traceback)
