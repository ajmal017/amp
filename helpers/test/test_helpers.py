import logging
import os
from typing import Optional

import numpy as np
import pandas as pd
import pytest

import helpers.csv as csv
import helpers.env as env
import helpers.git as git
import helpers.io_ as io_
import helpers.list as hlist
import helpers.printing as prnt
import helpers.s3 as hs3
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################
# csv.py
# #############################################################################


class Test_convert_csv_to_dict(ut.TestCase):
    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        actual_result = csv.convert_csv_to_dict(test_csv_path, remove_nans=True)
        expected_result = {
            "col1": ["a", "b", "c", "d"],
            "col2": ["a", "b"],
            "col3": ["a", "b", "c"],
        }
        self.assertEqual(actual_result, expected_result)


class Test_from_typed_csv(ut.TestCase):
    """This test is aimed to check the opportunity to load correctly
    .csv file with dtype param, which exist in .types prefix file.
    And finally it checks that dtypes of loaded dataframe didn't change
    compared with the original one.
    """
    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        test_csv_types_path = os.path.join(dir_name, "test.csv.types")
        actual_result = csv.from_typed_csv(test_csv_path).dtypes.apply(lambda x: x.name).to_dict()
        expected_result = {
            'A': 'int64', 'B': 'float64', 'C': 'object', 'D': 'object', 'E': 'int64'
        }
        self.assertEqual(actual_result, expected_result)


class Test_to_typed_csv(ut.TestCase):
    """This test is aimed to check whether the function 'to_typed_csv'
    create file with '.types' prefix or not.
    """
    def test1(self) -> None:
        dir_name = self.get_input_dir()
        test_csv_path = os.path.join(dir_name, "test.csv")
        test_csv_types_path = os.path.join(dir_name, "test.csv.types")
        df = pd.read_csv(test_csv_path)
        csv.to_typed_csv(df, test_csv_path)
        self.assertTrue(os.path.exists(test_csv_types_path))
        os.remove(test_csv_types_path)


# #############################################################################
# env.py
# #############################################################################


class Test_env1(ut.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = env.get_system_signature()
        _LOG.debug(txt)


# #############################################################################
# git.py
# #############################################################################


class Test_git1(ut.TestCase):
    """Unfortunately we can't check the outcome of some of these functions
    since we don't know in which dir we are running.

    Thus we test that the function
    completes and visually inspect the outcome, if needed.
    TODO(gp): If we have Jenkins on AM side we could test for the outcome at
     least in that set-up.
    """

    def test_get_git_name1(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_is_inside_submodule1(self) -> None:
        func_call = "git.is_inside_submodule()"
        self._helper(func_call)

    def test_get_client_root1(self) -> None:
        func_call = "git.get_client_root(super_module=True)"
        self._helper(func_call)

    def test_get_client_root2(self) -> None:
        func_call = "git.get_client_root(super_module=False)"
        self._helper(func_call)

    def test_get_path_from_git_root1(self) -> None:
        file_name = "helpers/test/test_helpers.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)

    def test_get_repo_symbolic_name1(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_get_repo_symbolic_name2(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=False)"
        self._helper(func_call)

    def test_get_modified_files1(self) -> None:
        func_call = "git.get_modified_files()"
        self._helper(func_call)

    def test_get_previous_committed_files1(self) -> None:
        func_call = "git.get_previous_committed_files()"
        self._helper(func_call)

    def test_git_log1(self) -> None:
        func_call = "git.git_log()"
        self._helper(func_call)

    @pytest.mark.not_docker
    def test_git_log2(self) -> None:
        func_call = "git.git_log(my_commits=True)"
        self._helper(func_call)

    def test_git_all_repo_symbolic_names1(self) -> None:
        func_call = "git.get_all_repo_symbolic_names()"
        self._helper(func_call)

    def test_git_all_repo_symbolic_names2(self) -> None:
        all_repo_sym_names = git.get_all_repo_symbolic_names()
        for repo_sym_name in all_repo_sym_names:
            repo_github_name = git.get_repo_github_name(repo_sym_name)
            _LOG.debug(
                ut.to_string("repo_sym_name")
                + " -> "
                + ut.to_string("repo_github_name")
            )
            git.get_repo_prefix(repo_github_name)
            _LOG.debug(
                ut.to_string("repo_sym_name")
                + " -> "
                + ut.to_string("repo_sym_name_tmpA")
            )

    def test_get_branch_name1(self) -> None:
        _ = git.get_branch_name()

    @pytest.mark.not_docker(reason="Issue #3482")
    @pytest.mark.skipif('si.get_user_name() == "jenkins"', reason="#781")
    @pytest.mark.skipif(
        'git.get_repo_symbolic_name(super_module=False) == "alphamatic/amp"'
    )
    def test_get_submodule_hash1(self) -> None:
        dir_name = "amp"
        _ = git.get_submodule_hash(dir_name)

    def test_get_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

    def test_get_remote_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

    @pytest.mark.not_docker(reason="Issue #3482")
    @pytest.mark.skipif(
        'si.get_server_name() == "docker-instance"', reason="Issue #1522, #1831"
    )
    def test_report_submodule_status1(self) -> None:
        dir_names = ["."]
        short_hash = True
        _ = git.report_submodule_status(dir_names, short_hash)

    def _helper_group_hashes(
        self, head_hash: str, remh_hash: str, subm_hash: Optional[str], exp: str
    ) -> None:
        act = git._group_hashes(head_hash, remh_hash, subm_hash)
        self.assert_equal(act, exp)

    def test_group_hashes1(self) -> None:
        head_hash = "a2bfc704"
        remh_hash = "a2bfc704"
        subm_hash = None
        exp = "head_hash = remh_hash = a2bfc704"
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def test_group_hashes2(self) -> None:
        head_hash = "22996772"
        remh_hash = "92167662"
        subm_hash = "92167662"
        exp = """head_hash = 22996772
remh_hash = subm_hash = 92167662"""
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def test_group_hashes3(self) -> None:
        head_hash = "7ea03eb6"
        remh_hash = "7ea03eb6"
        subm_hash = "7ea03eb6"
        exp = "head_hash = remh_hash = subm_hash = 7ea03eb6"
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    @staticmethod
    def _helper(func_call: str) -> None:
        act = eval(func_call)
        _LOG.debug("%s=%s", func_call, act)


# #############################################################################
# io_.py
# #############################################################################


class Test_load_df_from_json(ut.TestCase):
    def test1(self) -> None:
        test_json_path = os.path.join(self.get_input_dir(), "test.json")
        actual_result = io_.load_df_from_json(test_json_path)
        expected_result = pd.DataFrame(
            {
                "col1": ["a", "b", "c", "d"],
                "col2": ["a", "b", np.nan, np.nan],
                "col3": ["a", "b", "c", np.nan],
            }
        )
        actual_result = prnt.dataframe_to_str(actual_result)
        expected_result = prnt.dataframe_to_str(expected_result)
        self.assertEqual(actual_result, expected_result)


# #############################################################################
# list.py
# #############################################################################


class Test_list_1(ut.TestCase):
    def test_find_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(list_out, [])

    def test_find_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(set(list_out), set("a f".split()))

    def test_remove_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d".split())

    def test_remove_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d e f".split())

    def test_remove_duplicates3(self) -> None:
        list_ = "a b c a d e f f".split()
        list_ = list(reversed(list_))
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "f e d a c b".split())


# #############################################################################
# numba.py
# #############################################################################


class Test_numba_1(ut.TestCase):
    def test1(self) -> None:
        # TODO(gp): Implement this.
        pass


# #############################################################################
# printing.py
# #############################################################################


class Test_printing1(ut.TestCase):
    def test_color_highlight1(self) -> None:
        for c in prnt.COLOR_MAP:
            _LOG.debug(prnt.color_highlight(c, c))


# #############################################################################
# s3.py
# #############################################################################


class Test_s3_1(ut.TestCase):
    def test_get_path1(self) -> None:
        file_path = (
            "s3://default00-bucket/kibot/All_Futures_Continuous_Contracts_daily"
        )
        bucket_name, file_path = hs3.parse_path(file_path)
        self.assertEqual(bucket_name, "default00-bucket")
        self.assertEqual(
            file_path, "kibot/All_Futures_Continuous_Contracts_daily"
        )

    @pytest.mark.slow
    def test_ls1(self) -> None:
        file_path = os.path.join(
            hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_daily"
        )
        file_names = hs3.ls(file_path)
        # We rely on the fact that Kibot data is not changing.
        self.assertEqual(len(file_names), 253)
