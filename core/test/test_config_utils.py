import collections
import pprint
from typing import List, Optional, cast

import pandas as pd

import core.config as cfg
import core.config_utils as cfgut
import helpers.printing as hprint
import helpers.unit_test as hut


def _get_test_config1() -> cfg.Config:
    """
    Build a test config for Crude Oil asset.
    """
    config = cfg.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Crude Oil"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config


def _get_test_config2() -> cfg.Config:
    """
    Same as `_get_test_config1()` but with "Gold" instead of "Crude Oil" for target
    asset.
    """
    config = _get_test_config1().copy()
    config["target_asset"] = "Gold"
    return config


def _get_test_config3() -> cfg.Config:
    """
    :return: Test config.
    """
    config = _get_test_config1().copy()
    config["hello"] = "world"
    return config


# ################################################################################

class Test_validate_configs1(hut.TestCase):

    def test_check_same_configs_error(self) -> None:
        """
        Verify that an error is raised when duplicated configs are encountered.
        """
        # Create list of configs with duplicates.
        configs = [
            _get_test_config1(),
            _get_test_config1(),
            _get_test_config2(),
        ]
        # Make sure function raises an error.
        with self.assertRaises(AssertionError) as cm:
            cfgut.validate_configs(configs)
        act = str(cm.exception)
        self.check_string(act, fuzzy_match=True)

    def test1(self) -> None:
        """
        Test configs without duplicates.
        """
        configs = [
            _get_test_config1(),
            _get_test_config2(),
            _get_test_config3(),
        ]
        cfgut.validate_configs(configs)


# ################################################################################


class Test_get_config_from_flattened_dict1(hut.TestCase):

    def test1(self) -> None:
        flattened = collections.OrderedDict(
            [
                (("read_data", "file_name"), "foo_bar.txt"),
                (("read_data", "nrows"), 999),
                (("single_val",), "hello"),
                (("zscore", "style"), "gaz"),
                (("zscore", "com"), 28),
            ]
        )
        config = cfgut.get_config_from_flattened_dict(flattened)
        act = str(config)
        exp = r"""
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28"""
        exp = hprint.dedent(exp)
        self.assert_equal(act, exp, fuzzy_match=False)


    def test2(self) -> None:
        flattened = collections.OrderedDict(
            [
                (("read_data", "file_name"), "foo_bar.txt"),
                (("read_data", "nrows"), 999),
                (("single_val",), "hello"),
                (("zscore",), cfg.Config()),
            ]
        )
        config = cfgut.get_config_from_flattened_dict(flattened)
        act = str(config)
        exp = r"""
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
        """
        exp = hprint.dedent(exp)
        self.assert_equal(act, exp, fuzzy_match=False)


# ################################################################################


class Test_get_config_from_nested_dict1(hut.TestCase):
    """
    Test `get_config_from_nested_dict()`.
    """

    def test1(self) -> None:
        nested = {
            "read_data": {
                "file_name": "foo_bar.txt",
                "nrows": 999,
            },
            "single_val": "hello",
            "zscore": {
                "style": "gaz",
                "com": 28,
            },
        }
        config = cfgut.get_config_from_nested_dict(nested)
        act = str(config)
        exp = r"""
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=False)

    def test2(self) -> None:
        nested = {
            "read_data": {
                "file_name": "foo_bar.txt",
                "nrows": 999,
            },
            "single_val": "hello",
            "zscore": cfg.Config(),
        }
        config = cfgut.get_config_from_nested_dict(nested)
        act = str(config)
        exp = r"""
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=False)


# ################################################################################


class Test_intersect_configs1(hut.TestCase):

    def test_same_config(self) -> None:
        """
        Verify that intersection of two same configs equals those configs.
        """
        # Prepare test config.
        config = _get_test_config1()
        # FInd intersection of two same configs.
        actual = cfgut.intersect_configs([config, config])
        # Verify that intersection is equal to initial config.
        self.assertEqual(str(actual), str(config))

    def test1(self) -> None:
        """
        Verify that intersection of two different configs is what is expected.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        intersection = cfgut.intersect_configs([config1, config2])
        act = str(intersection)
        exp = r"""
        build_model:
          activation: sigmoid
        build_targets:
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=False)


# ################################################################################


class Test_subtract_configs1(hut.TestCase):

    def test_same_config(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        config = _get_test_config1()
        diff = cfgut.subtract_config(config, config)
        # The difference should be empty.
        self.assertFalse(diff)

    def test1(self) -> None:
        """
        Verify that differing parameters of different configs are what
        expected.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        act = cfgut.subtract_config(config1, config2)
        # ??????
        exp = {
            "build_targets.target_asset": ["Crude Oil", "Gold"]
        }
        self.assert_equal(str(act), str(exp))


class Test_diff_configs1(hut.TestCase):

    def test_same_config(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        config = _get_test_config1()
        act = cfgut.diff_configs([config, config])
        exp = [cfg.Config(), cfg.Config()]
        self.assert_equal(str(act), str(exp))

    def test1(self) -> None:
        config1 = _get_test_config1()
        exp = """
        build_model:
          activation: sigmoid
        build_targets:
          target_asset: Crude Oil
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl"""
        exp = hprint.dedent(exp)
        self.assert_equal(str(config1), exp)
        #
        config2 = _get_test_config2()
        exp = """
        build_model:
          activation: sigmoid
        build_targets:
          target_asset: Crude Oil
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl
        target_asset: Gold"""
        exp = hprint.dedent(exp)
        self.assert_equal(str(config2), exp)
        #
        act = cfgut.diff_configs([config1, config2])
        exp = [cfg.Config(), cfgut.get_config_from_nested_dict({"target_asset": "Gold"})]
        self.assert_equal(str(act), str(exp))

    def test2(self) -> None:
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        config3 = _get_test_config3()
        #
        act = cfgut.diff_configs([config1, config2, config3])
        exp = [cfg.Config(),
               cfgut.get_config_from_nested_dict({"target_asset": "Gold"}),
               cfgut.get_config_from_nested_dict({"hello": "world"})]
        self.assert_equal(str(act), str(exp))


# # ################################################################################
#
#
# # TODO(gp): Test_get_configs_dataframe1
# class TestGetConfigDataframe(hut.TestCase):
#     """
#     Compare manually constructed dfs and dfs created by
#     `cfgut.get_configs_dataframe` using `pd.DataFrame.equals()`
#     """
#
#     def test_all_params(self) -> None:
#         """
#         Compute and verify dataframe with all config parameters.
#         """
#         # Get two test configs.
#         config1 = _get_test_config1()
#         config2 = _get_test_config2()
#         # Convert configs to dataframe.
#         actual_result = cfgut.get_configs_dataframe([config1, config2])
#         # Create expected dataframe and one with function.
#         expected_result = pd.DataFrame(
#             {
#                 "build_model.activation": ["sigmoid", "sigmoid"],
#                 "build_targets.target_asset": ["Crude Oil", "Gold"],
#                 "build_targets.preprocessing.preprocessor": [
#                     "tokenizer",
#                     "tokenizer",
#                 ],
#                 "meta.experiment_result_dir": ["results.pkl", "results.pkl"],
#             }
#         )
#         self.assertTrue(expected_result.equals(actual_result))
#
#     def test_different_params_subset(self) -> None:
#         """
#         Compute and verify dataframe with all only varying config parameters.
#         """
#         # Get two test configs.
#         config1 = _get_test_config1()
#         config2 = _get_test_config2()
#         # Convert configs to df, keeping only varying params.
#         actual_result = cfgut.get_configs_dataframe(
#             [config1, config2], params_subset="difference"
#         )
#         # Create expected dataframe and one with function.
#         expected_result = pd.DataFrame(
#             {"build_targets.target_asset": ["Crude Oil", "Gold"]}
#         )
#         self.assertTrue(expected_result.equals(actual_result))
#
#     def test_custom_params_subset(self) -> None:
#         """
#         Compute and verify dataframe with arbitrary config parameters.
#         """
#         # Get two test configs.
#         config1 = _get_test_config1()
#         config2 = _get_test_config2()
#         # Convert configs to df, keeping arbitrary parameter.
#         actual_result = cfgut.get_configs_dataframe(
#             [config1, config2], params_subset=["build_model.activation"]
#         )
#         # Create expected dataframe and one with function.
#         expected_result = pd.DataFrame(
#             {"build_model.activation": ["sigmoid", "sigmoid"]}
#         )
#         self.assertTrue(expected_result.equals(actual_result))
