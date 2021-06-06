import collections
import pprint
from typing import List, Optional, cast

import pandas as pd

import core.config as cfg
import core.config_builders as cfgb
import helpers.unit_test as hut


def _get_test_config_1() -> cfg.Config:
    """
    Build a test config for Crude Oil asset.

    :return: Test config.
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


def _get_test_config_2() -> cfg.Config:
    """
    Build a test config for Gold asset.

    :return: Test config.
    """
    config = cfg.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Gold"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config


# ################################################################################


# # TODO(gp): -> Test_get_config_from_flattened_dict1
class TestGetConfigFromFlattened1(hut.TestCase):
    """
    Test `get_config_from_flattened()`.
    """

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
        config = cfgb.get_config_from_flattened(flattened)
        self.check_string(str(config))

    def test2(self) -> None:
        flattened = collections.OrderedDict(
            [
                (("read_data", "file_name"), "foo_bar.txt"),
                (("read_data", "nrows"), 999),
                (("single_val",), "hello"),
                (("zscore",), cfg.Config()),
            ]
        )
        config = cfgb.get_config_from_flattened(flattened)
        self.check_string(str(config))


# ################################################################################


# TODO(gp): -> Test_get_config_intersection1
class TestGetConfigFromNestedDict1(hut.TestCase):
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
        config = cfgb.get_config_from_nested_dict(nested)
        self.check_string(str(config))

    def test2(self) -> None:
        nested = {
            "read_data": {
                "file_name": "foo_bar.txt",
                "nrows": 999,
            },
            "single_val": "hello",
            "zscore": cfg.Config(),
        }
        config = cfgb.get_config_from_nested_dict(nested)
        self.check_string(str(config))


# ################################################################################


# TODO(gp): -> Test_get_config_intersection1
class TestConfigIntersection(hut.TestCase):
    """
    Test `get_config_intersection()`.
    """

    def test_different_config_intersection(self) -> None:
        """
        Verify that intersection of two different configs is what expected.
        """
        # Prepare actual output of intersection function.
        # TODO(*): Bad unit testing fomr! What are these configs?
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        intersection = cfgb.get_config_intersection([config_1, config_2])
        self.check_string(str(intersection))

    def test_same_config_intersection(self) -> None:
        """
        Verify that intersection of two same configs equals those configs.
        """
        # Prepare test config.
        # TODO(*): Bad unit testing form! What is this config?
        test_config = _get_test_config_1()
        # FInd intersection of two same configs.
        actual_intersection = cfgb.get_config_intersection(
            [test_config, test_config]
        )
        # Verify that intersection is equal to initial config.
        self.assertEqual(str(test_config), str(actual_intersection))


# ################################################################################


# TODO(gp): -> Test_validate_configs1.
class TestCheckSameConfigs(hut.TestCase):
    """
    Test `validate_configs()`.
    """
    def test_check_same_configs_error(self) -> None:
        """
        Verify that an error is raised when same configs are encountered.
        """
        # Create list of configs with duplicates.
        configs = [
            _get_test_config_1(),
            _get_test_config_1(),
            _get_test_config_2(),
        ]
        # Make sure function raises an error.
        with self.assertRaises(AssertionError) as cm:
            cfgb.validate_configs(configs)
        act = str(cm.exception)
        self.check_string(act, fuzzy_match=True)


# ################################################################################


# TODO(gp): -> Test_get_config_difference1
class TestConfigDifference(hut.TestCase):
    def test_varying_config_difference(self) -> None:
        """
        Verify that differing parameters of different configs are what
        expected.
        """
        # Create two different configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Compute variation between configs.
        actual_difference = cfgb.get_config_difference([config_1, config_2])
        # Define expected variation.
        expected_difference = {
            "build_targets.target_asset": ["Crude Oil", "Gold"]
        }
        self.assertEqual(expected_difference, actual_difference)

    def test_same_config_difference(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        # Create test config.
        config = _get_test_config_1()
        # Compute difference between two instances of same config.
        actual_difference = cfgb.get_config_difference([config, config])
        # Verify that the difference is empty.
        self.assertFalse(actual_difference)


# ################################################################################


# TODO(gp): Test_get_configs_dataframe1
class TestGetConfigDataframe(hut.TestCase):
    """
    Compare manually constructed dfs and dfs created by
    `cfgb.get_configs_dataframe` using `pd.DataFrame.equals()`
    """

    def test_all_params(self) -> None:
        """
        Compute and verify dataframe with all config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to dataframe.
        actual_result = cfgb.get_configs_dataframe([config_1, config_2])
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {
                "build_model.activation": ["sigmoid", "sigmoid"],
                "build_targets.target_asset": ["Crude Oil", "Gold"],
                "build_targets.preprocessing.preprocessor": [
                    "tokenizer",
                    "tokenizer",
                ],
                "meta.experiment_result_dir": ["results.pkl", "results.pkl"],
            }
        )
        self.assertTrue(expected_result.equals(actual_result))

    def test_different_params_subset(self) -> None:
        """
        Compute and verify dataframe with all only varying config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to df, keeping only varying params.
        actual_result = cfgb.get_configs_dataframe(
            [config_1, config_2], params_subset="difference"
        )
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_targets.target_asset": ["Crude Oil", "Gold"]}
        )
        self.assertTrue(expected_result.equals(actual_result))

    def test_custom_params_subset(self) -> None:
        """
        Compute and verify dataframe with arbitrary config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to df, keeping arbitrary parameter.
        actual_result = cfgb.get_configs_dataframe(
            [config_1, config_2], params_subset=["build_model.activation"]
        )
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_model.activation": ["sigmoid", "sigmoid"]}
        )
        self.assertTrue(expected_result.equals(actual_result))
