import logging
import pprint

import core.config as cfg
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# TODO(gp): Use `self.assert_equal()` instead of `self.check_string()`.


class Test_config1(hut.TestCase):
    def test_set1(self) -> None:
        """
        Set a key and print a flat config.
        """
        config = cfg.Config()
        config["hello"] = "world"
        act = str(config)
        exp = r"""
        hello: world
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_check_python1(self) -> None:
        """
        Test serialization/deserialization for a flat config.
        """
        config = self._get_flat_config1()
        #
        act = self._check_roundtrip_transformation(config)
        exp = r"""
        # config=
        hello: world
        foo: [1, 2, 3]
        # code=
        Config([('hello', 'world'), ('foo', [1, 2, 3])])
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_get1(self) -> None:
        """
        Test Config.get().
        """
        config = cfg.Config()
        config["nrows"] = 10000
        # Look up the key.
        self.assertEqual(config["nrows"], 10000)
        self.assertEqual(config.get("nrows", None), 10000)
        # Look up a non-existent key.
        self.assertEqual(config.get("nrows_tmp", None), None)

    def test_get2(self) -> None:
        """
        Test Config.get() with missing key.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        act = str(config)
        exp = """
        nrows: 10000
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        with self.assertRaises(AssertionError) as cm:
            _ = config["read_data2"]
        act = str(cm.exception)
        exp = """
        * Failed assertion *
        'read_data2' in 'odict_keys(['nrows', 'read_data', 'single_val', 'zscore'])'
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        with self.assertRaises(AssertionError) as cm:
            _ = config["read_data"]["file_name2"]
        act = str(cm.exception)
        exp = """
        * Failed assertion *
        'file_name2' in 'odict_keys(['file_name', 'nrows'])'
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]["file_name2"]
        elem = config["read_data"]["file_name"]
        self.assertEqual(elem, "foo_bar.txt")

    def test_nested_config_print1(self) -> None:
        """
        Test print nested config.
        """
        config = self._get_nested_config1()
        act = str(config)
        exp = r"""
        nrows: 10000
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_nested_config_to_python1(self) -> None:
        """
        Test to_python() nested config.
        """
        config = self._get_nested_config1()
        act = config.to_python()
        exp = r"""
        Config([('nrows', 10000), ('read_data', Config([('file_name', 'foo_bar.txt'), ('nrows', 999)])), ('single_val', 'hello'), ('zscore', Config([('style', 'gaz'), ('com', 28)]))])
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_config6(self) -> None:
        """
        Test serialization / deserialization for nested config.
        """
        config = self._get_nested_config1()
        # Check.
        act = self._check_roundtrip_transformation(config)

        exp = r"""
        # config=
        nrows: 10000
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28
        # code=
        Config([('nrows', 10000), ('read_data', Config([('file_name', 'foo_bar.txt'), ('nrows', 999)])), ('single_val', 'hello'), ('zscore', Config([('style', 'gaz'), ('com', 28)]))])
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)
        
    def test_config7(self) -> None:
        """
        Compare two different styles of building a nested config.
        """
        config1 = self._get_nested_config1()
        config2 = self._get_nested_config2()
        #
        self.assertEqual(str(config1), str(config2))

    def test_hierarchical_getitem1(self) -> None:
        """
        Test accessing the config with hierarchical access.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        elem1 = config[("read_data", "file_name")]
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_getitem2(self) -> None:
        """
        Test accessing the config with hierarchical access with correct and
        incorrect paths.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2", "file_name")]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2")]
        with self.assertRaises(AssertionError):
            _ = config[["read_data2"]]
        #
        elem = config[("read_data", "file_name")]
        self.assertEqual(elem, "foo_bar.txt")

    def test_hierarchical_get1(self) -> None:
        """
        Show that hierarchical access is equivalent to chained access.
        """
        config = self._get_nested_config1()
        elem1 = config.get(("read_data", "file_name"), None)
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_get2(self) -> None:
        """
        Test `get()` with hierarchical access.
        """
        config = self._get_nested_config1()
        elem = config.get(("read_data2", "file_name"), "hello_world1")
        self.assertEqual(elem, "hello_world1")
        elem = config.get(("read_data2", "file_name2"), "hello_world2")
        self.assertEqual(elem, "hello_world2")
        elem = config.get(("read_data", "file_name2"), "hello_world3")
        self.assertEqual(elem, "hello_world3")

    def test_hierarchical_update1(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("write_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val2"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore2")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config1.update(config2)
        # Check.
        act = str(config1)
        exp = r"""
        read_data:
          file_name: foo_bar.txt
          nrows: 999
        single_val: hello
        zscore:
          style: gaz
          com: 28
        write_data:
          file_name: baz.txt
          nrows: 999
        single_val2: goodbye
        zscore2:
          style: gaz
          com: 28
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)


    def test_hierarchical_update2(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("read_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore")
        config_tmp["style"] = "super"
        #
        config_tmp = config2.add_subconfig("extra_zscore")
        config_tmp["style"] = "universal"
        config_tmp["tau"] = 32
        #
        config1.update(config2)
        # Check.
        act = str(config1)
        exp = r"""
        read_data:
          file_name: baz.txt
          nrows: 999
        single_val: goodbye
        zscore:
          style: super
          com: 28
        extra_zscore:
          style: universal
          tau: 32
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)


    def test_hierarchical_update_empty_nested_config1(self) -> None:
        """
        Generate a config of `{"key1": {"key0": }}` structure.
        """
        subconfig = cfg.Config()
        subconfig.add_subconfig("key0")
        #
        config = cfg.Config()
        config_tmp = config.add_subconfig("key1")
        config_tmp.update(subconfig)
        #
        expected_result = cfg.Config()
        config_tmp = expected_result.add_subconfig("key1")
        config_tmp.add_subconfig("key0")
        self.assertEqual(str(config), str(expected_result))

    def test_config_with_function(self) -> None:
        config = cfg.Config()
        config[
            "filters"
        ] = "[(<function _filter_relevance at 0x7fe4e35b1a70>, {'thr': 90})]"
        expected_result = "filters: [(<function _filter_relevance>, {'thr': 90})]"
        actual_result = str(config)
        self.assertEqual(actual_result, expected_result)

    def test_config_with_object(self) -> None:
        config = cfg.Config()
        config[
            "dag_builder"
        ] = "<dataflow_p1.task2538_pipeline.ArPredictorBuilder object at 0x7fd7a9ddd190>"
        expected_result = "dag_builder: <dataflow_p1.task2538_pipeline.ArPredictorBuilder object>"
        actual_result = str(config)
        self.assertEqual(actual_result, expected_result)

    def test_flatten1(self) -> None:
        config = cfg.Config()
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config_tmp = config.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        flattened = config.flatten()
        # TODO(*): `sort_dicts` param new in 3.8.
        # string = pprint.pformat(flattened, sort_dicts=False)
        act = pprint.pformat(flattened)
        exp = r"""
OrderedDict([(('read_data', 'file_name'), 'foo_bar.txt'),
             (('read_data', 'nrows'), 999),
             (('single_val',), 'hello'),
             (('zscore', 'style'), 'gaz'),
             (('zscore', 'com'), 28)])
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_flatten2(self) -> None:
        config = cfg.Config()
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config.add_subconfig("zscore")
        #
        flattened = config.flatten()
        # TODO(*): `sort_dicts` param new in 3.8.
        # string = pprint.pformat(flattened, sort_dicts=False)
        act = pprint.pformat(flattened)
        exp = r"""
        OrderedDict([(('read_data', 'file_name'), 'foo_bar.txt'),
             (('read_data', 'nrows'), 999),
             (('single_val',), 'hello'),
             (('zscore',), )])
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)


    @staticmethod
    def _get_flat_config1() -> cfg.Config:
        """
        Build a flat (i.e., non-nested) config.
        """
        config = cfg.Config()
        config["hello"] = "world"
        config["foo"] = [1, 2, 3]
        return config

    @staticmethod
    def _get_nested_config1() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config.add_subconfig("read_data")
        config["read_data"]["file_name"] = "foo_bar.txt"
        config["read_data"]["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config.add_subconfig("zscore")
        config["zscore"]["style"] = "gaz"
        config["zscore"]["com"] = 28
        return config

    @staticmethod
    def _get_nested_config2() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config_tmp = config.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        return config

    def _check_roundtrip_transformation(self, config: cfg.Config) -> str:
        """
        Convert a config into Python code and back.

        :return: signature of the test
        """
        # Convert a config to Python code.
        code = config.to_python()
        _LOG.debug("code=%s", code)
        # Build a config from Python code.
        config2 = cfg.Config.from_python(code)
        # Verify that the round-trip transformation is correct.
        self.assertEqual(str(config), str(config2))
        # Build the signature of the test.
        act = []
        act.append("# config=\n%s" % str(config))
        act.append("# code=\n%s" % str(code))
        act = "\n".join(act)
        return act


class Test_subtract_config1(hut.TestCase):
    def test_test1(self) -> None:
        config1 = cfg.Config()
        config1[("l0",)] = "1st_floor"
        config1[["l1", "l2"]] = "2nd_floor"
        config1[["r1", "r2", "r3"]] = [1, 2]
        #
        config2 = cfg.Config()
        config2["l0"] = "Euro_1nd_floor"
        config2[["l1", "l2"]] = "2nd_floor"
        config2[["r1", "r2", "r3"]] = [1, 2]
        #
        diff = cfg.subtract_config(config1, config2)
        # Check.
        act = str(diff)
        exp = r"""
        l0: 1st_floor
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_test2(self) -> None:
        config1 = cfg.Config()
        config1[("l0",)] = "1st_floor"
        config1[["l1", "l2"]] = "2nd_floor"
        config1[["r1", "r2", "r3"]] = [1, 2, 3]
        #
        config2 = cfg.Config()
        config2["l0"] = "Euro_1nd_floor"
        config2[["l1", "l2"]] = "2nd_floor"
        config2[["r1", "r2", "r3"]] = [1, 2]
        config2["empty"] = cfg.Config()
        #
        diff = cfg.subtract_config(config1, config2)
        # Check.
        act = str(diff)
        exp = r"""
        l0: 1st_floor
        r1:
          r2:
            r3: [1, 2, 3]
        """.lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match=True)


# TODO(gp): Unit tests all the functions.
