import logging
import os
from typing import List, Tuple

import pytest

import core.config as cfg
import core.config_builders as cfgb
import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


@pytest.mark.slow
class TestRunNotebook1(hut.TestCase):
    def test1(self) -> None:
        """
        Run an experiment with 2 notebooks (without any failure) serially.
        """
        cmd = (
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()' "
            "--skip_on_error "
            "--num_threads 'serial'"
        )
        exp = ""
        rc = self._run_notebook_helper(cmd, exp)
        self.assertEqual(rc, 0)

    def test2(self) -> None:
        """
        Run an experiment with 2 notebooks (without any failure) with 2 threads.
        """
        cmd = (
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()' "
            "--num_threads 2"
        )
        exp = ""
        rc = self._run_notebook_helper(cmd, exp)
        self.assertEqual(rc, 0)

    def test3(self) -> None:
        """
        Run an experiment with 3 notebooks (with one failing) using 3 threads.
        """
        cmd = (
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()' "
            "--num_threads 3"
        )
        cmd += " 2>&1 >/dev/null"
        _LOG.warning("This command is supposed to fail")
        exp = ""
        rc = self._run_notebook_helper(cmd, exp)
        self.assertNotEqual(rc, 0)

    @staticmethod
    def _get_files() -> Tuple[str, str]:
        amp_path = git.get_amp_abs_path()
        exec_file = os.path.join(
            amp_path, "dev_scripts/notebooks/run_notebook.py"
        )
        dbg.dassert_file_exists(exec_file)
        notebook_file = os.path.join(
            amp_path, "dev_scripts/notebooks/test/simple_notebook.ipynb"
        )
        dbg.dassert_file_exists(notebook_file)
        return exec_file, notebook_file

    def _run_notebook_helper(self, cmd: str, exp: str) -> int:
        # Build command line.
        dst_dir = self.get_scratch_space()
        exec_file, notebook_file = self._get_files()
        cmd = (
            f"{exec_file} "
            f"--dst_dir {dst_dir} "
            f"--notebook {notebook_file} " +
            cmd)
        # Run command.
        rc = si.system(cmd, abort_on_error=False)
        # Compute and compare the dir signature.
        act = hut.get_dir_signature(dst_dir)
        self.assert_equal(act, exp)
        return rc


def build_configs1() -> List[cfg.Config]:
    """
    Build 2 configs that won't make the notebook to fail.
    """
    config_template = cfg.Config()
    config_template["fail"] = None
    configs = cfgb.build_multiple_configs(
        config_template, {("fail",): (False, False)}
    )
    # Duplicate configs are not allowed, so we need to add identifiers for
    # them.
    for i, config in enumerate(configs):
        config["id"] = i
    return configs


def build_configs2() -> List[cfg.Config]:
    """
    Build 3 configs with one failing.
    """
    config_template = cfg.Config()
    config_template["fail"] = None
    configs = cfgb.build_multiple_configs(
        config_template, {("fail",): (False, False, True)}
    )
    # Duplicate configs are not allowed, so we need to add identifiers for
    # them.
    for i, config in enumerate(configs):
        config["id"] = i
    return configs
