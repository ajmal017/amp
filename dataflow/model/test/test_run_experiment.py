import logging
import os
import unittest.mock as umock
from typing import Any, List

import pytest

import dev_scripts.test.test_run_notebook as trnot
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

# #############################################################################


def _run_experiment_helper(
    self: Any, cmd_opts: List[str], exp_pass: bool, exp: str
) -> None:
    """
    Build, run, and check a `run_experiment` command line.
    """
    amp_path = hgit.get_amp_abs_path()
    # Get the executable.
    exec_file = os.path.join(amp_path, "dataflow/model/run_experiment.py")
    hdbg.dassert_file_exists(exec_file)
    # Build command line.
    dst_dir = self.get_scratch_space()
    cmd = [
        f"{exec_file}",
        "--experiment_builder dataflow.model.test.simple_experiment.run_experiment",
        f"--dst_dir {dst_dir}",
    ]
    trnot.run_cmd_line(self, cmd, cmd_opts, dst_dir, exp, exp_pass)


# #############################################################################
# TestRunExperimentSuccess1
# #############################################################################


# TODO(gp): We could factor out more common code between here and the corresponding
#  unit tests in TestRunNotebook*. The difference is only in the command lines.
class TestRunExperimentSuccess1(hunitest.TestCase):
    """
    Run an experiment list of two experiment that both succeed.

    These tests are equivalent to `TestRunNotebook1` but using the
    `run_experiment.py` flow instead of `run_notebook.py`.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_experiment.1.log
        $SCRATCH_SPACE/result_1/success.txt"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - serially
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 'serial'",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - with 2 threads
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################
# TestRunExperimentFail2
# #############################################################################


class TestRunExperimentFail2(hunitest.TestCase):
    """
    Run experiments that fail.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_experiment.1.log
        $SCRATCH_SPACE/result_1/success.txt
        $SCRATCH_SPACE/result_2
        $SCRATCH_SPACE/result_2/config.pkl
        $SCRATCH_SPACE/result_2/config.txt
        $SCRATCH_SPACE/result_2/run_experiment.2.log"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - serially
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads serial",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_serial2(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - serially
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads serial",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - with 2 threads
        - aborting on error

        Same as `test_serial1` but using 2 threads.
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads 2",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel2(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - with 2 threads
        - skipping on error

        Same as `test_serial1` but using 2 threads.
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################
# TestRunExperimentArchiveOnS3
# #############################################################################


class TestRunExperimentArchiveOnS3(hunitest.TestCase):
    """
    Run experiments that succeed and archive the results on S3.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/output_metadata.json
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt"""

    # TODO(gp): This test needs write access to S3 for `infra` user. For now we
    #  gave access to the entire bucket. It would be better to give only access to
    #  `tmp`.
    @pytest.mark.slow
    @umock.patch.dict(hs3.os.environ, {"AM_AWS_PROFILE": "am"})
    def test_serial1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - serially
        """
        scratch_dir = self.get_scratch_space()
        aws_profile = "am"
        # Actions.
        create_s3_archive = True
        check_s3_archive = True
        clean_up_s3_archive = True
        # Create archive on S3.
        if create_s3_archive:
            output_metadata_file = f"{scratch_dir}/output_metadata.json"
            s3_tmp_path = self.get_s3_scratch_dir()
            cmd_opts = [
                "--config_builder 'dev_scripts.test.test_run_notebook.build_configs3()'",
                "--num_threads 'serial'",
                f"--aws_profile '{aws_profile}'",
                f"--s3_path {s3_tmp_path}",
                f"--json_output_metadata {output_metadata_file}",
                "--archive_on_S3",
                # f"-v DEBUG",
            ]
            #
            exp_pass = True
            _run_experiment_helper(
                self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME
            )
            # Read the metadata back.
            output_metadata = hparser.read_output_metadata(output_metadata_file)
            s3_path = output_metadata["s3_path"]
            _LOG.debug("s3_path=%s", s3_path)
        if check_s3_archive:
            # s3_path = "s3://alphamatic-data/tmp/tmp.20210802-121908.scratch.tgz"
            tgz_dst_dir = hs3.retrieve_archived_data_from_s3(
                s3_path, scratch_dir, aws_profile
            )
            _LOG.info("Retrieved to %s", tgz_dst_dir)
            # Check the content.
            cmd = f"ls -1 {tgz_dst_dir}"
            files = hsystem.system_to_files(cmd)
            _LOG.debug("Files are:\n%s", files)
            # TODO(gp): We should check that the output looks like:
            # EXPECTED_OUTCOME = r"""# Dir structure
            #     $SCRATCH_SPACE/result_0
            #     $SCRATCH_SPACE/result_0/config.pkl
            #     $SCRATCH_SPACE/result_0/config.txt
            #     $SCRATCH_SPACE/result_0/run_experiment.0.log
            #     $SCRATCH_SPACE/result_0/success.txt"""
        if clean_up_s3_archive:
            # Clean up S3.
            s3fs_ = hs3.get_s3fs(aws_profile)
            hs3.dassert_path_exists(s3_path, s3fs_)
            s3fs_.rm(s3_path)
            hs3.dassert_path_not_exists(s3_path, s3fs_)
