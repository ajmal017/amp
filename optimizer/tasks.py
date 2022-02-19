import logging
import os

import helpers.hversion as hversi

# Expose the pytest targets.
# Extract with:
# > i print_tasks --as-code
from helpers.lib_tasks import set_default_params  # This is not an invoke target.
from helpers.lib_tasks import (  # noqa: F401  # pylint: disable=unused-import
    docker_bash,
    docker_build_local_image,
    docker_build_prod_image,
    docker_cmd,
    docker_images_ls_repo,
    docker_jupyter,
    docker_kill,
    docker_login,
    docker_ps,
    docker_pull,
    docker_push_dev_image,
    docker_release_all,
    docker_release_dev_image,
    docker_release_prod_image,
    docker_stats,
    docker_tag_local_image_as_dev,
    find,
    find_check_string_output,
    find_test_class,
    find_test_decorator,
    fix_perms,
    gh_create_pr,
    gh_issue_title,
    gh_login,
    gh_workflow_list,
    gh_workflow_run,
    git_add_all_untracked,
    git_branch_copy,
    git_branch_diff_with_base,
    git_branch_diff_with_master,
    git_branch_files,
    git_branch_next_name,
    git_clean,
    # TODO(gp): -> git_branch_create
    git_create_branch,
    # TODO(gp): -> git_patch_create
    git_create_patch,
    git_delete_merged_branches,
    # TODO(gp): -> git_files_list
    git_files,
    # TODO(gp): -> git_files_last_commit_
    git_last_commit_files,
    # TODO(gp): -> git_master_merge
    git_merge_master,
    git_pull,
    # TODO(gp): -> git_master_fetch
    git_fetch_master,
    # TODO(gp): -> git_branch_rename
    git_rename_branch,
    integrate_create_branch,
    integrate_diff_dirs,
    integrate_diff_overlapping_files,
    integrate_files,
    integrate_find_files,
    lint,
    print_setup,
    print_tasks,
    pytest_clean,
    run_blank_tests,
    run_fast_slow_tests,
    run_fast_tests,
    run_slow_tests,
    run_superslow_tests,
    traceback,
)
from im_v2.im_lib_tasks import (  # noqa: F401  # pylint: disable=unused-import
    im_docker_cmd,
    im_docker_down,
)

_LOG = logging.getLogger(__name__)


# #############################################################################
# Setup.
# #############################################################################


# TODO(gp): Move it to lib_tasks.
ECR_BASE_PATH = os.environ["AM_ECR_BASE_PATH"]


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "amp_tmp",
    "BASE_IMAGE": "amp_opt",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod",
    "USE_ONLY_ONE_DOCKER_COMPOSE": True,
}


set_default_params(default_params)
