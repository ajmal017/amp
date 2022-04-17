"""
This file contains info specific of `//amp` repo.
"""

import logging
import os
from typing import Dict, List

_LOG = logging.getLogger(__name__)
# We can't use `__file__` since this file is imported with an exec.
_LOG.debug("Importing //amp/repo_config.py")


def get_repo_map() -> Dict[str, str]:
    """
    Return a mapping of short repo name -> long repo name.
    """
    repo_map: Dict[str, str] = {}
    return repo_map


# TODO(gp): -> get_gihub_host_name
def get_host_name() -> str:
    return "github.com"


def get_invalid_words() -> List[str]:
    return []


def get_docker_base_image_name() -> str:
    """
    Return a base name for docker image.
    """
    base_image_name = "amp"
    return base_image_name


# Copied from `system_interaction.py` to avoid circular imports.
def is_inside_ci() -> bool:
    """
    Return whether we are running inside the Continuous Integration flow.
    """
    if "CI" not in os.environ:
        ret = False
    else:
        ret = os.environ["CI"] != ""
    return ret


def run_docker_as_root() -> bool:
    """
    Return whether Docker should be run with root user.
    """
    # We want to run as user anytime we can.
    res = False
    if is_inside_ci():
        # When running as user in GH action we get an error:
        # ```
        # /home/.config/gh/config.yml: permission denied
        # ```
        # see https://github.com/alphamatic/amp/issues/1864
        # So we run as root in GH actions.
        res = True
    return res


# TODO(gp): use_docker_privileged_mode
def has_dind_support() -> bool:
    """
    Return whether this repo image supports Docker-in-Docker.
    """
    host_name = os.uname()[1]
    _LOG.debug("host_name=%s", host_name)
    if host_name == "cf-spm-dev4":
        val = False
    else:
        val = True
    return val


def use_docker_sibling_containers() -> bool:
    """ """
    # Typically we use either sibling or children containers.
    val = not has_dind_support()
    return val


def use_docker_shared_cache() -> bool:
    """ """
    host_name = os.uname()[1]
    if host_name == "cf-spm-dev4":
        # Unclear
        val = True
    else:
        val = False
    return val
