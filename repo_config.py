"""
This file contains info specific of this repo.
"""

from typing import Dict, List


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


def run_docker_as_root() -> bool:
    """
    Return whether Docker should be run with root user.
    """
    res = True
    if os.environ.get("CI", False):
        # Running as user in GH action we get an error:
        # ```
        # /home/.config/gh/config.yml: permission denied
        # ```
        # see https://github.com/alphamatic/amp/issues/1864
        # We run as root.
        res = True
    return res


def has_dind_support() -> bool:
    """
    Return whether this repo image supports Docker-in-Docker.
    """
    return True
