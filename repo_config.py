"""
Contain info specific of `//amp` repo.
"""

import logging
import os
from typing import Dict, List

_LOG = logging.getLogger(__name__)


def _print(msg: str) -> None:
    # _LOG.info(msg)
    print(msg)


# We can't use `__file__` since this file is imported with an exec.
# _print("Importing //amp/repo_config.py")


# #############################################################################
# Repo info.
# #############################################################################


def get_name() -> str:
    return "//amp"


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


# #############################################################################


# //amp runs on:
# - MacOS
#   - Supports Docker privileged mode
#   - The same user and group is used inside the container
#   - Root can also be used
# - Linux (dev server, GitHub CI)
#   - Supports Docker privileged mode
#   - The same user and group is used inside the container
# - Linux (spm-dev4)
#   - Doesn't support Docker privileged mode
#   - A different user and group is used inside the container


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


def is_inside_docker() -> bool:
    """
    Return whether we are inside a container or not.
    """
    # From https://stackoverflow.com/questions/23513045
    return os.path.exists("/.dockerenv")


# End copy.

# We can't rely only on the name of the host to infer where we are running,
# since inside Docker the name of the host is like `01a7e34a82a5`. Of course,
# there is no way to know anything about the host for security reason, so we
# pass this value from the external environment to the container, through env
# vars (e.g., `AM_HOST_NAME`, `AM_HOST_OS_NAME`).


def is_dev1() -> bool:
    # sysname='Darwin'
    # nodename='gpmac.lan'
    # release='19.6.0'
    # version='Darwin Kernel Version 19.6.0: Mon Aug 31 22:12:52 PDT 2020; root:xnu-6153.141.2~1/RELEASE_X86_64'
    # machine='x86_64'
    host_name = os.uname()[1]
    is_dev1_ = host_name == "dev1"
    return is_dev1_


def is_dev4() -> bool:
    """
    Return whether it's running on dev4.
    """
    host_name = os.uname()[1]
    dev4 = "cf-spm-dev4"
    am_host_name = os.environ.get("AM_HOST_NAME")
    _LOG.debug("host_name=%s am_host_name=%s", host_name, am_host_name)
    is_dev4_ = host_name == dev4 or am_host_name == dev4
    return is_dev4_


def is_mac() -> bool:
    host_os_name = os.uname()[0]
    am_host_os_name = os.environ.get("AM_HOST_OS_NAME")
    _LOG.debug(
        "host_os_name=%s am_host_os_name=%s", host_os_name, am_host_os_name
    )
    is_mac_ = host_os_name == "Darwin" or am_host_os_name == "Darwin"
    return is_mac_


def has_docker_sudo() -> bool:
    if is_mac():
        val = True
    elif is_dev1():
        val = True
    else:
        val = False
    return val


# TODO(gp): -> has_docker_privileged_mode
def has_dind_support() -> bool:
    """
    Return whether this repo image supports Docker-in-Docker.
    """
    # `//amp` is executed on systems that can or cannot run Docker in privileged
    # mode.
    # Thus we rely on the approach from https://stackoverflow.com/questions/32144575
    # checking if we can execute.
    # This is equivalent to the env var `AM_DOCKER_HAS_PRIVILEGED_MODE`.
    cmd = "ip link add dummy0 type dummy >/dev/null 2>&1"
    if is_inside_docker() and has_docker_sudo():
        cmd = "sudo " + cmd
    rc = os.system(cmd)
    has_dind = rc == 0
    return has_dind


def use_docker_sibling_containers() -> bool:
    """ """
    # Typically we use either sibling or children containers.
    val = not has_dind_support()
    return val


def use_docker_shared_cache() -> bool:
    """ """
    if is_dev4():
        val = True
    else:
        val = False
    return val


def run_docker_as_root() -> bool:
    """
    Return whether Docker should be run with root user.
    """
    if is_inside_ci():
        # When running as user in GH action we get an error:
        # ```
        # /home/.config/gh/config.yml: permission denied
        # ```
        # see https://github.com/alphamatic/amp/issues/1864
        # So we run as root in GH actions.
        res = True
    else:
        if is_dev4():
            # //lime runs on a system with Docker remap which assumes we don't
            # specify user credentials.
            res = True
        else:
            # In //amp we run as users specifying the user / group id as
            # outside.
            res = True
    return res


def get_docker_user() -> str:
    """
    Return the user that runs Docker, if any.
    """
    if is_dev4():
        val = "spm-sasm"
    else:
        val = ""
    return val


def get_docker_shared_user() -> str:
    """
    Return the group of the user running Docker, if any.
    """
    if is_dev4():
        val = "spm-sasm-fileshare"
    else:
        val = ""
    return val


# #############################################################################
# S3 buckets.
# #############################################################################


def is_AM_S3_available() -> bool:
    # AM bucket is always available.
    val = True
    _LOG.debug("val=%s", val)
    return val


def is_CK_S3_available() -> bool:
    val = True
    if is_mac():
        val = False
    elif is_dev4():
        # CK bucket is not available on dev4.
        val = False
    elif is_inside_ci():
        if get_name() == "//amp":
            # No CK bucket.
            val = False
    _LOG.debug("val=%s", val)
    return val


# #############################################################################


def config_func_to_str() -> str:
    """
    Print the value of all the config functions.
    """
    ret: List[str] = []
    for func_name in [
        "get_name",
        "get_repo_map",
        "get_host_name",
        "get_invalid_words",
        "get_docker_base_image_name",
        "is_dev1",
        "is_dev4",
        "is_inside_ci",
        "is_mac",
        "has_dind_support",
        "use_docker_sibling_containers",
        "use_docker_shared_cache",
        "run_docker_as_root",
        "get_docker_user",
        "is_AM_S3_available",
        "is_CK_S3_available",
    ]:
        try:
            func_value = eval("%s()" % func_name)
        except NameError:
            func_value = "*undef*"
        msg = "%s='%s'" % (func_name, func_value)
        ret.append(msg)
        # _print(msg)
    ret = "\n".join(ret)
    return ret


if False:
    print(config_func_to_str())
    # assert 0
