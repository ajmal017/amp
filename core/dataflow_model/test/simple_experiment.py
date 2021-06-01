import logging
import os

import core.config as cfg
import core.config_builders as cfgb
import core.dataflow as cdataf
import helpers.pickle_ as hpickl

_LOG = logging.getLogger(__name__)

def run_experiment(config: cfg.Config) -> None:
    """
    This experiment fails/succeeds depending on the return code stored inside
    the config.
    """
    _LOG.info("config=\n%s", config)
    if config is None:
        raise ValueError("No config provided")
    if config["fail"]:
        raise ValueError("Failure")
    _LOG.info("Success")