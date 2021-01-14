import abc
import logging
from typing import List, Optional

import core.config as cfg
from core.dataflow.core import DAG

_LOG = logging.getLogger(__name__)


# TODO(Paul): Consider moving this to `core.py`.
class DagBuilder(abc.ABC):
    """
    Abstract class for creating DAGs.

    Concrete classes must specify:
      - a default configuration (which may depend upon variables used in class
        initialization)
      - the construction of a DAG
    """

    def __init__(self, nid_prefix: Optional[str] = None) -> None:
        """

        :param nid_prefix: a namespace ending with "/" for graph node naming.
            This may be useful if the DAG built by the builder is either built
            upon an existing DAG or will be built upon subsequently.
        """
        # If no nid prefix is specified, make it an empty string to simplify
        # the implementation of helpers.
        self._nid_prefix = nid_prefix or ""
        # Make sure the nid_prefix ends with "/" (unless it is "").
        if self._nid_prefix and not self._nid_prefix.endswith("/"):
            _LOG.warning(
                "Appended '/' to nid_prefix. To avoid this warning, "
                "only pass nid prefixes ending in '/'."
            )
            self._nid_prefix += "/"

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    @property
    @abc.abstractmethod
    def input_nids(self) -> List[str]:
        """
        Input node identifiers.
        """

    @property
    @abc.abstractmethod
    def result_nids(self) -> List[str]:
        """
        Result node identifiers.
        """

    @property
    @abc.abstractmethod
    def methods(self) -> List[str]:
        """
        Methods supported by the DAG.
        """

    @abc.abstractmethod
    def get_config_template(self) -> cfg.Config:
        """
        Return a config template compatible with `self.get_dag`.

        :return: a valid configuration for `self.get_dag`, possibly with some
            "dummy" required paths.
        """

    @abc.abstractmethod
    def get_dag(self, config: cfg.Config, dag: Optional[DAG] = None) -> DAG:
        """
        Build DAG given `config`.

        WARNING: This function modifies `dag` in-place.
        TODO(Paul): Consider supporting deep copies for `dtf.DAG`.

        :param config: configures DAG. It is up to the client to guarantee
            compatibility. The result of `self.get_config_template` should
            always be compatible following template completion.
        :param dag: may or may not have nodes. If the DAG already has nodes,
            it is up to the client to ensure that there are no `nid` (node id)
            collisions, which can be ensured through the use of `nid_prefix`.
            If this parameter is `None`, then a new `dtf.DAG` object is
            created.
        :return: `dag` with all builder operations applied
        """

    def _get_nid(self, stage_name: str) -> str:
        nid = self._nid_prefix + stage_name
        return nid
