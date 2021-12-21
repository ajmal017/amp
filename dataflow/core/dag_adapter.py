"""
Import as:

import dataflow.core.dag_adapter as dtfcodaada
"""

from typing import Any, Dict, List

import core.config as cconfig
import dataflow.core.builders as dtfcorbuil
import dataflow.core.dag as dtfcordag
import dataflow.core.node as dtfcornode
import helpers.dbg as hdbg
import helpers.printing as hprint


class DagAdapter(dtfcorbuil.DagBuilder):
    """
    Adapt a DAG builder by overriding part of the config and appending nodes.
    """

    def __init__(
        self,
        dag_builder: dtfcorbuil.DagBuilder,
        overriding_config: Dict[str, Any],
        nodes_to_append: List[dtfcornode.Node],
    ):
        """
        Constructor.

        :param dag_builder: a `DagBuilder` containing a single sink
        :param overriding_config: a template `Config` containing the fields to
            override. Note that this `Config` can still be a template, i.e.,
            containing dummies that are finally overwritten by callers.
        :param nodes_to_append: list of tuples `(node name, constructor)` storing
            the nodes to append to the DAG created from `dag_builder`.
            The node constructor function should accept only the `nid` and the
            configuration dict, while all the other inputs need to be already
            specified.
        """
        super().__init__()
        hdbg.dassert_isinstance(dag_builder, dtfcorbuil.DagBuilder)
        self._dag_builder = dag_builder
        hdbg.dassert_isinstance(overriding_config, cconfig.Config)
        self._overriding_config = overriding_config
        hdbg.dassert_container_type(nodes_to_append, list, tuple)
        self._nodes_to_append = nodes_to_append

    def __str__(self) -> str:
        txt = []
        #
        txt.append("dag_builder=")
        txt.append(hprint.indent(str(self._dag_builder), 2))
        #
        txt.append("overriding_config=")
        txt.append(hprint.indent(str(self._overriding_config), 2))
        #
        txt.append("nodes_to_append=")
        txt.append(hprint.indent("\n".join(map(str, self._nodes_to_append)), 2))
        #
        txt = "\n".join(txt)
        return txt

    def get_config_template(self) -> cconfig.Config:
        config = self._dag_builder.get_config_template()
        config.update(self._overriding_config)
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        dag = self._dag_builder.get_dag(config, mode=mode)
        # To append a node we need to assume that there is a single sink node.
        tail_nid = dag.get_unique_sink()
        for stage, node in self._nodes_to_append:
            nid = dag._get_nid(stage)
            hdbg.dassert_in(nid, config)
            node = node_ctor(
                nid,
                **config[nid].to_dict(),
            )
            tail_nid = self._append(dag, nid, node)
        _ = tail_nid
        return dag
