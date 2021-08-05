import abc
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import networkx as networ
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg
import helpers.list as hlist

_LOG = logging.getLogger(__name__)


# #############################################################################
# Core node classes
# #############################################################################


# We use a string to represent a node's unique identifier. This type helps
# improve the interface and make the code more readable (e.g., `Dict[Nid, ...]`
# instead of `Dict[str, ...]`).
# TODO(gp): Use this everywhere.
Nid = str

# Name of a method, e.g., `fit` or `predict`.
Method = str


# TODO(gp): If this is private -> _NodeInterface.
class NodeInterface(abc.ABC):
    """
    Abstract node class for creating DAGs of functions.

    Common use case: `Node`s wrap functions with a common method (e.g., `fit`).

    This class provides some convenient introspection (input/output names)
    accessors and, importantly, a unique identifier (`nid`) for building
    graphs of nodes. The `nid` is also useful for config purposes.

    For nodes requiring fit/transform, we can subclass/provide a mixin with
    the desired methods.
    """

    # TODO(gp): Are inputs / output without names useful? If not we can simplify the
    #   interface.
    def __init__(
        self,
        nid: str,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        """
        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of `input_names`. `None` for no names.
        :param outputs: list-like string names of `output_names`. `None` for no names.
        """
        dbg.dassert_isinstance(nid, str)
        dbg.dassert(nid, "Empty string chosen for unique nid!")
        self._nid = nid
        self._input_names = self._init_validation_helper(inputs)
        self._output_names = self._init_validation_helper(outputs)

    @property
    def nid(self) -> str:
        return self._nid

    @property
    def input_names(self) -> List[str]:
        return self._input_names

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    # TODO(gp): Consider using the more common approach with `_check_validity()`.
    @staticmethod
    def _init_validation_helper(items: Optional[List[str]]) -> List[str]:
        """
        Ensure that items are valid and returns the validated items.
        """
        if items is None:
            return []
        # Make sure the items are all non-empty strings.
        for item in items:
            dbg.dassert_isinstance(item, str)
            dbg.dassert_ne(item, "")
        dbg.dassert_no_duplicates(items)
        return items


class Node(NodeInterface):
    """
    A node class that stores and retrieves its output values on a "per-method"
    basis.

    TODO(*): Explain use case.
    """

    def __init__(
        self,
        nid: str,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        """
        Implement the same interface as `NodeInterface`.
        """
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        # Dictionary method name -> output node name -> output.
        self._output_vals: Dict[str, Dict[str, Any]] = {}

    def get_output(self, method: str, name: str) -> Any:
        """
        Return the value of output `name` for the requested `method`.
        """
        dbg.dassert_in(
            method,
            self._output_vals.keys(),
            "%s of node %s has no output!",
            method,
            self.nid,
        )
        dbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        return self._output_vals[method][name]

    def get_outputs(self, method: str) -> Dict[str, Any]:
        """
        Return all the output values for the requested `method`.
        """
        dbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method]

    def _store_output(self, method: str, name: str, value: Any) -> None:
        """
        Store the output for `name` and the specific `method`.
        """
        dbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        # Create a dictionary of values for `method` if it doesn't exist.
        if method not in self._output_vals:
            self._output_vals[method] = {}
        # Assign the requested value.
        self._output_vals[method][name] = value


# #############################################################################
# Class for creating and executing a DAG of nodes.
# #############################################################################


class DAG:
    """
    Class for building DAGs using Nodes.

    The DAG manages node execution and storage of outputs (within
    executed nodes).
    """

    def __init__(
        self, name: Optional[str] = None, mode: Optional[str] = None
    ) -> None:
        """
        Create a DAG.

        :param name: optional str identifier
        :param mode: determines how to handle an attempt to add a node that already
            belongs to the DAG:
            - "strict": asserts
            - "loose": deletes old node (also removes edges) and adds new node. This
              is useful for interactive notebooks and debugging.
        """
        self._dag = networ.DiGraph()
        #
        if name is not None:
            dbg.dassert_isinstance(name, str)
        self._name = name
        #
        if mode is None:
            mode = "strict"
        dbg.dassert_in(
            mode, ["strict", "loose"], "Unsupported mode %s requested!", mode
        )
        self._mode = mode

    # TODO(gp): A bit confusing since other classes have `dag / get_dag` method that
    #  returns a DAG. Also the code does `dag.dag`. Maybe -> `nx_dag()` to say that
    #  we are extracting the networkx data structures.
    @property
    def dag(self) -> networ.DiGraph:
        return self._dag

    # TODO(*): Should we force to always have a name? So mypy can perform more
    #  checks.
    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    def add_node(self, node: Node) -> None:
        """
        Add `node` to the DAG.

        Rely upon the unique nid for identifying the node.
        """
        # In principle, `NodeInterface` could be supported; however, to do so,
        # the `run` methods below would need to be suitably modified.
        # TODO(gp): issubclass?
        # dbg.dassert_isinstance(
        #     node, Node, "Only DAGs of class `Node` are supported!"
        # )
        # NetworkX requires that nodes be hashable and uses hashes for
        # identifying nodes. Because our Nodes are objects whose hashes can
        # change as operations are performed, we use the `Node.nid` as the
        # NetworkX node and the `Node` instance as a `node attribute`, which we
        # identifying internally with the keyword `stage`.
        #
        # Note that this usage requires that nid's be unique within a given
        # DAG.
        if self.mode == "strict":
            dbg.dassert(
                not self._dag.has_node(node.nid),
                "A node with nid=%s already belongs to the DAG!",
                node.nid,
            )
        elif self.mode == "loose":
            # If a node with the same id already belongs to the DAG:
            #   - Remove the node and all of its successors (and their incident
            #     edges)
            #   - Add the new node to the graph.
            # This is useful for notebook research flows, e.g., rerunning
            # blocks that build the DAG incrementally.
            if self._dag.has_node(node.nid):
                _LOG.warning(
                    "Node `%s` is already in DAG. Removing existing node, "
                    "successors, and all incident edges of such nodes. ",
                    node.nid,
                )
                # Remove node successors.
                for nid in networ.descendants(self._dag, node.nid):
                    _LOG.warning("Removing nid=%s", nid)
                    self.remove_node(nid)
                # Remove node.
                _LOG.warning("Removing nid=%s", node.nid)
                self.remove_node(node.nid)
        else:
            dbg.dfatal("Invalid mode='%s'", self.mode)
        # Add node.
        self._dag.add_node(node.nid, stage=node)

    def get_node(self, nid: str) -> Node:
        """
        Implement a convenience node accessor.

        :param nid: unique node id
        """
        dbg.dassert_isinstance(
            nid, str, "Expected str nid but got type %s!", type(nid)
        )
        dbg.dassert(self._dag.has_node(nid), "Node `%s` is not in DAG!", nid)
        return self._dag.nodes[nid]["stage"]  # type: ignore

    def remove_node(self, nid: str) -> None:
        """
        Remove node from DAG and clear any connected edges.
        """
        dbg.dassert(self._dag.has_node(nid), "Node `%s` is not in DAG!", nid)
        self._dag.remove_node(nid)

    def connect(
        self,
        parent: Union[Tuple[str, str], str],
        child: Union[Tuple[str, str], str],
    ) -> None:
        """
        Add a directed edge from parent node output to child node input.

        Raise if the requested edge is invalid or forms a cycle.

        If this is called multiple times on the same nid's but with different
        output/input pairs, the additional input/output pairs are simply added
        to the existing edge (the previous ones are not overwritten).

        :param parent: tuple of the form (nid, output) or nid if it has a single
            output
        :param child: tuple of the form (nid, input) or just nid if it has a single
            input
        """
        # Automatically infer output name when the parent has only one output.
        # Ensure that parent node belongs to DAG (through `get_node` call).
        if isinstance(parent, tuple):
            parent_nid, parent_out = parent
        else:
            parent_nid = parent
            parent_out = hlist.assert_single_element_and_return(
                self.get_node(parent_nid).output_names
            )
        dbg.dassert_in(parent_out, self.get_node(parent_nid).output_names)
        # Automatically infer input name when the child has only one input.
        # Ensure that child node belongs to DAG (through `get_node` call).
        if isinstance(child, tuple):
            child_nid, child_in = child
        else:
            child_nid = child
            child_in = hlist.assert_single_element_and_return(
                self.get_node(child_nid).input_names
            )
        dbg.dassert_in(child_in, self.get_node(child_nid).input_names)
        # Ensure that `child_in` is not already hooked up to an output.
        for nid in self._dag.predecessors(child_nid):
            dbg.dassert_not_in(
                child_in,
                self._dag.get_edge_data(nid, child_nid),
                "`%s` already receiving input from node %s",
                child_in,
                nid,
            )
        # Add the edge along with an `edge attribute` indicating the parent
        # output to connect to the child input.
        kwargs = {child_in: parent_out}
        self._dag.add_edge(parent_nid, child_nid, **kwargs)
        # If adding the edge causes the DAG property to be violated, remove the
        # edge and raise an error.
        if not networ.is_directed_acyclic_graph(self._dag):
            self._dag.remove_edge(parent_nid, child_nid)
            dbg.dfatal(
                f"Creating edge {parent_nid} -> {child_nid} introduces a cycle!"
            )

    def get_sources(self) -> List[Nid]:
        """
        :return: list of nid's of source nodes
        """
        sources = []
        for nid in networ.topological_sort(self._dag):
            if not any(True for _ in self._dag.predecessors(nid)):
                sources.append(nid)
        return sources

    def get_sinks(self) -> List[Nid]:
        """
        :return: list of nid's of sink nodes
        """
        sinks = []
        for nid in networ.topological_sort(self._dag):
            if not any(True for _ in self._dag.successors(nid)):
                sinks.append(nid)
        return sinks

    def get_unique_sink(self) -> Nid:
        """
        Return the only sink node, asserting if there is more than one.
        """
        sinks = self.get_sinks()
        dbg.dassert_eq(
            len(sinks), 1, "There is more than one sink node %s", str(sinks)
        )
        return sinks[0]

    def run_dag(self, method: str) -> Dict[str, Any]:
        """
        Execute entire DAG.

        :param method: method of class `Node` (or subclass) to be executed for
            the entire DAG.
        :return: dict keyed by sink node nid with values from node's
            `get_outputs(method)`.
        """
        sinks = self.get_sinks()
        for nid in networ.topological_sort(self._dag):
            self._run_node(nid, method)
        return {sink: self.get_node(sink).get_outputs(method) for sink in sinks}

    # TODO(gp): We should have a type for Dict[str, Any] since this is everywhere
    #  in the code base and it gets confused with other types (e.g., `**kwargs`).
    def run_leq_node(
        self, nid: str, method: str, progress_bar: bool = True
    ) -> Dict[str, Any]:
        """
        Execute DAG up to (and including) Node `nid` and returns output.

        "leq" refers to the partial ordering on the vertices. This method
        runs a node if and only if there is a directed path from the node to
        `nid`. Nodes are run according to a topological sort.

        :param nid: desired terminal node for execution.
        :param method: `Node` subclass method to be executed.
        :return: result of node nid's `get_outputs(method)`.
        """
        ancestors = filter(
            lambda x: x in networ.ancestors(self._dag, nid),
            networ.topological_sort(self._dag),
        )
        # The `ancestors` filter only returns nodes strictly less than `nid`,
        # and so we need to add `nid` back.
        nids = itertools.chain(ancestors, [nid])
        if progress_bar:
            nids = tqdm(list(nids), desc="run_leq_node")
        for n in nids:
            _LOG.debug("Executing node '%s'", n)
            self._run_node(n, method)
        return self.get_node(nid).get_outputs(method)

    def _run_node(self, nid: str, method: str) -> None:
        """
        Run a single node.

        This method DOES NOT run (or re-run) ancestors of `nid`.
        """
        _LOG.debug("Node nid=`%s` executing method `%s`...", nid, method)
        kwargs = {}
        for pre in self._dag.predecessors(nid):
            kvs = self._dag.edges[[pre, nid]]
            pre_node = self.get_node(pre)
            for k, v in kvs.items():
                # Retrieve output from store.
                kwargs[k] = pre_node.get_output(method, v)
        _LOG.debug("kwargs are %s", kwargs)
        node = self.get_node(nid)
        try:
            output = getattr(node, method)(**kwargs)
        except Exception as e:
            raise Exception(
                f"An exception occurred in node '{nid}'.\n{str(e)}"
            ) from e
        for out in node.output_names:
            node._store_output(  # pylint: disable=protected-access
                method, out, output[out]
            )
