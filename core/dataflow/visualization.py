import networkx as networ

import IPython

import helpers.dbg as dbg
import helpers.io_ as hio

# TODO(gp): Think if this is part of the DAG interface.


# TODO(gp): Pass a DAG through the interface.
def draw(graph: networ.Graph) -> IPython.core.display.Image:
    """
    Render NetworkX graph in a notebook.
    """
    dbg.dassert_isinstance(graph, networ.Graph)
    # Convert the graph into pygraphviz object.
    agraph = networ.nx_agraph.to_agraph(graph)
    image = IPython.display.Image(agraph.draw(format="png", prog="dot"))
    return image


def to_file(graph: networ.Graph, file_name: str = "graph.png") -> None:
    """
    Save NetworkX graph to file.
    """
    dbg.dassert_isinstance(graph, networ.Graph)
    # Convert the graph into pygraphviz object.
    agraph = networ.nx_agraph.to_agraph(graph)
    # Save to file.
    hio.create_enclosing_dir(file_name)
    agraph.draw(file_name, prog="dot")