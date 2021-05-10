import networkx as networ

import IPython

import helpers.dbg as dbg
import helpers.io_ as hio

# TODO(gp): Are these methods of DAG since we need to extract .dag.dag?

def draw(graph: networ.Graph) -> IPython.core.display.Image:
    """
    Render NetworkX graph in a notebook.
    """
    dbg.dassert_isinstance(graph, networ.Graph)
    # Convert the graph into pygraphviz object.
    agraph = networ.nx_agraph.to_agraph(graph)
    image = IPython.display.Image(agraph.draw(format="png", prog="dot"))
    print(type(image))
    return image


def to_file(graph: networ.Graph, file_name: str = "graph.png") -> None:
    """
    Save NetworkX graph to file.
    """
    dbg.dassert_isinstance(graph, networ.Graph)
    # Convert the graph into pygraphviz object.
    agraph = networ.nx_agraph.to_agraph(graph)
    hio.create_enclosing_dir(file_name)
    agraph.draw(file_name, prog="dot")
    #image = agraph.draw(format="png", prog="dot")
    # with open(file_name, "wb") as file:
    #     file.write(image)
    assert 0