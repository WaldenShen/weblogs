import networkx as nx
from networkx.drawing.nx_agraph import write_dot, read_dot

a = nx.Graph(read_dot("dot1.dot"))
b = nx.Graph(read_dot("dot2.dot"))

write_dot(nx.compose(a, b), "tttt.dot")
