# encoding:utf-8
import pandas as pd
from py2neo.data import Node, Relationship
from py2neo import NodeMatcher, RelationshipMatcher, Graph


def create_node(graph, node_tuple):
    """
    create the new node in the graph if the node doesn't exist
    :param graph:
    :param node_tuple:
    :return:
    """
    label = node_tuple[0]
    name = node_tuple[1]

    # check dup
    matcher = NodeMatcher(graph)
    first = matcher.match(label,name=name).first()
    if first:
        return

    # create node
    node = Node(label,name=name)
    graph.create(node)


def create_relationship(graph, rel_tuple):
    """
    create the new rel in the graph if the rel doesn't exist, create new nodes if necessary
    :param graph:
    :param rel_tuple:
    :return:
    """
    label_1 = str(rel_tuple[0][0])
    name_1 = str(rel_tuple[0][1])
    label_2 = str(rel_tuple[2][0])
    name_2 = str(rel_tuple[2][1])
    rel_type = str(rel_tuple[1][0])
    rel_name = str(rel_tuple[1][1])

    # check dup
    matcher = NodeMatcher(graph)
    node_1 = matcher.match(label_1,name=name_1).first()
    node_2 = matcher.match(label_2,name=name_2).first()
    if node_1 and node_2:
        r_matcher = RelationshipMatcher(graph)
        first_rel = r_matcher.match([node_1,node_2],rel_type,name=rel_name).first()
        if first_rel:
            return
    elif node_1 and not node_2:
        node_2 = Node(label_2,name=name_2)
    elif node_2 and not node_1:
        node_1 = Node(label_1,name=name_1)
    else:
        node_1 = Node(label_1,name=name_1)
        node_2 = Node(label_2,name=name_2)

    # create rel
    rel = Relationship(node_1,rel_type,node_2,name = rel_name)
    graph.create(rel)


def update_node_property(graph, prop_tuple):
    """
    update or add property of a node, create a new one if the node doesn't exist
    :param graph:
    :param prop_tuple:
    :return:
    """
    label_type = str(prop_tuple[0])
    name = str(prop_tuple[1])
    prop_key = str(prop_tuple[2])
    prop_value = str(prop_tuple[3])

    matcher = NodeMatcher(graph)
    node = matcher.match(label_type,name=name).first()
    if not node:
        node = Node(label_type,name=name)
    graph.merge(node)
    prop_value_set = set() if node[prop_key] is None else set(node[prop_key].split(', '))
    prop_value_set.add(prop_value)
    node[prop_key] = ', '.join(prop_value_set)
    graph.push(node)


def query_with_cypher(graph, cypher):
    """
    return a list of res dict with a cypher query
    :param graph:
    :param cypher:
    :return:
    """
    return graph.run(cypher).data()


if __name__ == '__main__':
    graph = Graph("http://localhost:7474",
                  username="neo4j",
                  password="123456")

    res = query_with_cypher(graph,"MATCH (n) RETURN distinct keys(n)")
    print(pd.DataFrame(res))