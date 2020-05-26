# encoding:utf-8
from py2neo import Graph
import codecs
from graph import graph_data
from graph import graph_op
import pandas as pd
import os


def import_kg_by_file(graph, dir):
    node_df = pd.read_csv(dir + 'kg_node.txt', sep='\t', encoding='utf-8')
    node_list = graph_data.get_node_data(node_df)
    for node in node_list:
        graph_op.create_node(graph, node)

    rel_df = pd.read_csv(dir + 'kg_rel.txt', sep='\t', encoding='utf-8')
    rel_list = graph_data.get_relationship_data(rel_df)
    for rel in rel_list:
        graph_op.create_relationship(graph, rel)

    prop_df = pd.read_csv(dir + 'kg_node_prop.txt', sep='\t', encoding='utf-8')
    prop_list = graph_data.get_property_data(prop_df)
    for prop in prop_list:
        graph_op.update_node_property(graph, prop)


def export_dict(graph, out_dir):
    '''
    export the names, property keys, rel names of the nodes as a dict file for word cut
    :param graph:
    :param out_dir:
    :return:
    '''
    # names

    name_dict = codecs.open(out_dir + 'kg_name.dict', 'w', 'utf-8')
    cql = "MATCH (n) RETURN DISTINCT n.name,labels(n)"
    names_data = graph_op.query_with_cypher(graph, cql)
    for item in names_data:
        if item['labels(n)']:
            word = item['n.name']
            attr = '__nodeName_' + item['labels(n)'][0] + '__'
            name_dict.write(word + " 999999 " + attr + "\n")
    name_dict.close()

    # property keys
    prop_key_dict = codecs.open(out_dir + 'kg_prop_key.dict', 'w', 'utf-8')
    cql = "MATCH (n) RETURN DISTINCT keys(n)"
    prop_key = graph_op.query_with_cypher(graph, cql)
    for item in prop_key:
        for word in item['keys(n)']:
            prop_key_dict.write(word + " 999999 __propKey__\n")
    prop_key_dict.close()

    # rel names
    rel_name_dict = codecs.open(out_dir + 'kg_rel_name.dict', 'w', 'utf-8')
    cql = "MATCH (e1)-[r]->(e2) RETURN DISTINCT r.name"
    names_data = graph_op.query_with_cypher(graph, cql)
    for item in names_data:
        word = item['r.name']
        rel_name_dict.write(word + " 999999 __relName__\n")
    rel_name_dict.close()

    # property values
    prop_value_dict = codecs.open(out_dir + 'kg_prop_value.dict', 'w', 'utf-8')
    cql = "MATCH (n) RETURN EXTRACT(key IN keys(n) | {key: key, value: n[key]}) AS prop_kv"
    prop_value = graph_op.query_with_cypher(graph, cql)
    for k_v in prop_value:
        for item in k_v['prop_kv']:
            word = item['value']
            if len(word) < 10 and item['key'] != 'name':
                prop_value_dict.write(word + " 999999 __propValue_" + item['key'] + "__\n")
    prop_value_dict.close()

    # labels
    label_dict = codecs.open(out_dir + 'kg_label.dict', 'w', 'utf-8')
    cql = 'MATCH (e) RETURN DISTINCT labels(e) AS label'
    label_list = graph_op.query_with_cypher(graph, cql)
    for label in label_list:
        for l in label['label']:
            word = l
            label_dict.write(word + " 999999 __label__\n")
    label_dict.close()


def create_kg(dir_name, host="http://localhost:7474", username='neo4j', password='123456'):
    graph = Graph(host,
                  username=username,
                  password=password)

    import_kg_by_file(graph, os.path.join(dir_name, 'kg/'))

    save_dir = os.path.join(dir_name, 'dict/')
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    export_dict(graph, save_dir)


if __name__ == '__main__':
    dir = 'F:/PythonWorkspace/neo4j_data_load_demo/HIKVision/kg/'
    dict_dir = 'F:/PythonWorkspace/neo4j_data_load_demo/HIKVision/dict/'
    graph = Graph("http://localhost:7474",
                  username="neo4j",
                  password="123456")

    import_kg_by_file(graph, dir)
    export_dict(graph, dict_dir)
