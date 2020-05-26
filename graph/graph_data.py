# encoding:utf-8

def get_node_data(df):
    """
    return a list of tuple
    like (node_label,node_name)
    for neo4j by dataframe
    :param df:
    :return:
    """
    node_lst = []
    for idx, row in df.iterrows():
        label = row['label']
        name = row['name']
        node = (label, name)
        node_lst.append(node)

    return node_lst


def get_relationship_data(df):
    """
    return a list of tuple
    like ((node1_label,node1_name), (rel_type, rel_name), (node2_label,node2_name))
    for neo4j by dataframe
    :param df:
    :return:
    """
    rel_lst = []
    for idx, row in df.iterrows():
        label_1 = row['label_1']
        name_1 = row['name_1']
        label_2 = row['label_2']
        name_2 = row['name_2']
        rel_type = row['rel_type']
        rel_name = row['rel_name']
        rel = ((label_1, name_1), (rel_type, rel_name), (label_2, name_2))
        rel_lst.append(rel)

    return rel_lst


def get_property_data(df):
    """
    return a list of tuple
    like (label/type, node_name/rel_name, property_key, property_value)
    :param df:
    :return:
    """
    prop_lst = []
    for idx, row in df.iterrows():
        label = row['label']
        name = row['name']
        prop_key = row['prop_key']
        prop_value = row['prop_value']
        prop = (label,name,prop_key,prop_value)
        prop_lst.append(prop)

    return prop_lst

