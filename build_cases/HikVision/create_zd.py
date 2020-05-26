import os
from creater.triples_trans import KgFilesCreater
from creater.import_data import create_kg


home = 'C:\\Users\\Roy\\Desktop'
triple_file_names = 'hik_spo.txt'
dir_name = 'D:\\KG_Space\\HIKVison'

with open(os.path.join(home, triple_file_names), 'r', encoding='utf-8') as f:
    triples = []
    for line in f:
        triples.append(line.strip().split('\t'))

    entity_dict = {}
    property_dict = {}
    relation_dict = {}
    for triple in triples:
        sub = triple[0]
        prop = triple[1]
        obj = triple[2]
        entity_dict[sub] = sub
        entity_dict[obj] = 'other'
        property_dict[prop] = 'property'
        relation_dict[prop] = prop
    creater = KgFilesCreater(triples, entity_dict, property_dict, relation_dict)
    creater.save_all_stuff(dir_name)
    create_kg(dir_name, host="http://localhost:7474",)
