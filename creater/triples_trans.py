import os


class KgFilesCreater:
    def __init__(self, triples, entity_dict, property_dict, relation_dict):
        self.triples = triples
        self.entity_dict = entity_dict
        self.property_dict = property_dict
        self.relation_dict = relation_dict

        self.nodes = self.create_nodes()
        self.properties = self.create_properties()
        self.relations = self.create_relations()

    def create_all(self):
        nodes = self.nodes
        properties = self.properties
        relations = self.relations
        return nodes, properties, relations

    def save_all_stuff(self, dir_name):
        save_dir = os.path.join(dir_name, 'kg')
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)

        with open(os.path.join(save_dir, 'kg_node.txt'), 'w', encoding='utf-8') as f:
            lines = [str(line) + '\n' for line in self.nodes]
            f.writelines(lines)

        with open(os.path.join(save_dir, 'kg_node_prop.txt'), 'w', encoding='utf-8') as f:
            lines = [str(line) + '\n' for line in self.properties]
            f.writelines(lines)

        with open(os.path.join(save_dir, 'kg_rel.txt'), 'w', encoding='utf-8') as f:
            lines = [str(line) + '\n' for line in self.relations]
            f.writelines(lines)

    def create_nodes(self):
        lines = ['\t'.join(["label", "name"])]
        for key, value in self.entity_dict.items():
            lines.append('\t'.join([value, key]))
        return lines

    def create_properties(self):
        lines = ['\t'.join(["label", "name", "prop_key", "prop_value"])]
        for key, value in self.entity_dict.items():
            lines.append('\t'.join([value, key, 'name', key]))
        for triple in self.triples:
            sub = triple[0]
            prop = triple[1]
            obj = triple[2]
            if sub not in self.entity_dict.keys() or prop not in self.property_dict.keys() \
                    or obj not in self.entity_dict.keys():
                continue
            label = self.entity_dict[sub]
            name = sub
            prop_key = self.property_dict[prop]
            prop_value = value
            lines.append('\t'.join([label, name, prop_key, prop_value]))
        return lines

    def create_relations(self):
        lines = ['\t'.join(["label_1", "name_1", "rel_type", "rel_name", "label_2", "name_2"])]
        for triple in self.triples:
            sub = triple[0]
            prop = triple[1]
            obj = triple[2]
            if sub not in self.entity_dict.keys() or prop not in self.relation_dict.keys() \
                    or obj not in self.entity_dict.keys():
                continue

            label_1 = self.entity_dict[sub]
            name_1 = sub
            rel_type = self.relation_dict[prop]
            rel_name = prop
            label_2 = self.entity_dict[obj]
            name_2 = obj
            lines.append('\t'.join([label_1, name_1, rel_type, rel_name, label_2, name_2]))
        return lines
