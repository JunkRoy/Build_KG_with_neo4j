import os
from collections import Counter

import json

home = 'H:\\KG_Corpus\\baidu_SPO_corpus'
dev_name = 'dev_data.json'
train_name = 'train_data.json'

names = [dev_name, train_name]

entity = {}
all_entity_types = []

types = ["行政区", "城市", "地点"]

sample_types = {}
for t in types:
    sample_types[t] = []

predicates = []

for name in names:
    with open(os.path.join(home, name), 'r', encoding='utf-8') as f:
        # samples = json.load(f)
        for line in f:
            sample = eval(line.strip())
            for triple in sample['spo_list']:
                subject_type = triple['subject_type']
                object_type = triple['object_type']
                predicate = triple['predicate']
                predicates.append(predicate)
                all_entity_types.append(subject_type)
                all_entity_types.append(object_type)

                if subject_type in types:
                    sample_types[subject_type].append(triple['subject'])
                if object_type in types:
                    sample_types[object_type].append(triple['object'])

print(Counter(all_entity_types).keys())
print(Counter(predicates))

for key, value in sample_types.items():
    print(key, list(set(value))[:10])