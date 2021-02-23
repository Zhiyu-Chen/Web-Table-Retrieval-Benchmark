from metadata import *
import pandas as pd
import numpy as np
from collections import defaultdict
from elastic import Elastic
import itertools
import pickle
import json
from collections import Counter
from extract import get_table_entities


def look_webTable():
    f = open(webtable_dump,'r')
    header_pos = set()
    table_type = set()
    keyCol_idx = set()
    header_row_idx = set()
    orientation = set()
    ct = 0
    for line in f:
        line = json.loads(line)
        header_pos.add(line['headerPosition']) # {'FIRST_ROW', 'MIXED', 'FIRST_COLUMN', 'NONE'}
        table_type.add(line['tableType']) # {'RELATION'}
        keyCol_idx.add(line['keyColumnIndex']) #{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 20, 21, 22, 25, 28, 47, 53, 54, -1}
        header_row_idx.add(line['headerRowIndex']) #{0, -1}
        orientation.add(line['tableOrientation']) # {'VERTICAL', 'HORIZONTAL'}
        ct += 1 #3006026
    f.close()
    print("header positions: {}".format(header_pos))
    print("table type: {}".format(table_type))
    print("key column index: {}".format(keyCol_idx))
    print("header row index: {}".format(header_row_idx))
    print("table size: {}".format(ct))



def parse_webTable(line):
    tid = line['json_loc']
    table_content = ' '.join(list(itertools.chain(*line['relation'])))
    textBefore = line['textBeforeTable']
    textAfter = line['textAfterTable']
    pageTitle = line['pageTitle']
    title = line['title']
    entities = line['entities']
    url = line['url']
    orientation = line['tableOrientation']
    header_idx = line['headerRowIndex']
    rows = list(map(list, zip(*line['relation'])))
    header = " ".join(rows[header_idx]) if header_idx != -1 else ""
    key_col_idx = line['keyColumnIndex']
    key_col = " ".join(line['relation'][key_col_idx]) if key_col_idx != -1 else ""
    return tid, table_content, textBefore, textAfter, pageTitle, title,entities,orientation, url,header,key_col


def correct_entities():
    #matched file and index
    elastic = Elastic('webtables',timeout = 200)
    t2e = get_table_entities()
    f_from = open(webtable_dump,'r')
    f_to = open(webtable_dump+'.new','w')
    ct = 0
    for line in f_from:
        line = json.loads(line)
        etids = line['json_loc'].split('/')[1][:-5]
        line['entities'] = t2e[etids]
        try:
            elastic.update_doc(line['json_loc'],'entities',t2e[etids])
        except:
            print(line['json_loc'])
        f_to.write(json.dumps(line) + '\n')
        ct += 1
        if ct%100 == 0:
            print(ct)

    f_from.close()
    f_to.close()


def index_webTables(index_name = 'webtables'):
    mappings = {
        "tid": Elastic.notanalyzed_field(),
        "content": Elastic.notanalyzed_field(),
        "textBefore": Elastic.analyzed_field(),
        "textAfter": Elastic.analyzed_field(),
        "pageTitle": Elastic.analyzed_field(),
        "title": Elastic.analyzed_field(),
        "entities": Elastic.analyzed_field(),
        "orientation": Elastic.notanalyzed_field(),
        "url": Elastic.notanalyzed_field(),
        "raw_json": Elastic.notanalyzed_field(),
        "header": Elastic.analyzed_field(),
        "key_col": Elastic.analyzed_field(),
        "catchall": Elastic.analyzed_field(),
    }

    elastic = Elastic(index_name,timeout = 200)
    elastic.create_index(mappings,force=True)
    f = open(webtable_dump,'r')
    f.readline().strip()
    for line in f:
        tid, table_content, textBefore, textAfter, pageTitle, title, entities, orientation, url, header, key_col = parse_webTable(json.loads(line))
        index_content = {
            "tid": tid,
            "content": table_content,
            "textBefore": textBefore,
            "textAfter": textAfter,
            "pageTitle": pageTitle,
            "title": title,
            "url": url,
            "entities":entities,
            "orientation":orientation,
            "header":header,
            "key_col":key_col,
            "raw_json": line,
            "catchall": ' '.join([table_content,textBefore,textAfter,pageTitle,title]),
    }
        try:
            elastic.add_doc(tid,index_content)
        except:
            print(str(tid))
        print("current dataset id {0}".format(tid))
    f.close()



"""
recheck entities:
1. missed ones in original dump ?
2. only check pool_dump: entities not in RDF2VEC.  RAW_JSON V.S. ENTITIES

"""


# read from original dump
# t2e = get_table_entities()
# entities = set()
# for tid in t2e:
#     for each in t2e[tid]:
#         entities.add(each)
#
# # read missed entities
# missed = []
# f = open('missed_entities.txt')
# for line in f:
#         missed.append(line.strip())
#
#
# for each in missed:
#     if each not in entities:
#         print(each)
#
# print("Done")

# if __name__ == '__main__':
#     index_webTables(webtable_index_name)




