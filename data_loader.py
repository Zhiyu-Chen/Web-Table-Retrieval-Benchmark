import json,os,pickle,requests
from nltk.tokenize import word_tokenize
import numpy as np
import pandas as pd
from collections import defaultdict
from metadata import *

'''
For WWW 2018 STR paper data
'''

def request_entities(query):
    query = word_tokenize(query)
    query = '+'.join(query)
    url = 'http://api.nordlys.cc/er?q={}&1st_num_docs=10&model=mlm'.format(query)
    rs = requests.get(url)
    try:
        rs = json.loads(rs.text)
        return [rs['results'][each]['entity'] for each in rs['results']]
    except:
        return None

class WikiTables:
    def __init__(self,wiki_path):
        self.wiki_path = wiki_path

    def get_fold(self,fold,split):
        f_name = open(os.path.join(self.wiki_path,str(fold) + '_' + split + '.jsonl'))
        samples = []
        for line in f_name:
            line = json.loads(line)
            samples.append([line['qid'],line['docid'],line['rel']])
        f_name.close()
        return samples


    def get_doc_dict(self):
        f_name = open(os.path.join(self.wiki_path, 'wiki_pool.json'))
        doc_dict = dict()
        for line in f_name:
            line = json.loads(line)
            if line['docid'] not in doc_dict:
                doc_dict[line['docid']] = line['table']['t_headings'] + ' ' + line['table']['t_body']
        return doc_dict


    def get_queries(self):
        q_dict = dict()
        f = open(os.path.join(self.wiki_path,'queries.txt'),'r')
        for line in f:
            q_id = line[:line.find(' ')]
            query = line[line.find(' ')+1:].strip()
            q_dict[q_id] = query
        return q_dict

    def get_query_entities(self):
        wiki_qe_path = os.path.join(self.wiki_path,wiki_qe_dump)
        if os.path.exists(wiki_qe_path):
            return pickle.load(open(wiki_qe_path,'rb'))

        q_dict = self.get_queries()
        q_entities = dict()
        for qid in q_dict:
            query = q_dict[qid].split(' ')
            query = '+'.join(query)
            url = 'http://api.nordlys.cc/er?q={}&1st_num_docs=10&model=mlm'.format(query)
            rs = requests.get(url)
            rs = json.loads(rs.text)
            q_entities[qid] = [rs['results'][each]['entity'] for each in rs['results']]

        pickle.dump(q_entities,open(wiki_qe_path,'wb'))
        return q_entities



    def get_all_features(self):
        feature_file = os.path.join(self.wiki_path,'features.csv')
        ids_left = []
        ids_right = []
        features = []
        labels = []
        f_f = open(feature_file, 'r')
        line = f_f.readline()
        for line in f_f:
            seps = line.strip().split(',')
            qid = seps[0]
            tid = seps[2]
            ids_left.append(qid)
            ids_right.append(tid)
            rel = seps[-1]
            labels.append(int(rel))
            '''
            if int(rel) > 0:
                labels.append(1)
            else:
                labels.append(0)
            '''
            q_doc_f = np.array([float(each) for each in seps[3:-1]])
            features.append(q_doc_f)
        df = pd.DataFrame({
            'id_left': ids_left,
            'id_right': ids_right,
            'features': features,
            'label': labels
        })
        return df


'''
For WDC Corpus
'''
class WDCTables:
    def __init__(self,path=wdc_pool_path):
        self.path = path
        self.table_dict = self.get_doc_dict()
        #self.e2i, self.i2e = self.get_all_entities()

    def get_doc_dict(self):
        f = open(self.path, 'r')
        table_dict = dict()
        for line in f:
            line = json.loads(line)
            table_dict[line['tid']] = line
        f.close()
        return table_dict

    def get_db_entities(self):
        '''
        retrieve entities from DB repository,
        :return:
        '''
        if os.path.exists(wdc_table_entity):
            return pickle.load(open(wdc_table_entity, 'rb'))

        db_entities = defaultdict(list)
        for tid in self.table_dict:
            text_before = self.table_dict[tid]['textBefore']
            text_after = self.table_dict[tid]['textAfter']
            pageTitle = self.table_dict[tid]['pageTitle']
            if text_before:
                rs = request_entities(text_before)
                if rs:
                    db_entities[tid].append(rs)
            if text_after:
                rs = request_entities(text_after)
                if rs:
                    db_entities[tid].append(rs)
            if pageTitle:
                rs = request_entities(pageTitle)
                if rs:
                    db_entities[tid].append(rs)

        pickle.dump(db_entities, open(wdc_table_entity, 'wb'))
        return db_entities



    def get_table_entities(self,tid):
        return self.table_dict[tid]['entities']


