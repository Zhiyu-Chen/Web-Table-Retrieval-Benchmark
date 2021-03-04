'''
unsupervised baselines:
1. BM25 for different fields
2. MLM

Output
    1. trec formated result file from each baseline
    2. raw table content from pooled results
'''
from scorer import ScorerMLM
from elastic import Elastic
from metadata import *
from data_loader import WikiTables
from glob import glob
import json

def run_WDC_singleField(topn=20):
    es = Elastic(index_name=webtable_index_name)
    wiki_loader = WikiTables('./data/www2018')
    q_dict = wiki_loader.get_queries()
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    fields = ['content','textBefore','textAfter','pageTitle','title','header','catchall']
    for field in fields:
        rs = es.bulk_search(queries,field)
        #generate result file
        f_rank = open(os.path.join(wdc_rank_path,field+'.txt'), 'w')
        for q_id, query in enumerate(queries):
            rank = 1
            for each_rs in sorted(rs[q_id].items(), key=lambda kv: kv[1], reverse=True)[:topn]:
                f_rank.write(str(q_id+1) + "\tQ0\t" + each_rs[0] + "\t" + str(rank) + "\t" + str(
                    each_rs[1]) + "\t" + field + "\n")
                rank += 1
        f_rank.close()

def collect_pooled_WDC_tables():
    # collect table ids from all result files
    pool_files = glob(os.path.join(wdc_rank_path,'*'))
    top_tids = set()
    for pool_file in pool_files:
        f = open(pool_file,'r')
        for line in f:
            top_tids.add(line.split('\t')[2])
        f.close()

    # get table content from elasticsearch
    f_table = open(os.path.join(wdc_data_path,'wdc_pool.json'),'w')
    es = Elastic(index_name=webtable_index_name)
    for tid in top_tids:
        doc = es.get_doc(tid)
        f_table.write(json.dumps(doc['_source'])+'\n')
    f_table.close()


# def run_WDC_multiField(topn=20):
#     es = Elastic(index_name=webtable_index_name)
#     wiki_loader = WikiTables('./data/www2018')
#     q_dict = wiki_loader.get_queries()
#     queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
#     fields = ['content', 'textBefore', 'textAfter', 'pageTitle', 'title', 'header']
#     field_weights = {
#         'content':0.6,
#         'textBefore':0.1,
#         'textAfter':0.1,
#         'pageTitle':0.1,
#         'title':0.05,
#         'header':0.05,
#     }
#     params = {"fields": field_weights}
#     for query in queries:
#         rs = ScorerMLM(es,query.params).score_doc()


if __name__  == '__main__':
   run_WDC_singleField()
   collect_pooled_WDC_tables()