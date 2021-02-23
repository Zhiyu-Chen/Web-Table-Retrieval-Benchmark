import os

'''
path structure for extracting from wdc WebTable dumps:
-home
    - data_dir
        - wdc_path
            - wdc_files # **.tar
            - file_list.txt
        - db_path
            - match_file # me_corres.csv


'''

home_dir = "/home/zhc415"
# used for extracting dumps
data_dir = os.path.join(home_dir,"myspace/hb_tables")
wdc_path = os.path.join(data_dir,'wdc') # root dir of WDC files
db_path = os.path.join(data_dir,'www2020-webtables') # root dir of WWW paper data

match_file = os.path.join(db_path,"table_matching/me_corres.csv")
webtable_dump = os.path.join(db_path,"web_tables.json")
wiki_dir_path = os.path.join(db_path,'transitive_redirects_en.ttl')
wiki_link_path = os.path.join(db_path,'page_links_en.ttl')
wiki_link_m = os.path.join(db_path,'page_links_en.matrix')
wiki_entity_path = os.path.join(db_path,'dbpedia.entity')

# used for unsupervised pooling methods
webtable_index_name = 'webtables'
wdc_data_path = './data/wdc'
wdc_rank_path = os.path.join(wdc_data_path,'ranking')
wdc_pool_path = os.path.join(wdc_data_path,'wdc_pool.json')
wdc_table_entity = os.path.join(wdc_data_path,'wdc_table.entities')
wdc_STR_path = os.path.join(wdc_data_path,'wdc.STR')

wiki_data_path = './data/www2018'
wiki_qe_dump = 'query.entities'
rdf2vec_dump = os.path.join('/home/zhc415/myspace/NLP/pretrained_embeddings','DB2Vec_cbow_200_5_5_2_500')
word2vec_dump = os.path.join('/home/zhc415/myspace/NLP/pretrained_embeddings','GoogleNews-vectors-negative300.bin')
