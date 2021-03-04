import os

'''
path structure for extracting from wdc WebTable dumps:
-home
    - data_dir
        - wdc_path (direcotry to store the WDC dumps)
            - wdc_files # **.tar
            - file_list.txt
        - db_path (direcotry to store the entity linking results)
            - match_file # me_corres.csv


'''

home_dir = os.getenv("HOME")
# used for extracting dumps
data_dir = os.path.join(home_dir,"data_dir")
wdc_path = os.path.join(data_dir,'wdc') # root dir of WDC files
db_path = os.path.join(data_dir,'www2020-webtables') # root dir of WWW paper data

match_file = os.path.join(db_path,"table_matching/me_corres.csv")
webtable_dump = os.path.join(db_path,"web_tables.json")


# used for unsupervised pooling methods
webtable_index_name = 'webtables'
wdc_data_path = './data'
wdc_rank_path = os.path.join('./ranking')
wdc_pool_path = os.path.join(wdc_data_path,'wdc_pool.json')
wdc_table_entity = os.path.join(wdc_data_path,'wdc_table.entities')
wdc_STR_path = os.path.join(wdc_data_path,'wdc_STR.csv')


