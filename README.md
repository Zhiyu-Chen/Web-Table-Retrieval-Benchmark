# WTR test collection

Web Table Retrieval (WTR) test collection is a benchmark for table retrieval based on a large-scale Web Table Corpora extracted from the Common Crawl.
We not only provide relevance judgments of query-table pairs, but also the relevance judgments of query-table context pairs with respect to a query.

![AMT example](./figures/task_illu.png)



## Data Preprocessing and Indexing


To reproduce and index the WTR table dump, there are three steps: 

1. download the wdc dump files from file_list:
```
      wget -i file_list.txt
```
3. match the downloaded wdc dumps with the entity linking results (available [here](https://zenodo.org/record/3627274#.YD31RS2cbcI) ) from "Novel Entity Discovery from Web Tables, WWW 2020":
```
      python extract.py
```
5. create the index (with elasticsearch running as the backend):
```
      python indexer.py
```

Please check the data paths/folder structures in "metadata.py".
For convenience, we provide the processed WTR table dump [here](http://www.cse.lehigh.edu/~brian/data/WTR_tables.tar.gz).

## Pooled Results

You can obtian the initial top-20 results from unsupervised baselines(BM25 on different fields) by running:
```
 "python pool_ranker.py"
```

The ranking results in TREC format are saved under "./ranking/pool/". The file name corresponds to the field.
We also provide the pooled tables in "./data/wdc_pool.json.tar.gz" where you can access the raw table content according to the table id,.


## 5 Fold

We provide the 5-fold data splits under "./data/" and each split is named as "fold_split.jsonl". For example, "1_train.jsonl" is the training set for fold 1. In each record of a JSON file, we provide the query, table content and label.

## Baselines

The rankings of baselines are under "./rankings/". Except "pool" folder, each of the rest folder is named by a baseline method and includes the corresponding ranking results in that folder.

The STR features are saved in "./data/wdc_STR.csv". The 1st 9 features in the file are LTR features.
