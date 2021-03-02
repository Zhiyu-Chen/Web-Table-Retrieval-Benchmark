# Table-Retrieval-Benchmark

## Data Preparition: extracting tables from WDC table dump

1. run "python download.py" to download the wdc dump files from file_list. Check metadata.py for folder structure.
2. run "python extract.py" to match the downloaded wdc dumps with the entity linking results (available [here](https://zenodo.org/record/3627274#.YD31RS2cbcI) ) from "Novel Entity Discovery from Web Tables, WWW 2020".
3. run "python indexer.py" to create the index.

We also provide the processed WTR table dump here.

## Initial Pooled Results from Unsupervised Baselines

run "python pool_ranker.py" to get initial top-20 results from unsupervised baselines(BM25 on different fields).

The ranking results in TREC format are saved under "./ranking/pool/". The file name corresponds to the field.
We also provide the pooled tables in './data/wdc_pool.json.tar.gz' where you can access the raw table content according to the table id,.


## 5 Fold


## Baselines
