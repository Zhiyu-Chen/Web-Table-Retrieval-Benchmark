# Table-Retrieval-Benchmark

## Data Preparition: extracting tables from WDC table dump

1. run "python download.py" to download the wdc dump files from file_list. Check metadata.py for folder structure.
2. run "python extract.py" to match the wdc dumps with the data from WWW 2020 entity linking paper.
3. run "python indexer.py" to create the index.


## Initial Pooled Results from Unsupervised Baselines

run "python pool_ranker.py" to get initial top-20 results from unsupervised baselines(BM25 on different fields).

The ranking results in TREC format are saved under "./wdc/ranking". The file name corresponds to the field.
The WDC corpus dump is saved in './data/wdc/wdc_pool.json.tar.gz' where you can access the raw table content according to the table id,.

For convenience in comparing with pooled results of wiki corpus (www 2018), the pooled wiki dump is saved in '.data/www2018/wiki_pool.json'.
 
 
 ## Create  Input File for Amazon Turk
 
 Under the folder of annotate, run "python pre_annotate.py".