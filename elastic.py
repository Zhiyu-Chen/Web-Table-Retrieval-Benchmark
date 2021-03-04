"""
elastic
-------

Tools for working with Elasticsearch.
This class is to be instantiated for each index.

"""
import numpy as np
from pprint import pprint
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import defaultdict
from joblib import Parallel, delayed
from metadata import *
from scipy.spatial.distance import cosine


ES_config = {
  "hosts": [
    "localhost:9200"
  ],
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}
ELASTIC_HOSTS = ES_config.get("hosts")
ELASTIC_SETTINGS = ES_config.get("settings")


class Elastic(object):
    FIELD_CATCHALL = "catchall"
    FIELD_ELASTIC_CATCHALL = "_all"
    DOC_TYPE = "doc"  # we don't make use of types
    ANALYZER_STOP_STEM = "english"
    ANALYZER_STOP = "stop_en"
    BM25 = "BM25"
    SIMILARITY = "sim"  # Used when other similarities are used

    def __init__(self, index_name = 'toy_index',timeout=30):
        self.__es = Elasticsearch(hosts=ELASTIC_HOSTS,timeout=timeout)
        self.es = self.__es
        self.__index_name = index_name
        self.fasttext_model = None
        self.gensim_wrapper = None
        self.all_ids = None

    @staticmethod
    def analyzed_field(analyzer=ANALYZER_STOP):
        """Returns the mapping for analyzed fields.

        :param analyzer: name of the analyzer; valid options: [ANALYZER_STOP, ANALYZER_STOP_STEM]
        """
        if analyzer not in {Elastic.ANALYZER_STOP, Elastic.ANALYZER_STOP_STEM}:
            print("Error: Analyzer", analyzer, "is not valid.")
            exit(0)
        return {"type": "text",
                "term_vector": "with_positions_offsets",
                "analyzer": analyzer}

    @staticmethod
    def notanalyzed_field():
        """Returns the mapping for not-analyzed fields."""
        return {"type": "text",
                "index": "not_analyzed"}
        # "similarity": Elastic.SIMILARITY}

    def __gen_similarity(self, model, params=None):
        """Gets the custom similarity function."""
        similarity = params if params else {}
        similarity["type"] = model
        return {Elastic.SIMILARITY: similarity}

    def __gen_analyzers(self):
        """Gets custom analyzers.
        We include customized analyzers in the index setting, a field may or may not use it.
        """
        analyzer = {"type": "standard", "stopwords": "_english_"}
        analyzers = {"analyzer": {Elastic.ANALYZER_STOP: analyzer}}
        return analyzers

    def analyze_query(self, query, analyzer=ANALYZER_STOP):
        """Analyzes the query.

        :param query: raw query
        :param analyzer: name of analyzer
        """
        #tokens = self.__es.indices.analyze(index=self.__index_name, body=query, analyzer=analyzer)["tokens"]
        tokens = self.__es.indices.analyze(index=self.__index_name, body=query)["tokens"]
        query_terms = []
        for t in sorted(tokens, key=lambda x: x["position"]):
            query_terms.append(t["token"])
        return " ".join(query_terms)

    def get_text_tokens(self,text):
        tokens = self.__es.indices.analyze(index=self.__index_name, body={'text':text})["tokens"]
        terms = []
        for t in sorted(tokens, key=lambda x: x["position"]):
            terms.append(t["token"])
        return terms

    def get_mapping(self):
        """Returns mapping definition for the index."""
        mapping = self.__es.indices.get_mapping(index=self.__index_name, doc_type=self.DOC_TYPE)
        return mapping[self.__index_name]["mappings"][self.DOC_TYPE]["properties"]

    def get_settings(self):
        """Returns index settings."""
        return self.__es.indices.get_settings(index=self.__index_name)[self.__index_name]["settings"]["index"]

    def __update_settings(self, settings):
        """Updates the index settings."""
        self.__es.indices.close(index=self.__index_name)
        self.__es.indices.put_settings(index=self.__index_name, body=settings)
        self.__es.indices.open(index=self.__index_name)
        self.__es.indices.refresh(index=self.__index_name)

    def add_field(self,field_name):
        if field_name in self.get_mapping():
            print("filed already exists !")
            return
        cml = '''
                curl -X PUT "localhost:9200/''' \
              + self.__index_name \
              + '''/_mapping/doc" -H 'Content-Type: application/json' -d'
                {
                  "properties": {
                    " '''+ field_name + '''": {
                      "type": "text",
                      "term_vector": "with_positions_offsets",
                      "analyzer": "stop_en"
                    }
                  }
                }
                '
        '''
        print(cml)
        os.system(cml)



    def update_similarity(self, model=BM25, params=None):
        """Updates the similarity function "sim", which is fixed for all index fields.

         The method and param should match elastic settings:
         https://www.elastic.co/guide/en/elasticsearch/reference/2.3/index-modules-similarity.html

        :param model: name of the elastic model
        :param params: dictionary of params based on elastic
        """
        old_similarity = self.get_settings()["similarity"]
        new_similarity = self.__gen_similarity(model, params)
        # We only update the similarity if it is different from the old one.
        # this avoids unnecessary closing of the index


        if old_similarity != new_similarity:
            self.__update_settings({"similarity": new_similarity})

    def delete_index(self):
        """Deletes an index."""
        self.__es.indices.delete(index=self.__index_name)
        print("Index <" + self.__index_name + "> has been deleted.")

    def create_index(self, mappings, model=BM25, model_params=None, force=False):
        """Creates index (if it doesn't exist).

        :param mappings: field mappings
        :param model: name of elastic search similarity
        :param model_params: name of elastic search similarity
        :param force: forces index creation (overwrites if already exists)
        """
        if self.__es.indices.exists(self.__index_name):
            if force:
                self.delete_index()
            else:
                print("Index already exists. No changes were made.")
                return

        # sets general elastic settings
        body = ELASTIC_SETTINGS

        # sets the global index settings
        # number of shards should be always set to 1; otherwise the stats would not be correct
        body["settings"] = {"analysis": self.__gen_analyzers(),
                            "index": {"number_of_shards": 1,
                                      "number_of_replicas": 0},
                            }

        # sets similarity function
        # If model is not BM25, a similarity module with the given model and params is defined
        if model != Elastic.BM25:
            body["settings"]["similarity"] = self.__gen_similarity(model, model_params)
        sim = model if model == Elastic.BM25 else Elastic.SIMILARITY
        for mapping in mappings.values():
            mapping["similarity"] = sim

        # sets the field mappings
        body["mappings"] = {self.DOC_TYPE: {"properties": mappings}}

        # creates the index
        self.__es.indices.create(index=self.__index_name, body=body)
        pprint(body)
        print("New index <" + self.__index_name + "> is created.")

    def add_docs_bulk(self, docs):
        """Adds a set of documents to the index in a bulk.

        :param docs: dictionary {doc_id: doc}
        """
        actions = []
        for doc_id, doc in docs.items():
            action = {
                "_index": self.__index_name,
                "_type": self.DOC_TYPE,
                "_id": doc_id,
                "_source": doc
            }
            actions.append(action)

        if len(actions) > 0:
            helpers.bulk(self.__es, actions)

    def add_doc(self, doc_id, contents):
        """Adds a document with the specified contents to the index.

        :param doc_id: document ID
        :param contents: content of document
        """
        self.__es.index(index=self.__index_name, doc_type=self.DOC_TYPE, id=doc_id, body=contents)

    def get_doc(self, doc_id, fields=None, source=True):
        """Gets a document from the index based on its ID.

        :param doc_id: document ID
        :param fields: list of fields to return (default: all)
        :param source: return document source as well (default: yes)
        """
        return self.__es.get(index=self.__index_name, doc_type=self.DOC_TYPE, id=doc_id, _source=source)

    def update_doc(self,doc_id,field,updated_content):
        body = {self.DOC_TYPE:{field:updated_content }}
        return self.__es.update(index=self.__index_name, doc_type=self.DOC_TYPE, id=doc_id,body=body)

    def get_all_doc_ids(self):
        pass

    def load_embedding(self,mode):
        pass

    def search(self, query, field, num=100, fields_return="", start=0):
        """Searches in a given field using the similarity method configured in the index for that field.

        :param query: query string
        :param field: field to search in
        :param num: number of hits to return (default: 100)
        :param fields_return: additional document fields to be returned
        :param start: starting offset (default: 0)
        :return: dictionary of document IDs with scores
        """
        hits = self.__es.search(index=self.__index_name, q=query, df=field, _source=False, size=num,
                                from_=start)["hits"]["hits"]
        results = {}
        for hit in hits:
            results[hit["_id"]] = hit["_score"]
        return results

    def bulk_search(self, queries,field):
        results = Parallel(n_jobs= -1, backend="threading")\
            (delayed(bulk_search_wrapper)(i) for i in zip([self]*len(queries), queries,[field]*len(queries)))
        return results

    def multi_search(self, query, field_weights,num=100,only_ids = True,start=0):
        """

        :param query:
        :param field_weights:  a dictionary containing field names and corresponding weights
        :param start:
        :return:
        """
        fields = list(field_weights.keys())
        total_doc = self.num_docs()
        doc_scores = defaultdict(lambda: defaultdict(int))
        for field in fields:
            hits = self.__es.search(index=self.__index_name, q=query, df=field, _source=False, size=total_doc,
                                from_=start)["hits"]["hits"]
            for each_hit in hits:
                doc_scores[each_hit['_id']][field] = each_hit['_score']

        #combine scores
        for doc_id in doc_scores:
            d_score = 0
            for field in doc_scores[doc_id]:
                d_score += doc_scores[doc_id][field]*field_weights[field]
            doc_scores[doc_id]['score'] =  d_score

        items = sorted(doc_scores.items(), key=lambda kv: kv[1]['score'], reverse=True)
        items = items[:num]
        if only_ids:
            return [each[0] for each in items]
        return items



    def bulk_multi_search(self,queries, field_weights,num=100,only_ids = True,start=0):
        results = Parallel(n_jobs=-1, backend="threading") \
            (delayed(bulk_multi_search_wrapper)(i) for i in zip([self] * len(queries), queries, [field_weights] * len(queries),[num]*len(queries)))
        return results

    def search_schema(self, header_mode,tid_schemas,tid_origin,query,schema_fields, field_weights,schema_sim, num=100, only_ids=False,start=0):
        """
        :param query:
        :param schema_fields:
        :param field_weights:  a dictionary containing field names and corresponding weights
        :param start:
        :return:
        """
        fields = list(field_weights.keys())
        total_doc = self.num_docs()
        doc_scores = defaultdict(lambda: defaultdict(float))
        for field in fields:
            if field not in schema_fields:
                if field_weights[field] == 0:
                    continue
                hits = self.__es.search(index=self.__index_name, q=query, df=field, _source=False, size=total_doc,
                                        from_=start)["hits"]["hits"]
                for each_hit in hits:
                    doc_scores[each_hit['_id']][field] = each_hit['_score']
            else:
                # comparing similarity between query and schema embedding
                if self.all_ids is None:
                    self.get_all_doc_ids()

                self.load_embedding(schema_sim)
                for doc_id in self.all_ids:
                    if header_mode == 'origin':
                        schemas = tid_origin[doc_id]
                    elif header_mode == 'generated':
                        schemas = tid_schemas[doc_id]
                    elif header_mode == 'both':
                        schemas = tid_origin[doc_id] + tid_schemas[doc_id]
                    if len(schemas) == 0:
                        doc_scores[doc_id][field] = 0
                        continue
                    if schema_sim == 'cosine':
                        schema_vec = self.fasttext_model.get_sentence_vector(schemas)
                        query_vec = self.fasttext_model.get_sentence_vector(query)
                        doc_scores[doc_id][field] = 1 - cosine(schema_vec,query_vec)
                    elif schema_sim == 'wmd':
                        query_tokens = self.get_text_tokens(query)
                        #schema_tokens = self.get_text_tokens(schemas)
                        schema_tokens = list(set([each.lower() for each in schemas]))
                        doc_scores[doc_id][field] = - self.gensim_wrapper.wmdistance(query_tokens,schema_tokens)
        # combine scores
        for d_id in doc_scores:
            d_score = 0
            for field in doc_scores[d_id]:
                d_score += doc_scores[d_id][field] * field_weights[field]
            doc_scores[d_id]['score'] = d_score

        items = sorted(doc_scores.items(), key=lambda kv: kv[1]['score'], reverse=True)
        items = items[:num]
        if only_ids:
            return [each[0] for each in items]
        return items

    def bulk_schema_search(self,tid_schemas,queries,schema_fields, field_weights,schema_sim,num=100,only_ids = True,start=0):
        results = Parallel(n_jobs=-1, backend="threading") \
            (delayed(bulk_schema_search_wrapper)(i) for i in zip([self] * len(queries),[tid_schemas]* len(queries), queries, [schema_fields] * len(queries), [field_weights] * len(queries),[schema_sim]*len(queries),[num]*len(queries)))
        return results


    def schema_rerank(self, query,schema_fields, field_weights,schema_sim,num=100, only_ids=True, start=0):
        """
        :param query:
        :param schema_fields:
        :param field_weights:  a dictionary containing field names and corresponding weights
        :param start:
        :return:
        """
        fields = list(field_weights.keys())
        fields = [each for each in fields if each not in schema_fields]
        doc_scores = defaultdict(lambda: defaultdict(int))
        for field in fields:
            if field_weights[field] == 0:
                continue
            hits = self.__es.search(index=self.__index_name, q=query, df=field, _source=False, size=num,
                                    from_=start)["hits"]["hits"]
            for each_hit in hits:
                doc_scores[each_hit['_id']][field] = each_hit['_score']


        self.load_embedding(schema_sim)
        for field in schema_fields:
            for doc_id in doc_scores:
                doc_rs = self.get_doc(doc_id,source=[field])
                if field not in doc_rs['_source']:
                    doc_scores[doc_id][field] = 0
                    continue
                schemas = doc_rs['_source'][field]
                if schema_sim == 'cosine':
                    schema_vec = self.fasttext_model.get_sentence_vector(schemas)
                    query_vec = self.fasttext_model.get_sentence_vector(query)
                    doc_scores[doc_id][field] = 1 - cosine(schema_vec,query_vec)
                elif schema_sim == 'wmd':
                    query_tokens = self.get_text_tokens(query)
                    schema_tokens = self.get_text_tokens(schemas)
                    doc_scores[doc_id][field] = - self.gensim_wrapper.wmdistance(query_tokens,schema_tokens)

        # combine scores
        for d_id in doc_scores:
            d_score = 0
            for field in doc_scores[d_id]:
                d_score += doc_scores[d_id][field] * field_weights[field]
            doc_scores[d_id]['score'] = d_score

        items = sorted(doc_scores.items(), key=lambda kv: kv[1]['score'], reverse=True)
        items = items[:num]
        if only_ids:
            return [each[0] for each in items]
        return items

    def bulk_schema_rerank(self,queries,schema_fields, field_weights,schema_sim,num=100,only_ids = True,start=0):
        results = Parallel(n_jobs=-1, backend="threading") \
            (delayed(bulk_schema_rerank_wrapper)(i) for i in zip([self] * len(queries), queries, [schema_fields] * len(queries), [field_weights] * len(queries),[schema_sim]*len(queries),[num]*len(queries)))
        return results

    def estimate_number(self, query):
        """Search body, return the number of hits containg body"""
        try:
            return self.__es.search(index=self.__index_name, q = query, _source=False, size=1,
                                from_=0)["hits"]["total"]
        except:
            return 0

    def search_complex(self, body, num=100, fields_return="", start=0):
        """Searches in a given field using the similarity method configured in the index for that field.

        :param body: query body
        :param field: field to search in
        :param num: number of hits to return (default: 100)
        :param fields_return: additional document fields to be returned
        :param start: starting offset (default: 0)
        :return: dictionary of document IDs with scores
        """
        hits = self.__es.search(index=self.__index_name, body=body, _source=False, size=num,
                                from_=start)["hits"]["hits"]
        results = {}
        for hit in hits:
            results[hit["_id"]] = hit["_score"]
        return results

    def estimate_number_complex(self, body):
        """Search body, return the number of hits containg body"""
        try:
            return self.__es.search(index=self.__index_name, body=body, _source=False, size=1,
                                from_=0)["hits"]["total"]
        except:
            return 0

    def get_field_stats(self, field):
        """Returns stats of the given field."""
        return self.__es.field_stats(index=self.__index_name, fields=[field])["indices"]["_all"]["fields"][field]

    def get_fields(self):
        """Returns name of fields in the index."""
        return list(self.get_mapping().keys())

    # =========================================
    # ================= Stats =================
    # =========================================
    def _get_termvector(self, doc_id, field, term_stats=False):
        """Returns a term vector for a given document field, including global field and term statistics.
        Term stats can have a serious performance impact; should be set to true only if it is needed!

        :param doc_id: document ID
        :param field: field name
        """
        tv = self.__es.termvectors(index=self.__index_name, doc_type=self.DOC_TYPE, id=doc_id, fields=field,
                                   term_statistics=term_stats)
        return tv.get("term_vectors", {}).get(field, {}).get("terms", {})

    def _get_coll_termvector(self, term, field):
        """Returns a term vector containing collection stats of a term."""
        body = {"query": {"bool": {"must": {"term": {field: term}}}}}
        hits = self.search_complex(body, num=1)
        # hits = self.search(term, field, num=1)
        doc_id = next(iter(hits.keys())) if len(hits) > 0 else None
        return self._get_termvector(doc_id, field, term_stats=True) if doc_id else {}

    def num_docs(self):
        """Returns the number of documents in the index."""
        return self.__es.count(index=self.__index_name, doc_type=self.DOC_TYPE)["count"]

    def num_fields(self):
        """Returns number of fields in the index."""
        return len(self.get_mapping())

    def doc_count(self, fields):
        """Returns number of documents with at least one term for the given field."""
        return self.__es.indices.stats(index=self.__index_name,fields=fields)['_all']['primaries']['docs']['count']

    def coll_length(self, field):
        """Returns length of field in the collection."""
        return self.get_field_stats(field)["sum_total_term_freq"]

    def avg_len(self, field):
        """Returns average length of a field in the collection."""
        return self.coll_length(field) / self.doc_count(field)

    def doc_length(self, doc_id, field):
        """Returns length of a field in a document."""
        return sum(self.term_freqs(doc_id, field).values())

    def doc_freq(self, term, field, tv=None):
        """Returns document frequency for the given term and field."""
        coll_tv = tv if tv else self._get_coll_termvector(term, field)
        return coll_tv.get(term, {}).get("doc_freq", 0)


    def coll_term_freq(self, term, field, tv=None):
        """ Returns collection term frequency for the given field."""
        coll_tv = tv if tv else self._get_coll_termvector(term, field)
        return coll_tv.get(term, {}).get("ttf", 0)

    def term_freqs(self, doc_id, field, tv=None):
        """Returns term frequencies of all terms for a given document and field."""
        doc_tv = tv if tv else self._get_termvector(doc_id, field)
        term_freqs = {}
        for term, val in doc_tv.items():
            term_freqs[term] = val["term_freq"]
        return term_freqs

    def term_freq(self, doc_id, field, term):
        """Returns frequency of a term in a given document and field."""
        return self.term_freqs(doc_id, field).get(term, 0)


if __name__ == "__main__":
    field = "content"
    term = "gonna"
    doc_id = 4

    es = Elastic("toy_index")
    pprint(es.search("gonna", "content"))

    print("================= Stats =================")
    print("[FIELD]: %s [TERM]: %s" % (field, term))
    print("- Number of documents: %d" % es.num_docs())
    print("- Number of fields: %d" % es.num_fields())
    print("- Document count: %d" % es.doc_count(field))
    print("- Collection length: %d" % es.coll_length(field))
    print("- Average length: %.2f" % es.avg_len(field))
    print("- Document length: %d" % es.doc_length(doc_id, field))
    print("- Number of fields: %d" % es.num_fields())
    print("- Document frequency: %d" % es.doc_freq(term, field))
    print("- Collection frequency: %d" % es.coll_term_freq(term, field))
    print("- Term frequencies:")
    pprint(es.term_freqs(doc_id, field))
    person_id = "you"
    # search doc containing person_id
    body = {
        "query": {
            "bool": {
                "must": {
                    "term": {"content": person_id}
                }
            }
        }
    }
    # search docs containing both person_id and a(analyzed)
    a = es.analyze_query("me")
    body = {"query": {
        "bool": {
            "must": [
                {
                    "match": {"content": person_id}
                },
                {
                    "match_phrase": {"content": a}
                }
            ]
        }
    }}
    print(es.search_complex(body, "content"))
    print(es.term_freqs(1, "content"))

    # pprint.pprint(es.get_termvector("<dbpedia:Category:People_of_Canadian_descent>", "title"))
    # pprint.pprint(es.search("people", "title", fields_return="title"))


def bulk_search_wrapper(arg, **kwarg):
    return Elastic.search(*arg, **kwarg)


def bulk_multi_search_wrapper(arg, **kwarg):
    return Elastic.multi_search(*arg, **kwarg)


def bulk_schema_search_wrapper(arg, **kwarg):
    return Elastic.search_schema(*arg, **kwarg)

def bulk_schema_rerank_wrapper(arg, **kwarg):
    return Elastic.schema_rerank(*arg, **kwarg)
