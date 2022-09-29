import json
import os
from pathlib import Path

import rdflib
import requests
from SPARQLWrapper import SPARQLWrapper, JSON, POST

from JsonLdCompleter import JsonLdCompleter


class TripleQueryWrapper:
    def __init__(self, otl_db_path: Path, use_graph_db: bool, url_or_path: str = None):
        self.use_graph_db = use_graph_db
        self.url_or_path = url_or_path
        self.graph = None
        if not use_graph_db:
            self.graph = rdflib.Graph()
        self.jsonld_completer = JsonLdCompleter(otl_db_path=otl_db_path)

    def select_query(self, query: str):
        if self.use_graph_db:
            return self.select_use_graph_db(query)
        else:
            return self.select_use_rdflib(query)

    def print_db(self):
        for s, p, o in self.graph:
            print(s, p, o)

    def select_use_graph_db(self, query):
        sparql = SPARQLWrapper(self.url_or_path)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)

        try:
            ret = sparql.queryAndConvert()
            return ret["results"]["bindings"]
        except Exception as e:
            print(e)

    def select_use_rdflib(self, query):
        res = self.graph.query(query)
        return res

    def update_query(self, query: str):
        if self.use_graph_db:
            return self.update_use_graph_db(query)
        else:
            return self.update_use_rdflib(query)

    def update_use_graph_db(self, query):
        sparql = SPARQLWrapper(self.url_or_path + '/statements')
        sparql.setMethod(POST)
        sparql.setQuery(query)
        return sparql.query().response.read()

    def update_use_rdflib(self, query):
        res = self.graph.update(query)
        return res

    def load_json(self, jsonld_string, context):
        if self.use_graph_db:
            g = rdflib.Graph()
            g.parse(data=jsonld_string, format='json-ld', context=context)
            g.serialize(destination='temp.ttl', format='ttl')

            headers = {
              'Content-Type': 'application/x-turtle',
              'Accept': 'application/json'
            }

            with open('temp.ttl', 'rb') as f:
                requests.post(self.url_or_path + '/statements', data=f, headers=headers)
            os.unlink('temp.ttl')
        else:
            self.graph.parse(data=jsonld_string, format='json-ld', context=context)

    def save_to_params(self, param_dict):
        for k, v in param_dict.items():
            if isinstance(v, str):
                v = '<' + v + '>'

            q = """        
PREFIX params: <http://www.w3.org/ns/params/>
DELETE { ?p params:$k$ ?v }
INSERT { ?p params:$k$ $v$ }
WHERE { ?p a <params:Params> }""".replace('$k$', k).replace("$v$", str(v))
            self.update_query(q)

    def init_params(self):
        self.update_query("""        
PREFIX params: <http://www.w3.org/ns/params/>        
INSERT DATA { <http://www.0.org> a <params:Params> }""")
        self.save_to_params({'sync_step': -1, 'pagingcursor': '', 'pagesize': 1000})

    def get_params(self):
        params = self.select_query("""        
PREFIX params: <http://www.w3.org/ns/params/>
SELECT ?p ?r ?x
WHERE {
    ?p a <params:Params> .
    ?p ?r ?x 
}""")
        d = {}

        if len(params) == 0:
            return d

        for s, p, o in params:
            if str(p) == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                continue
            p = str(p).replace('http://www.w3.org/ns/params/', '')
            if p == 'fresh_sync':
                d[p] = bool(o)
            elif p in ['sync_step', 'pagesize']:
                d[p] = int(o)
            else:
                d[p] = str(o)
        return d

