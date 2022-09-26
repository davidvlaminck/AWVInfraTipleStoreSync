import json

import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST


class TripleQueryWrapper:
    def __init__(self, use_graph_db: bool, url_or_path: str = None):
        self.use_graph_db = use_graph_db
        self.url_or_path = url_or_path
        self.graph = None
        if not use_graph_db:
            self.graph = rdflib.Graph()

    def select_query(self, query: str):
        if self.use_graph_db:
            return self.select_use_graph_db(query)
        else:
            return self.select_use_rdflib(query)

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
            raise NotImplementedError
        else:
            self.graph.parse(data=jsonld_string, format='json-ld', context=context)

    def save_to_params(self, param_dict):
        params = self.get_params()

        if params == {}:
            self.update_query("""        
            PREFIX params: <http://www.w3.org/ns/params/>        
            INSERT DATA { <0> a <params:Params> }""")

        for k, v in param_dict.items():
            q = """        
PREFIX params: <http://www.w3.org/ns/params/>
DELETE { ?p params:$k$ ?v }      
INSERT { ?p params:$k$ <$v$> }
WHERE { ?p a <params:Params>}""".replace('$k$', k).replace("$v$", str(v))
            self.update_query(q)

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
            else:
                d[p] = str(o)
        return d


# # Create a Graph, add in some test data
# g = Graph()
# g.parse(
#     data="""
#         <x:> a <c:> .
#         <y:> a <c:> .
#     """,
#     format="turtle"
# )

# # Select all the things (s) that are of type (rdf:type) c:
# qres = g.query("""SELECT ?s WHERE { ?s a <c:> }""")
#
# for row in qres:
#     print(f"{row.s}")
# # prints:
# # x:
# # y:
#
# # Add in a new triple using SPATQL UPDATE
# g.update("""INSERT DATA { <z:> a <c:> }""")
#
# # Select all the things (s) that are of type (rdf:type) c:
# qres = g.query("""SELECT ?s WHERE { ?s a <c:> }""")
#
# print("After update:")
# for row in qres:
#     print(f"{row.s}")
# # prints:
# # x:
# # y:
# # z:
#
# # Change type of <y:> from <c:> to <d:>
# g.update("""
#          DELETE { <y:> a <c:> }
#          INSERT { <y:> a <d:> }
#          WHERE { <y:> a <c:> }
#          """)
# print("After second update:")
# qres = g.query("""SELECT ?s ?o WHERE { ?s a ?o }""")
# for row in qres:
#     print(f"{row.s} a {row.o}")
# # prints:
# # x: a c:
# # z: a c:
# # y: a d:
