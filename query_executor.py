import logging
from pathlib import Path

import rdflib
from SPARQLWrapper import N3, RDF, JSON, JSONLD, TURTLE, CSV

from SettingsManager import SettingsManager
from TripleQueryWrapper import TripleQueryWrapper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_TripleStoreSyncer.json')
    db_settings = settings_manager.settings['databases']['prd']

    triple_wrapper = TripleQueryWrapper(use_graph_db=True,
                                        url_or_path=db_settings['host'] + db_settings['database'],
                                        otl_db_path=Path('OTL 2.5.db'))

    query = """DESCRIBE <https://data.awvvlaanderen.be/id/asset/000c960e-b694-4f96-bf56-51872325c714-b25kZXJkZWVsI0NhbWVyYQ>"""
    query = """
PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
SELECT ?asset ?p ?v
WHERE {
    ?asset ?p ?v .
    FILTER (?asset = <https://data.awvvlaanderen.be/id/asset/000c960e-b694-4f96-bf56-51872325c714-b25kZXJkZWVsI0NhbWVyYQ>)
}"""
    query = """
SELECT ?t ?r 
WHERE { 
    ?t a ?types .
    FILTER (?types IN (<https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#TLCfiPoort>,<https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Verkeersregelaar>)) .
    ?t <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMDBStatus.isActief> TRUE
    FILTER NOT EXISTS {
        ?r a <https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij> . 	
        ?v a <https://lgc.data.wegenenverkeer.be/ns/installatie#VRLegacy> .
        ?r <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMDBStatus.isActief> TRUE .
        ?v <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMDBStatus.isActief> TRUE .
        ?r <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel> ?v .  
        ?r <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron> ?t } .
}
"""

    results = triple_wrapper.select_query(query, return_format=CSV)

    g = rdflib.Graph()
    # # result = g.parse('trees.ttl')
    # # result = g.parse('trees.ttl', format='ttl')
    results = g.parse(results, format='json')
    # for stmt in g:
    #     print(stmt)

    for record in results:
        print(record)

