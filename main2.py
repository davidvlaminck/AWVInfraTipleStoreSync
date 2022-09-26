# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json

from SPARQLWrapper import SPARQLWrapper, JSON, TURTLE, POST
from rdflib import Graph



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    g = Graph()
    context = {
        "@context": {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "term": "http://purl.org/dc/terms/",
            "asset": "https://data.awvvlaanderen.be/id/asset/",
            "purl": "http://purl.org/dc/terms/",
            "schema":"https://schema.org/"

        }
    }

    
    jsonld = [{
            "@type": "http://purl.org/dc/terms/Agent",
            "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA",
            "purl:Agent.naam": "Agent 1",
            "purl:Agent.contactinfo": [
                {
                    "schema:ContactPoint.telefoon": "+3234567890",
                    "schema:ContactPoint.email": "agent.1@mow.vlaanderen.be",
                    "schema:ContactPoint.adres": {
                        "DtcAdres.straatnaam": "straatnaam",
                        "DtcAdres.huisnummer": "1",
                        "DtcAdres.postcode": "2000",
                        "DtcAdres.gemeente": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAlgGemeente/antwerpen"
                    }
                }
            ]
        }, {
            "@type": "http://purl.org/dc/terms/Agent",
            "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA",
            "purl:Agent.naam": "Agent 2",
            "purl:Agent.contactinfo": [
                {
                    "schema:ContactPoint.telefoon": "+3234567890",
                    "schema:ContactPoint.email": "agent.2@mow.vlaanderen.be",
                    "schema:ContactPoint.adres": {
                        "DtcAdres.straatnaam": "straatnaam",
                        "DtcAdres.huisnummer": "2",
                        "DtcAdres.postcode": "2000",
                        "DtcAdres.gemeente": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAlgGemeente/antwerpen"
                    }
                }
            ]
        }]
    g.parse(data=json.dumps(jsonld), format='json-ld', context=context)
    for s, p, o in g:
        print(s, p, o)

    print('\n')

    qres = g.query("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX term: <http://purl.org/dc/terms/>
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
            PREFIX purl: <http://purl.org/dc/terms/>

            SELECT *
            WHERE {
                ?agent a <http://purl.org/dc/terms/Agent> . 
                ?agent purl:Agent.naam ?naam
                FILTER(?agent=<https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA>).
            }
            """)

    for row in qres:
        print(row.agent, row.naam)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
