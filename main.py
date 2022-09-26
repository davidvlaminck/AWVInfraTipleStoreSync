# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from SPARQLWrapper import SPARQLWrapper, JSON, TURTLE, POST
from rdflib import Graph


def create():
    sparql = SPARQLWrapper(
        "http://localhost:7200/repositories/awvinfra_dev/statements"
    )
    sparql.setMethod(POST)
    sparql.setQuery("""
        PREFIX installatietype: <https://wegenenverkeer.data.vlaanderen.be/ns/installatie#>
        PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
        INSERT DATA { <asset:00009b12-4dd1-4761-bc4d-af0f9a8f7ecd-aW5zdGFsbGF0aWUjTGluaw> a <installatietype:Link> }
        """
                    )
    results = sparql.query().response.read()
    print(results)

def select():
    sparql = SPARQLWrapper(
        "http://localhost:7200/repositories/awvinfra_dev"
    )
    sparql.setReturnFormat(JSON)
    sparql.setQuery("""
        PREFIX installatietype: <https://wegenenverkeer.data.vlaanderen.be/ns/installatie#>

        SELECT *
        WHERE {
            ?a a installatietype:Link .
        }
        ORDER BY ?a
        LIMIT 3
        """)

    try:
        ret = sparql.queryAndConvert()

        for r in ret["results"]["bindings"]:
            print(r)
    except Exception as e:
        print(e)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    create()
    select()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
