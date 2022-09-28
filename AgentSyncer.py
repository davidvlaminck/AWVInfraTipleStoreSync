import json

from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AgentSyncer:
    def __init__(self, triple_query_wrapper: TripleQueryWrapper, em_infra_importer: EMInfraImporter):
        self.triple_query_wrapper = triple_query_wrapper
        self.eminfra_importer = em_infra_importer

    def sync_agents(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor

        context = {
            "@context": {
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "term": "http://purl.org/dc/terms/",
                "asset": "https://data.awvvlaanderen.be/id/asset/",
                "purl": "http://purl.org/dc/terms/",
                "schema": "https://schema.org/"

            }
        }
        for agents_json_ld in self.eminfra_importer.import_agents_from_webservice_page_by_page(page_size=page_size):
            agents_json_ld = self.triple_query_wrapper.jsonld_completer.transform_json_ld(agents_json_ld)

            self.triple_query_wrapper.load_json(jsonld_string=agents_json_ld, context=context)
            self.triple_query_wrapper.save_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

