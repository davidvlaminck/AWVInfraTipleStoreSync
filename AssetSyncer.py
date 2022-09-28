from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AssetSyncer:
    def __init__(self, triple_query_wrapper: TripleQueryWrapper, em_infra_importer: EMInfraImporter):
        self.triple_query_wrapper = triple_query_wrapper
        self.eminfra_importer = em_infra_importer

    def sync_assets(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor

        context = {
            "@context": {
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "asset": "https://data.awvvlaanderen.be/id/asset/",
                "lgc": "https://wegenenverkeer.data.vlaanderen.be/oef/legacy/",
                "ins": "https://wegenenverkeer.data.vlaanderen.be/oef/inspectie/",
                "ond": "https://wegenenverkeer.data.vlaanderen.be/oef/onderhoud/",
                "loc": "https://wegenenverkeer.data.vlaanderen.be/oef/locatie/",
                "tz": "https://wegenenverkeer.data.vlaanderen.be/oef/toezicht/",
            }
        }
        for assets_json_ld in self.eminfra_importer.import_assets_from_webservice_page_by_page(page_size=page_size):
            assets_json_ld = self.triple_query_wrapper.jsonld_completer.transform_json_ld(assets_json_ld)

            self.triple_query_wrapper.load_json(jsonld_string=assets_json_ld, context=context)
            self.triple_query_wrapper.save_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

            #self.triple_query_wrapper.graph.serialize(destination='100_assets.ttl', format='ttl')
