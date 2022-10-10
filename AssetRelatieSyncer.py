from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AssetRelatieSyncer:
    def __init__(self, triple_query_wrapper: TripleQueryWrapper, em_infra_importer: EMInfraImporter):
        self.triple_query_wrapper = triple_query_wrapper
        self.eminfra_importer = em_infra_importer

    def sync_assetrelaties(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor

        context = {
            "@context": {
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "assetrelatie": "https://data.awvvlaanderen.be/id/assetrelatie/",
                "lgc": "https://wegenenverkeer.data.vlaanderen.be/oef/legacy/",
                "ins": "https://wegenenverkeer.data.vlaanderen.be/oef/inspectie/",
                "ond": "https://wegenenverkeer.data.vlaanderen.be/oef/onderhoud/",
                "loc": "https://wegenenverkeer.data.vlaanderen.be/oef/locatie/",
                "tz": "https://wegenenverkeer.data.vlaanderen.be/oef/toezicht/",
            }
        }
        for assets_json_ld in self.eminfra_importer.import_assetrelaties_from_webservice_page_by_page(page_size=page_size):
            assets_json_ld, count = self.triple_query_wrapper.jsonld_completer.transform_json_ld(assets_json_ld)

            self.triple_query_wrapper.load_json(jsonld_string=assets_json_ld, context=context)
            print(f'finished loading {count} assetrelaties')

            if self.eminfra_importer.pagingcursor == '':
                break
            else:
                self.triple_query_wrapper.save_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})
