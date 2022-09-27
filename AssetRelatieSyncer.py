import json

from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AssetRelatieSyncer:
    def __init__(self, triple_query_wrapper: TripleQueryWrapper, emInfraImporter: EMInfraImporter):
        self.triple_query_wrapper = triple_query_wrapper
        self.eminfra_importer = emInfraImporter

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
            assets_json_ld = self.transform_json_ld(assets_json_ld)

            self.triple_query_wrapper.load_json(jsonld_string=assets_json_ld, context=context)
            self.triple_query_wrapper.save_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    @staticmethod
    def transform_json_ld(assets_json_ld):
        asset_dict = json.loads(assets_json_ld)
        new_list = []
        for asset in asset_dict:
            new_dict = AssetRelatieSyncer.fix_dict(asset)
            new_list.append(new_dict)

        return json.dumps(new_list)

    @staticmethod
    def fix_dict(old_dict):
        new_dict = {}
        for k, v in old_dict.items():
            if ':' not in k:
                if isinstance(v, dict):
                    v = AssetRelatieSyncer.fix_dict(v)
                if k.startswith('AIMObject') or k.startswith('AIMDBStatus') or k.startswith('AIMToestand') \
                        or k.startswith('RelatieObject') or k.startswith('KwantWrd') or k.startswith('Dtc'):
                    new_dict['https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#' + k] = v
                else:
                    new_dict[k] = v
            else:
                new_dict[k] = v
        return new_dict

