import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

from AssetSyncer import AssetSyncer
from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AssetSyncerTests(TestCase):
    def get_select_value_query(self, predicate, aim_id):
        select_value_by_uuid_key_query = """
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
            SELECT ?v
            WHERE {
                ?asset <$p$> ?v 
                FILTER (?asset = <$aim-id$>)             
            }
            """
        return select_value_by_uuid_key_query.replace('$aim-id$', aim_id).replace('$p$', predicate)

    def setup(self):
        self.eminfra_importer = EMInfraImporter(MagicMock())
        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False,
                                                       otl_db_path=Path().resolve().parent / 'OTL 2.5.db')

    def test_sync_asset(self):
        self.setup()
        self.assets_syncer = AssetSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         em_infra_importer=self.eminfra_importer)

        self.eminfra_importer.import_assets_from_webservice_page_by_page = self.return_assets

        select_naam_asset1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMNaamObject.naam',
            aim_id='https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA')
        select_toestand_asset1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMToestand.toestand',
            aim_id='https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA')
        select_naam_asset2_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMNaamObject.naam',
            aim_id='https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA')
        select_naam_asset3_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMNaamObject.naam',
            aim_id='https://data.awvvlaanderen.be/id/asset/30000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA')


        naam_result = self.triple_query_wrapper.select_query(select_naam_asset1_query)

        self.assertListEqual([], naam_result.bindings)

        self.assets_syncer.sync_assets()

        self.triple_query_wrapper.print_db()

        naam_result = self.triple_query_wrapper.select_query(select_naam_asset1_query)
        self.assertEqual('asset 1', str(naam_result.bindings[0]['v']))
        toestand_result = self.triple_query_wrapper.select_query(select_toestand_asset1_query)
        self.assertEqual('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik', str(toestand_result.bindings[0]['v']))
        naam2_result = self.triple_query_wrapper.select_query(select_naam_asset2_query)
        self.assertEqual('asset 2', str(naam2_result.bindings[0]['v']))
        naam3_result = self.triple_query_wrapper.select_query(select_naam_asset3_query)
        self.assertEqual('asset 3', str(naam3_result.bindings[0]['v']))

        select_asset_id_asset1_query = """
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
            PREFIX ie: <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#>
            SELECT ?v
            WHERE {
                ?asset <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.assetId> ?w .
                ?w <https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator> ?v
                FILTER (?asset = <https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA>)
            }
            """
        asset_id_result = self.triple_query_wrapper.select_query(select_asset_id_asset1_query)
        self.assertEqual('10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA', str(asset_id_result.bindings[0]['v']))

        self.triple_query_wrapper.graph.serialize(destination='3_assets.ttl', format='ttl')

    def return_assets(self, page_size):
        self.eminfra_importer.pagingcursor = '=2more='
        yield json.dumps([{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
            "AIMDBStatus.isActief": True,
            "AIMNaamObject.naam": "asset 1",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "AIMObject.notitie": "asset 1"
        }])
        self.eminfra_importer.pagingcursor = '=1more='
        yield json.dumps([{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-ontwerp",
            "AIMDBStatus.isActief": True,
            "AIMNaamObject.naam": "asset 2",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "AIMObject.notitie": "asset 2"
        }])
        self.eminfra_importer.pagingcursor = ''
        yield json.dumps([{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "@id": "https://data.awvvlaanderen.be/id/asset/30000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/uit-gebruik",
            "AIMDBStatus.isActief": False,
            "AIMNaamObject.naam": "asset 3",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "30000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "AIMObject.notitie": "asset 3"
        }])
