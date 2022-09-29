import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

from AgentSyncer import AgentSyncer
from BetrokkeneRelatieSyncer import BetrokkeneRelatieSyncer
from AssetSyncer import AssetSyncer
from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class BetrokkeneRelatieSyncerTests(TestCase):
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

    def get_select_complex_query(self, predicate1, predicate2, aim_id):
        select_value_by_uuid_key_query = """
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
            SELECT ?v
            WHERE {
                ?asset <$p1$> ?w .
                ?w <$p2$> ?v 
                FILTER (?asset = <$aim-id$>)             
            }
            """
        return select_value_by_uuid_key_query.replace('$aim-id$', aim_id).replace('$p1$', predicate1).replace('$p2$',
                                                                                                              predicate2)

    def setup(self):
        self.eminfra_importer = EMInfraImporter(MagicMock())
        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False,
                                                       otl_db_path=Path().resolve().parent / 'OTL 2.5.db')
        self.assets_syncer = AssetSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         em_infra_importer=self.eminfra_importer)
        self.eminfra_importer.import_assets_from_webservice_page_by_page = self.return_assets
        self.assets_syncer.sync_assets()

        self.agents_syncer = AgentSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         em_infra_importer=self.eminfra_importer)
        self.eminfra_importer.import_agents_from_webservice_page_by_page = self.return_agents
        self.agents_syncer.sync_agents()

    def test_sync_betrokkenerelaties(self):
        self.setup()
        self.triple_query_wrapper.print_db()
        self.betrokkenerelaties_syncer = BetrokkeneRelatieSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                                                 em_infra_importer=self.eminfra_importer)

        self.eminfra_importer.import_betrokkenerelaties_from_webservice_page_by_page = self.return_betrokkenerelaties

        select_bron_relatie1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ')
        select_doel_relatie1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ')
        select_rol_relatie1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene.rol',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ')

        self.betrokkenerelaties_syncer.sync_betrokkenerelaties()

        self.triple_query_wrapper.print_db()

        rol_result = self.triple_query_wrapper.select_query(select_rol_relatie1_query)
        self.assertEqual('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/toezichter',
                         str(rol_result.bindings[0]['v']))
        bron_relatie1_result = self.triple_query_wrapper.select_query(select_bron_relatie1_query)
        self.assertEqual(
            'https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA',
            str(bron_relatie1_result.bindings[0]['v']))
        doel_relatie1_result = self.triple_query_wrapper.select_query(select_doel_relatie1_query)
        self.assertEqual(
            'https://data.awvvlaanderen.be/id/asset/00000000-1000-0000-0000-000000000000-cHVybDpBZ2VudA',
            str(doel_relatie1_result.bindings[0]['v']))

    def return_betrokkenerelaties(self, page_size):
        self.eminfra_importer.pagingcursor = ''
        yield json.dumps([{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ",
            "RelatieObject.bron": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA"
            },
            "RelatieObject.doel": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000000-1000-0000-0000-000000000000-cHVybDpBZ2VudA"
            },
            "HeeftBetrokkene.rol": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/toezichter",
            "HeeftBetrokkene.specifiekeContactinfo": [],
            "AIMDBStatus.isActief": True
        }])

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

    def return_agents(self, page_size):
        self.eminfra_importer.pagingcursor = '=1more='
        yield json.dumps([{
            "@type": "http://purl.org/dc/terms/Agent",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-1000-0000-0000-000000000000-cHVybDpBZ2VudA",
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
        }])
        self.eminfra_importer.pagingcursor = ''
        yield json.dumps([{
            "@type": "http://purl.org/dc/terms/Agent",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-2000-0000-0000-000000000000-cHVybDpBZ2VudA",
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
        }])
