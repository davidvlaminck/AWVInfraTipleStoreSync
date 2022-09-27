import json
import os
from pathlib import Path
from unittest import TestCase

from AssetRelatieSyncer import AssetRelatieSyncer
from AssetSyncer import AssetSyncer
from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from TripleQueryWrapper import TripleQueryWrapper
from UnitTests.AssetSyncerTests import AssetSyncerTests


class AssetRelatieSyncerTests(TestCase):
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
        return select_value_by_uuid_key_query.replace('$aim-id$', aim_id).replace('$p1$', predicate1).replace('$p2$', predicate2)

    def setup(self):
        settings_manager = SettingsManager(
            settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)

        self.eminfra_importer = EMInfraImporter(request_handler)

        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False)

        self.assets_syncer = AssetSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         emInfraImporter=self.eminfra_importer)

        self.eminfra_importer.import_assets_from_webservice_page_by_page = self.return_assets
        self.assets_syncer.sync_assets()

    def test_sync_assetrelaties(self):
        self.setup()
        self.triple_query_wrapper.print_db()
        self.assetrelaties_syncer = AssetRelatieSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                                       emInfraImporter=self.eminfra_importer)

        self.eminfra_importer.import_assetrelaties_from_webservice_page_by_page = self.return_assetrelaties

        select_bron_relatie1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n')
        select_doel_relatie1_query = self.get_select_value_query(
            predicate='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n')
        select_id_relatie1_query = self.get_select_complex_query(
            predicate1='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.assetId',
            predicate2='https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator',
            aim_id='https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n')

        id_result = self.triple_query_wrapper.select_query(select_id_relatie1_query)

        self.assertListEqual([], id_result.bindings)

        self.assetrelaties_syncer.sync_assetrelaties()

        self.triple_query_wrapper.print_db()

        id_result = self.triple_query_wrapper.select_query(select_id_relatie1_query)
        self.assertEqual('00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n', str(id_result.bindings[0]['v']))
        bron_relatie1_result = self.triple_query_wrapper.select_query(select_bron_relatie1_query)
        self.assertEqual('https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA',
                         str(bron_relatie1_result.bindings[0]['v']))
        doel_relatie1_result = self.triple_query_wrapper.select_query(select_doel_relatie1_query)
        self.assertEqual('https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA',
                         str(doel_relatie1_result.bindings[0]['v']))

        self.triple_query_wrapper.graph.serialize(destination='2_assets_met_relatie.ttl', format='ttl')

    def return_assetrelaties(self, page_size):
        self.eminfra_importer.pagingcursor = ''
        yield json.dumps([{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n",
            "RelatieObject.bron": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA"
            },
            "RelatieObject.doel": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkelement",
                "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA"
            },
            "AIMDBStatus.isActief": True,
            "RelatieObject.assetId": {
                "DtcIdentificator.identificator": "00000000-1200-0000-0000-000000000000-b25kZXJkZWVsI0JldmVzdGlnaW5n",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.bronAssetId": {
                "DtcIdentificator.identificator": "10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.doelAssetId": {
                "DtcIdentificator.toegekendDoor": "AWV",
                "DtcIdentificator.identificator": "20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA"
            },
            "RelatieObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging"
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