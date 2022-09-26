import os
from pathlib import Path
from unittest import TestCase

from AgentSyncer import AgentSyncer
from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from TripleQueryWrapper import TripleQueryWrapper


class AgentSyncerTests(TestCase):
    select_name_by_uuid_query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX term: <http://purl.org/dc/terms/>
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>

            SELECT *
            WHERE {
                ?agent a <term:Agent> .  
                FILTER (?agent = <asset:00009b12-4dd1-4761-bc4d-af0f9a8f7ecd-aW5zdGFsbGF0aWUjTGluaw>)             
            }
            """

    def setup(self):
        settings_manager = SettingsManager(
            settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)
        self.eminfra_importer = EMInfraImporter(request_handler)

        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False)

    def test_sync_agent(self):
        self.setup()
        self.agents_syncer = AgentSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         emInfraImporter=self.eminfra_importer)
        self.agents_syncer.sync_agents()

        self.triple_query_wrapper.graph.serialize(destination='agents.ttl', format='ttl')

    def test_update_agents(self):
        self.setup()

        self.agents_syncer = AgentSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         emInfraImporter=self.eminfra_importer)

        self.create_agent()



        select_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX installatietype: <https://wegenenverkeer.data.vlaanderen.be/ns/installatie#>
        PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>

        SELECT *
        WHERE {
            ?link a <installatietype:Link> .
        }
        ORDER BY ?link
        LIMIT 3
        """
        r = self.triple_query_wrapper.select_query(select_query)

        self.triple_query_wrapper.update_query("""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX installatietype: <https://wegenenverkeer.data.vlaanderen.be/ns/installatie#>
        PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
        INSERT DATA { <asset:00009b12-4dd1-4761-bc4d-af0f9a8f7ecd-aW5zdGFsbGF0aWUjTGluaw> a <installatietype:Link> }""")

        qres = self.triple_query_wrapper.select_query(select_query)
        for row in qres:
            print(row.link)





        return
        create_agent_query = "INSERT INTO agents (uuid, naam, actief) VALUES ('d2d0b44c-f8ba-4780-a3e7-664988a6db66', 'unit test', true)"
        select_agent_query = "SELECT naam FROM agents WHERE uuid = '{uuid}'"
        count_agent_query = "SELECT count(*) FROM agents"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_agent_query)

        with self.subTest('name check the first agent created'):
            cursor.execute(select_agent_query.replace('{uuid}', 'd2d0b44c-f8ba-4780-a3e7-664988a6db66'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test', result)
        with self.subTest('number of agents before update'):
            cursor.execute(count_agent_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        agents = [{'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/005162f7-1d84-4558-b911-1f09a2e26640-cHVybDpBZ2VudA',
                   'purl:Agent.contactinfo': [
                       {'schema:ContactPoint.telefoon': '+3233666824',
                        'schema:ContactPoint.email': 'lvp@trafiroad.be'}],
                   'purl:Agent.naam': 'Ludovic Van Pée'},
                  {'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/0081576c-a62d-4b33-a884-597532cfdd77-cHVybDpBZ2VudA',
                   'purl:Agent.naam': 'Frederic Crabbe', 'purl:Agent.contactinfo': [
                      {'schema:ContactPoint.email': 'frederic.crabbe@mow.vlaanderen.be',
                       'schema:ContactPoint.telefoon': '+3250248103',
                       'schema:ContactPoint.adres': {'DtcAdres.straatnaam': 'CEL SCHADE WVL'}}]},
                  {'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/d2d0b44c-f8ba-4780-a3e7-664988a6db66',
                   'purl:Agent.naam': 'unit test changed'}]
        self.agents_syncer.update_agents(agent_dicts=agents)

        with self.subTest('name check after the first agent updated'):
            cursor.execute(select_agent_query.replace('{uuid}', 'd2d0b44c-f8ba-4780-a3e7-664988a6db66'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test changed', result)
        with self.subTest('name check after new agents created'):
            cursor.execute(select_agent_query.replace('{uuid}', '005162f7-1d84-4558-b911-1f09a2e26640'))
            result = cursor.fetchone()[0]
            self.assertEqual('Ludovic Van Pée', result)
        with self.subTest('number of agents after update'):
            cursor.execute(count_agent_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def return_agents(self, agent_uuids):
        return [{
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
        }]

    def create_agent(self):
        self.agents_syncer.update_agents(self.return_agents([]))
