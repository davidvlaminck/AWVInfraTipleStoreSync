import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

from AgentSyncer import AgentSyncer
from EMInfraImporter import EMInfraImporter
from TripleQueryWrapper import TripleQueryWrapper


class AgentSyncerTests(TestCase):
    def get_select_value_query(self, predicate, aim_id):
        select_value_by_uuid_key_query = """
            PREFIX asset: <https://data.awvvlaanderen.be/id/asset/>
            SELECT ?v
            WHERE {
                ?agent <$p$> ?v 
                FILTER (?agent = <$aim-id$>)             
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
        self.eminfra_importer = EMInfraImporter(MagicMock())
        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False,
                                                       otl_db_path=Path().resolve().parent / 'OTL 2.5.db')

    def test_sync_agent(self):
        self.setup()
        self.agents_syncer = AgentSyncer(triple_query_wrapper=self.triple_query_wrapper,
                                         em_infra_importer=self.eminfra_importer)

        self.eminfra_importer.import_agents_from_webservice_page_by_page = self.return_agents

        select_naam_agent1_query = self.get_select_value_query(
            predicate='http://purl.org/dc/terms/Agent.naam',
            aim_id='https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA')
        select_naam_agent2_query = self.get_select_value_query(
            predicate='http://purl.org/dc/terms/Agent.naam',
            aim_id='https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA')
        select_email_agent1_query = self.get_select_complex_query(
            predicate1='http://purl.org/dc/terms/Agent.contactinfo',
            predicate2='https://schema.org/ContactPoint.email',
            aim_id='https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA')

        naam_result = self.triple_query_wrapper.select_query(select_naam_agent1_query)

        self.assertListEqual([], naam_result.bindings)

        self.agents_syncer.sync_agents()

        self.triple_query_wrapper.print_db()

        naam_result = self.triple_query_wrapper.select_query(select_naam_agent1_query)
        self.assertEqual('Agent 1', str(naam_result.bindings[0]['v']))
        naam2_result = self.triple_query_wrapper.select_query(select_naam_agent2_query)
        self.assertEqual('Agent 2', str(naam2_result.bindings[0]['v']))
        email1_result = self.triple_query_wrapper.select_query(select_email_agent1_query)
        self.assertEqual('agent.1@mow.vlaanderen.be', str(email1_result.bindings[0]['v']))

    def return_agents(self, page_size):
        self.eminfra_importer.pagingcursor = '=1more='
        yield json.dumps([{
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
        }])
        self.eminfra_importer.pagingcursor = ''
        yield json.dumps([{
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
        }])