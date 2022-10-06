import logging
import time
import traceback
from datetime import datetime

import requests

from AgentSyncer import AgentSyncer
from AssetRelatieSyncer import AssetRelatieSyncer
from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler


class Syncer:
    def __init__(self, triple_wrapper, request_handler: RequestHandler, eminfra_importer: EMInfraImporter,
                 settings=None):
        self.triple_wrapper = triple_wrapper
        self.request_handler = request_handler
        self.eminfra_importer = eminfra_importer

        self.settings = settings
        self.sync_start = None
        self.sync_end = None
        if 'time' in self.settings:
            self.sync_start = self.settings['time']['start']
            self.sync_end = self.settings['time']['end']

    def start_syncing(self):
        while True:
            self.perform_fresh_start_sync()

    def perform_fresh_start_sync(self):
        while True:
            try:
                # main sync loop for a fresh start
                params = self.triple_wrapper.get_params()
                sync_step = params['sync_step']
                pagingcursor = params['pagingcursor']
                page_size = params['pagesize']

                if sync_step == -1:
                    sync_step = 1
                if sync_step == 5:
                    break

                if sync_step == 1:
                    self.sync_agents(page_size, pagingcursor)
                elif sync_step == 2:
                    self.sync_assets(page_size, pagingcursor)
                elif sync_step == 3:
                    self.sync_betrokkenerelaties()
                elif sync_step == 4:
                    self.sync_assetrelaties()

                pagingcursor = self.eminfra_importer.pagingcursor
                if pagingcursor == '':
                    sync_step += 1
                    self.triple_wrapper.save_props_to_params(
                        {'sync_step': sync_step})
                else:
                    self.triple_wrapper.save_props_to_params(
                        {'sync_step': sync_step,
                         'pagingcursor': pagingcursor})

            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)


    def sync_assetrelaties(self):
        start = time.time()
        assetrelatie_syncer = AssetRelatiesSyncer(em_infra_importer=self.eminfra_importer,
                                                  post_gis_connector=self.connector)
        while True:
            try:
                params = self.connector.get_params()
                assetrelatie_syncer.sync_assetrelaties(pagingcursor=params['pagingcursor'])
            except AssetMissingError as exc:
                self.events_processor.postgis_connector.connection.rollback()
                missing_assets = exc.args[0]
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                processor = NieuwAssetProcessor(cursor=self.connector.connection.cursor(), em_infra_importer=self.eminfra_importer)
                processor.process(missing_assets)
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.events_processor.postgis_connector.connection.save_props_to_params({'pagingcursor': current_paging_cursor})

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assetrelaties: {round(end - start, 2)}')

    def sync_betrokkenerelaties(self):
        start = time.time()
        betrokkenerelatie_syncer = BetrokkeneRelatiesSyncer(em_infra_importer=self.eminfra_importer,
                                                            post_gis_connector=self.connector)
        params = None
        while True:
            try:
                params = self.connector.get_params()
                betrokkenerelatie_syncer.sync_betrokkenerelaties(pagingcursor=params['pagingcursor'])
            except AgentMissingError:
                self.connector.connection.rollback()
                print('refreshing agents')
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                self.sync_agents(page_size=params['pagesize'], pagingcursor='')
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.connector.connection.save_props_to_params({'pagingcursor': current_paging_cursor})

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all betrokkenerelaties: {round(end - start, 2)}')

    def sync_bestekkoppelingen(self):
        start = time.time()
        bestek_koppeling_syncer = BestekKoppelingSyncer(em_infra_importer=self.eminfra_importer,
                                                        postGIS_connector=self.connector)
        bestek_koppeling_syncer.sync_bestekkoppelingen()
        end = time.time()
        logging.info(f'time for all bestekkoppelingen: {round(end - start, 2)}')

    def sync_assets(self, page_size, pagingcursor):
        start = time.time()
        asset_syncer = AssetSyncer(em_infra_importer=self.eminfra_importer,
                                   postgis_connector=self.connector)
        params = None
        while True:
            try:
                params = self.connector.get_params()
                asset_syncer.sync_assets(pagingcursor=params['pagingcursor'])
            except (AssetTypeMissingError, AttribuutMissingError):
                self.connector.connection.rollback()
                print('refreshing assettypes')
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                self.sync_assettypes(page_size=params['pagesize'], pagingcursor='')
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.connector.connection.save_props_to_params({'pagingcursor': current_paging_cursor})

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assets: {round(end - start, 2)}')

    def sync_relatietypes(self):
        start = time.time()
        relatietype_syncer = RelatietypeSyncer(em_infra_importer=self.eminfra_importer,
                                               postgis_connector=self.connector)
        relatietype_syncer.sync_relatietypes()
        end = time.time()
        logging.info(f'time for all relatietypes: {round(end - start, 2)}')

    def sync_assettypes(self, page_size, pagingcursor):
        start = time.time()
        assettype_syncer = AssetTypeSyncer(emInfraImporter=self.eminfra_importer,
                                           postGIS_connector=self.connector)
        assettype_syncer.sync_assettypes(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all assettypes: {round(end - start, 2)}')

    def sync_bestekken(self, page_size, pagingcursor):
        start = time.time()
        bestek_syncer = BestekSyncer(em_infra_importer=self.eminfra_importer,
                                     postGIS_connector=self.connector)
        bestek_syncer.sync_bestekken(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all bestekken: {round(end - start, 2)}')

    def sync_beheerders(self, page_size, pagingcursor):
        start = time.time()
        beheerder_syncer = BeheerderSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector)
        beheerder_syncer.sync_beheerders(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all beheerders: {round(end - start, 2)}')

    def sync_identiteiten(self, page_size, pagingcursor):
        start = time.time()
        identiteit_syncer = IdentiteitSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector)
        identiteit_syncer.sync_identiteiten(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all identiteiten: {round(end - start, 2)}')

    def sync_toezichtgroepen(self, page_size, pagingcursor):
        start = time.time()
        toezichtgroep_syncer = ToezichtgroepSyncer(emInfraImporter=self.eminfra_importer,
                                                   postGIS_connector=self.connector)
        toezichtgroep_syncer.sync_toezichtgroepen(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all toezichtgroepen: {round(end - start, 2)}')

    def sync_agents(self, page_size, pagingcursor):
        start = time.time()
        agent_syncer = AgentSyncer(em_infra_importer=self.eminfra_importer, triple_query_wrapper=self.triple_wrapper)
        agent_syncer.sync_agents(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all agents: {round(end - start, 2)}')
