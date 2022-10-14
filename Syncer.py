import logging
import time

from AgentSyncer import AgentSyncer
from AssetRelatieSyncer import AssetRelatieSyncer
from AssetSyncer import AssetSyncer
from BetrokkeneRelatieSyncer import BetrokkeneRelatieSyncer
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
            params = self.triple_wrapper.get_params()
            sync_step = params['sync_step']
            if sync_step == 5:
                break

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

                if sync_step == 2:
                    self.sync_agents(page_size, pagingcursor)
                elif sync_step == 1:
                    self.sync_assets()
                elif sync_step == 3:
                    self.sync_betrokkenerelaties()
                elif sync_step == 4:
                    self.sync_assetrelaties()

                pagingcursor = self.eminfra_importer.pagingcursor
                if pagingcursor == '':
                    sync_step += 1
                    self.triple_wrapper.delete_in_params(['pagingcursor'])
                    self.triple_wrapper.save_to_params(
                        {'sync_step': sync_step})
                else:
                    self.triple_wrapper.save_to_params(
                        {'sync_step': sync_step,
                         'pagingcursor': pagingcursor})

            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)


    def sync_assetrelaties(self):
        start = time.time()
        assetrelatie_syncer = AssetRelatieSyncer(em_infra_importer=self.eminfra_importer,
                                                 triple_query_wrapper=self.triple_wrapper)
        while True:
            params = self.triple_wrapper.get_params()
            assetrelatie_syncer.sync_assetrelaties(pagingcursor=params['pagingcursor'],
                                                   page_size=params['pagesize'])

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assetrelaties: {round(end - start, 2)}')

    def sync_betrokkenerelaties(self):
        start = time.time()
        betrokkenerelatie_syncer = BetrokkeneRelatieSyncer(em_infra_importer=self.eminfra_importer,
                                                           triple_query_wrapper=self.triple_wrapper)
        while True:
            params = self.triple_wrapper.get_params()
            betrokkenerelatie_syncer.sync_betrokkenerelaties(pagingcursor=params['pagingcursor'],
                                                             page_size=params['pagesize'])

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all betrokkenerelaties: {round(end - start, 2)}')

    def sync_assets(self):
        start = time.time()
        asset_syncer = AssetSyncer(em_infra_importer=self.eminfra_importer,
                                   triple_query_wrapper=self.triple_wrapper)

        while True:
            params = self.triple_wrapper.get_params()
            asset_syncer.sync_assets(pagingcursor=params['pagingcursor'], page_size=params['pagesize'])

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assets: {round(end - start, 2)}')

    def sync_agents(self, page_size, pagingcursor):
        start = time.time()
        agent_syncer = AgentSyncer(em_infra_importer=self.eminfra_importer, triple_query_wrapper=self.triple_wrapper)
        agent_syncer.sync_agents(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all agents: {round(end - start, 2)}')
