import logging
from pathlib import Path

from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from Syncer import Syncer
from TripleQueryWrapper import TripleQueryWrapper

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_TripleStoreSyncer.json')
    db_settings = settings_manager.settings['databases']['prd']

    triple_wrapper = TripleQueryWrapper(use_graph_db=True,
                                        url_or_path=db_settings['host'] + db_settings['database'],
                                        otl_db_path=Path('OTL 2.5.db'))
    triple_wrapper.init_params()

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
    request_handler = RequestHandler(requester)

    eminfra_importer = EMInfraImporter(request_handler)
    syncer = Syncer(triple_wrapper=triple_wrapper, request_handler=request_handler, eminfra_importer=eminfra_importer,
                    settings=settings_manager.settings)

    syncer.start_syncing()