import sys

from pymongo import MongoClient

from app.utils.logger_utils import get_logger
from config import CenticDBConfig

logger = get_logger('Centic DB')


class CenticDB:
    def __init__(self, connection_url=None):
        if connection_url is None:
            connection_url = CenticDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.client = MongoClient(connection_url)
        except Exception as ex:
            logger.warning("Failed connecting to MongoDB Centic")
            logger.exception(ex)
            sys.exit(1)

        self._db = self.client[CenticDBConfig.DATABASE]

        self._projects_coll = self._db['projects']

    #######################
    #       Projects      #
    #######################

    def get_project_settings(self, project_id):
        project = self._projects_coll.find_one({'projectId': project_id})
        if not project:
            return {}

        return project.get('settings', {})
