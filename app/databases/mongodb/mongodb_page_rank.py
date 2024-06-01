import pymongo
from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBPageRankCollections
from app.decorators.time_exe import sync_log_time_exe, TimeExeTag
from app.utils.logger_utils import get_logger
from config import MongoDBPageRankConfig

logger = get_logger('MongoDB Page Rank')


class MongoDBPageRank:
    def __init__(self, connection_url=None, database=MongoDBPageRankConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBPageRankConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._wallets_col = self.db[MongoDBPageRankCollections.wallets]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Pairs index
        ...

    #######################
    #        Wallets      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_wallets(self, projection=None):
        try:
            cursor = self._wallets_col.find(filter={}, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_top_wallets(self, skip=0, limit=None, projection=None):
        try:
            cursor = self._wallets_col.find(filter={}, projection=projection).sort('score', direction=pymongo.DESCENDING).skip(skip=skip)
            if limit is not None:
                cursor = cursor.limit(limit=limit)
            return cursor.batch_size(1000)
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_with_addresses(self, project_id, addresses, projection=None):
        keys = [f'{project_id}_{address}' for address in addresses]
        try:
            cursor = self._wallets_col.find(filter={'_id': {'$in': keys}}, projection=projection)
            return cursor.batch_size(1000)
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_with_address(self, project_id, address):
        key = f'{project_id}_{address}'
        try:
            doc = self._wallets_col.find_one(filter={'_id': key})
            return doc
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_number_of_docs(self):
        try:
            return self._wallets_col.count_documents(filter={})
        except Exception as ex:
            logger.exception(ex)
        return 0
