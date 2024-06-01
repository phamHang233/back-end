import sys

from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBCollections
from app.utils.logger_utils import get_logger
from config import MongoDBConfig

logger = get_logger('MongoDB')


class MongoDBLendingKLG:
    def __init__(self, connection_url=None, database=MongoDBConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._wallets_col = self.mongo_db[MongoDBCollections.wallets]
        self._multichain_wallets_col = self.mongo_db[MongoDBCollections.multichain_wallets]
        self._projects_col = self.mongo_db[MongoDBCollections.projects]
        self._smart_contracts_col = self.mongo_db[MongoDBCollections.smart_contracts]

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        ...

    #######################
    #       Wallets       #
    #######################

    def get_all_deposit_wallets(self, chain_id=None, projection=None):
        filter_ = {'depositInUSD': {'$gt': 0}}
        if chain_id is not None:
            filter_['chainId'] = chain_id

        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find(filter_, projection=projection_statement)
        return cursor

    def get_all_wallets(self, projection=None, batch_size=10000):
        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find({}, projection=projection_statement)
        return cursor.batch_size(batch_size=batch_size)

    #######################
    #       Common        #
    #######################

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements
