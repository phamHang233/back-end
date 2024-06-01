from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBdYdXCollections
from app.constants.network_constants import Chain
from app.decorators.time_exe import TimeExeTag, sync_log_time_exe
from app.utils.logger_utils import get_logger
from config import MongoDBdYdXConfig

logger = get_logger('MongoDB dYdX')


class MongoDBDydx:
    def __init__(self, connection_url=None, database=MongoDBdYdXConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBdYdXConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._wallets_col = self.db[MongoDBdYdXCollections.wallets]
        self._contracts_col = self.db[MongoDBdYdXCollections.smart_contracts]
        self._events_col = self.db[MongoDBdYdXCollections.events]
        self._configs_col = self.db[MongoDBdYdXCollections.configs]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        ...

    #######################
    #       Wallets       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet(self, address, chain_id=Chain.ETH, projection=None):
        try:
            wallet = self._wallets_col.find_one({'_id': f'{chain_id}_{address}'}, projection=projection)
            return wallet
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_wallets(self, chain_id=None, tag=None, projection=None):
        filter_ = {}
        if chain_id:
            filter_['chainId'] = chain_id
        if tag:
            filter_['tags'] = tag

        try:
            cursor = self._wallets_col.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_all_wallets_by_flagged(self, chain_id=None, flag=None):
        filter_ = {}
        if chain_id:
            filter_['chainId'] = chain_id
        if flag:
            filter_['flagged'] = flag

        try:
            cursor = self._wallets_col.find(filter_)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_wallet_by_filter(self, filter_):
        cursor = self._wallets_col.find(filter_)
        return cursor

    def count_wallet_by_filter(self, filter_):
        return self._wallets_col.count_documents(filter_)

    #######################
    #      Contracts      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dydx_contracts(self, chain_id=None, projection=None):
        filter_ = {'project': 'dydx'}
        if chain_id:
            filter_['chainId'] = chain_id

        try:
            cursor = self._contracts_col.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Events        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dydx_events(self, contract_addresses, from_block, to_block, projection=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_number': {'$gte': from_block, '$lt': to_block}
        }

        try:
            cursor = self._events_col.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Configs       #
    #######################

    def get_config(self, key, filter_=None):
        config = {}
        if key:
            config = self._configs_col.find_one({'_id': key})
        if filter_:
            config = self._configs_col.find_one(filter_)
        return config

    def get_configs(self, keys, filter_, projection=None):
        config = {}
        if keys:
            config = self._configs_col.find({'_id': {'$in': keys}}, projection=projection)
        if filter_:
            config = self._configs_col.find(filter_, projection=projection)
        return config
