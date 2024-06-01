from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBCommunityCollections
from app.utils.logger_utils import get_logger
from config import MongoDBCommunityConfig

logger = get_logger('MongoDB Community')


class MongoDBCommunity:
    def __init__(self, connection_url=None, database=MongoDBCommunityConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBCommunityConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        self.mongo_db = self.connection[database]

        self._deposit_users_col = self.mongo_db[MongoDBCommunityCollections.deposit_users]
        self._user_deposits_col = self.mongo_db[MongoDBCommunityCollections.user_deposits]

    def get_relate_exchange(self, address, chain_id):
        key = f'{chain_id}_{address}'
        deposit_wallet = self._deposit_users_col.find_one({'_id': key}) or {}
        user_wallet = self._user_deposits_col.find_one({'_id': key}) or {}
        return {
            'user_wallets': deposit_wallet.get('userWallets', []),
            'deposit_wallets': user_wallet.get('depositWallets', [])
        }
