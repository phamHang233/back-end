from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBLabelCollections
from app.utils.logger_utils import get_logger
from config import MongoDBLabelConfig

logger = get_logger('MongoDB Label')


class MongoDBLabel:
    def __init__(self, connection_url=None, database=MongoDBLabelConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBLabelConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        self.mongo_db = self.connection[database]

        self._wallets_col = self.mongo_db[MongoDBLabelCollections.wallets]
        self._smart_contracts_col = self.mongo_db[MongoDBLabelCollections.smart_contracts]
        self._labels_col = self.mongo_db[MongoDBLabelCollections.labels]

    def get_labels(self, address, chain_id):
        key = f'{chain_id}_{address}'
        wallet = self._wallets_col.find_one({'_id': key}) or {}
        smart_contract = self._smart_contracts_col.find_one({'_id': key}) or {}

        labels = []
        labels.extend(smart_contract.get('labels', []))
        labels.extend(wallet.get('labels', []))
        wallet.update(smart_contract)

        name = smart_contract.get('name') or wallet.get('name')
        if name:
            labels.insert(0, name)
        return labels
