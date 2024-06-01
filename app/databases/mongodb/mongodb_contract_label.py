from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBContractLabelCollections
from app.utils.logger_utils import get_logger
from config import MongoDBContractLabelConfig

logger = get_logger('MongoDB Contract Label')


class MongoDBContractLabel:
    def __init__(self, connection_url=None, database=MongoDBContractLabelConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBContractLabelConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._mapped_col = self.db[MongoDBContractLabelCollections.mapped_projects]
        self._protocols_col = self.db[MongoDBContractLabelCollections.protocols]
        self._smart_contracts_col = self.db[MongoDBContractLabelCollections.smart_contracts]

    def get_mapped_projects(self, ids):
        filter_statement = {'projects': {'$in': ids}}
        cursor = self._mapped_col.find(filter_statement)

        mapping = {}
        for doc in cursor:
            p_id = doc['_id']
            for dune_p_id in doc.get('projects', []):
                if dune_p_id not in mapping:
                    mapping[dune_p_id] = {'id': p_id, 'category': doc.get('category')}

        return mapping

    def get_protocol_reserves_list(self, protocol_ids):
        filter_statement = {'_id': {'$in': protocol_ids}}
        cursor = self._protocols_col.find(filter_statement)

        protocols_reserves_list = {}
        for doc in cursor:
            reserves_list = doc.get('reservesList', {})
            protocols_reserves_list[doc['_id']] = reserves_list

        return protocols_reserves_list

    def get_protocol_info(self, protocol_ids):
        filter_statement = {'_id': {'$in': protocol_ids}}
        cursor = self._protocols_col.find(filter_statement)

        protocols_info = {}
        for doc in cursor:
            protocols_info[doc['_id']] = doc

        return protocols_info

    def get_protocol_info_by_chains(self, chains):
        filter_statement = {'chainId': {'$in': chains}}
        cursor = self._protocols_col.find(filter_statement)

        return cursor

    def get_smart_contracts_by_keys(self, keys):
        filter_statement = {'_id': {'$in': keys}}
        cursor = self._smart_contracts_col.find(filter_statement)

        return cursor
