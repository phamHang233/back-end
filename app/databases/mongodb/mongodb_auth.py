import datetime

from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBAuthCollections
from app.utils.logger_utils import get_logger
from config import MongoDBAuthConfig

logger = get_logger('MongoDB Auth')


class MongoDBAuth:
    def __init__(self, connection_url=None, database=MongoDBAuthConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBAuthConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._keys_col = self.db[MongoDBAuthCollections.apigee_keys]
        self._requests_col = self.db[MongoDBAuthCollections.requests_count]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        ...

    #######################
    #        Keys         #
    #######################

    def get_api_key_by_wallet(self, address):
        try:
            key_info = self._keys_col.find_one({'attributes.address': address})
            return key_info
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_api_key(self, api_key):
        try:
            key_info = self._keys_col.find_one({'apiKey': api_key})
            return key_info
        except Exception as ex:
            logger.exception(ex)
        return None

    def save_api_key(self, key_info):
        try:
            _id = key_info['apiKey']
            self._keys_col.update_one({'_id': _id}, {'$set': key_info}, upsert=True)
        except Exception as ex:
            logger.exception(ex)

    def delete_api_key(self, api_key):
        try:
            self._keys_col.delete_one({'apiKey': api_key})
        except Exception as ex:
            logger.exception(ex)

    def get_api_keys_info(self, api_keys):
        cursor = self._keys_col.find(
            filter={'_id': {'$in': api_keys}},
            projection=['apiKey', 'appName', 'attributes']
        )
        return cursor

    #######################
    #        Keys         #
    #######################

    def save_requests_count(self, data):
        operators = []
        for info in data:
            timestamp = info.pop('timestamp')
            count = info.pop('count')
            operators.append({
                'metadata': info,
                'timestamp': datetime.datetime.fromtimestamp(timestamp).astimezone(datetime.timezone.utc),
                'count': count
            })
        self._requests_col.insert_many(operators, ordered=False)

    def get_requests_count(self, api_key, start_timestamp, end_timestamp):
        filter_statement = {
            'metadata.apiKey': api_key,
            'timestamp': {
                '$gte': datetime.datetime.fromtimestamp(start_timestamp).astimezone(datetime.timezone.utc),
                '$lte': datetime.datetime.fromtimestamp(end_timestamp).astimezone(datetime.timezone.utc)
            }
        }
        cursor = self._requests_col.find(filter_statement)
        return cursor

    def get_requests_count_by_keys(self, start_timestamp, end_timestamp):
        filter_statement = {
            'timestamp': {
                '$gte': datetime.datetime.fromtimestamp(start_timestamp).astimezone(datetime.timezone.utc),
                '$lte': datetime.datetime.fromtimestamp(end_timestamp).astimezone(datetime.timezone.utc)
            }
        }
        cursor = self._requests_col.find(filter_statement)
        return cursor
