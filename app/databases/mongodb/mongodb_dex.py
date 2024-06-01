import datetime

import pymongo
from pymongo import MongoClient, UpdateOne

from app.constants.mongo_constants import MongoDBDexCollections
from app.decorators.time_exe import sync_log_time_exe, TimeExeTag
from app.utils.list_dict_utils import flatten_dict, delete_none
from app.utils.logger_utils import get_logger
from config import MongoDBDexConfig

logger = get_logger('MongoDB DEX')


class MongoDBDex:
    def __init__(self, connection_url=None, database=MongoDBDexConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBDexConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._dexes_col = self.db[MongoDBDexCollections.dexes]
        self._pairs_col = self.db[MongoDBDexCollections.pairs]
        self._cexes_col = self.db[MongoDBDexCollections.cexes]
        self._wallets_col = self.db[MongoDBDexCollections.wallets]
        self._tokens_col = self.db[MongoDBDexCollections.tokens]
        self._nfts_col = self.db[MongoDBDexCollections.nfts]
        self._lendings_col = self.db[MongoDBDexCollections.lendings]
        self._tickers_col = self.db[MongoDBDexCollections.tickers]
        self._exchanges_col = self.db[MongoDBDexCollections.exchanges]

        self._token_prices_col = self.db[MongoDBDexCollections.token_prices]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Pairs index
        pairs_col_indexes = self._pairs_col.index_information()
        if 'pairs_index' not in pairs_col_indexes:
            self._pairs_col.create_index(
                [('lastInteractedAt', pymongo.ASCENDING), ('liquidityValueInUSD', pymongo.ASCENDING)],
                name='pairs_index', background=True
            )

    #######################
    #        DEXes        #
    #######################
    def update_dexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._dexes_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_dexes(self, projection=None):
        try:
            cursor = self._dexes_col.find(filter={}, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dexes(self, project_id, chain_id=None, projection=None):
        try:
            filter_statement = {'projectId': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id

            cursor = self._dexes_col.find(filter=filter_statement, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dexes_by_keys(self, keys, projection=None):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._dexes_col.find(filter=filter_statement, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #        Pairs        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_pairs(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._pairs_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs(self, project_id, chain_id=None, projection=None, batch_size=1000):
        try:
            filter_statement = {'project': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id
            cursor = self._pairs_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_by_keys(self, keys, projection=None, batch_size=1000):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._pairs_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_assets(self, keys, batch_size=1000):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._pairs_col.find(filter=filter_statement, projection=['chainId', 'address', 'project', 'tokens'], batch_size=batch_size)

            data = []
            for doc in cursor:
                assets = []
                for token in doc['tokens']:
                    assets.append({
                        'address': token['address'],
                        'amount': token.get('liquidityAmount', 0),
                        'valueInUSD': token.get('liquidityValueInUSD', 0)
                    })

                data.append({
                    'address': doc['address'],
                    'chainId': doc['chainId'],
                    'project': doc['project'],
                    'assets': assets
                })
            return data
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pair_assets(self, key):
        try:
            filter_statement = {'_id': key}
            doc = self._pairs_col.find_one(filter=filter_statement)
            if not doc:
                return None

            assets = []
            for token in doc['tokens']:
                assets.append({
                    'address': token['address'],
                    'amount': token.get('liquidityAmount', 0),
                    'valueInUSD': token.get('liquidityValueInUSD', 0)
                })

            return {
                'address': doc['address'],
                'chainId': doc['chainId'],
                'project': doc['project'],
                'assets': assets
            }
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_token_pairs(self, chain_id, token_address, limit=20):
        try:
            filter_statement = {'chainId': chain_id, 'tokens.address': token_address}
            cursor = (
                self._pairs_col.find(filter=filter_statement)
                .sort('liquidityValueInUSD', pymongo.DESCENDING)
                .limit(limit)
            )
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_with_tokens(self, chain_id, tokens, projects=None, projection=None):
        filter_statement = {
            'chainId': chain_id,
            'tokens': {
                '$elemMatch': {
                    'address': {'$in': tokens}
                }
            },
            'liquidityValueInUSD': {'$gt': 100000}
        }

        if projects is not None:
            filter_statement['project'] = {'$in': projects}
        cursor = self._pairs_col.find(filter_statement, projection=projection)
        return cursor

    #######################
    #        CEXes        #
    #######################

    def update_cexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._cexes_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_cexes(self, projection=None, batch_size=1000):
        try:
            cursor = self._cexes_col.find(filter={}, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_cexes(self, project_id, chain_id=None, projection=None):
        try:
            filter_statement = {'projectId': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id

            cursor = self._cexes_col.find(filter=filter_statement, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #     Hot Wallets     #
    #######################

    def update_hot_wallets(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._wallets_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_hot_wallets(self, project_id, chain_id=None, projection=None, batch_size=1000):
        try:
            filter_statement = {'project': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_hot_wallets_by_keys(self, keys, projection=None, batch_size=1000):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_assets(self, keys, batch_size=1000):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._wallets_col.find(filter=filter_statement, projection=['chainId', 'address', 'project', 'assets'], batch_size=batch_size)

            data = list(cursor)
            return data
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet_assets(self, key):
        try:
            filter_statement = {'_id': key}
            doc = self._wallets_col.find_one(filter=filter_statement)
            return doc
        except Exception as ex:
            logger.exception(ex)
        return None

    #######################
    #       Tokens        #
    #######################

    def update_tokens(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._tokens_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_tokens(self, projection=None, batch_size=1000):
        try:
            cursor = self._tokens_col.find(filter={}, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_keys(self, keys, projection=None):
        try:
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._tokens_col.find(filter=filter_statement, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #        NFTs         #
    #######################

    def update_nfts(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._nfts_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_nfts(self, projection=None, batch_size=1000):
        try:
            cursor = self._nfts_col.find(filter={}, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_nft(self, nft_id):
        try:
            filter_statement = {'_id': nft_id}
            doc = self._nfts_col.find_one(filter=filter_statement)
            return doc
        except Exception as ex:
            logger.exception(ex)
        return None

    #######################
    #       Lending       #
    #######################

    def update_lendings(self, data: list, merge=False, keep_none=True):
        if not merge and keep_none:
            bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
            self._lendings_col.bulk_write(bulk_operations)
        else:
            self.update_docs(MongoDBDexCollections.lendings, data, merge=merge, keep_none=keep_none)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_lending(self, protocol_id, chain_ids: list = None, projection=None):
        if chain_ids is not None:
            keys = [f'{chain_id}_{protocol_id}' for chain_id in chain_ids]
            filter_statement = {'_id': {'$in': keys}}
        else:
            filter_statement = {'projectId': protocol_id}

        try:
            cursor = self._lendings_col.find(filter=filter_statement, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_lendings(self, chain_id=None):
        if chain_id is None:
            filter_statement = {}
        else:
            filter_statement = {'chainId': chain_id}

        try:
            cursor = self._lendings_col.find(filter=filter_statement)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Tickers       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tickers(self, project_id, projection=None):
        filter_ = {
            'idCoingecko': project_id
        }
        try:
            cursor = self._tickers_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tickers_by_projects(self, projects, projection=None):
        filter_ = {
            'idCoingecko': {'$in': projects}
        }
        try:
            cursor = self._tickers_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #      Exchanges      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_exchange_by_id_coingecko(self, project_id, chain_id=None, projection=None):
        try:
            filter_statement = {'_id': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id

            doc = self._exchanges_col.find_one(filter=filter_statement, projection=projection)
            return doc
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_exchanges_by_id(self, project_id, chain_id=None, projection=None):
        try:
            filter_statement = {'projectId': project_id}
            if chain_id is not None:
                filter_statement['chainId'] = chain_id

            cursor = self._exchanges_col.find(filter=filter_statement, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    ######################
    #    Token prices    #
    ######################

    def get_token_prices(self, token_address, chain_id, start_time, end_time, exchange_id=None):
        filter_statement = {
            'metadata.tokenAddress': token_address,
            'metadata.chainId': chain_id,
            'timestamp': {
                '$gte': datetime.datetime.fromtimestamp(start_time).astimezone(datetime.timezone.utc),
                '$lt': datetime.datetime.fromtimestamp(end_time).astimezone(datetime.timezone.utc)
            }
        }
        if exchange_id is not None:
            filter_statement['metadata.exchangeId'] = exchange_id

        cursor = self._token_prices_col.find(filter_statement)
        return cursor

    def get_token_prices_with_aggregate(self, token_address, chain_id, start_time, end_time):
        cursor = self._token_prices_col.aggregate([
            {
                '$match': {
                    'metadata.tokenAddress': token_address,
                    'metadata.chainId': chain_id,
                    'timestamp': {
                        '$gte': datetime.datetime.fromtimestamp(start_time).astimezone(datetime.timezone.utc),
                        '$lt': datetime.datetime.fromtimestamp(end_time).astimezone(datetime.timezone.utc)
                    }
                },
                '$project': {
                    'metadata': 1,
                    'price': 1,
                    'timestamp': 1,
                    'date': {
                        '$dateToParts': {'date': "$timestamp"}
                    },
                }
            },
            {
                '$group': {
                    '_id': {
                        'metadata': '$metadata',
                        'date': {
                            'year': "$date.year",
                            'month': "$date.month",
                            'day': "$date.day"
                        }
                    },
                    'avgPrice': {'$avg': "$price"}
                }
            }
        ])
        return cursor

    #######################
    #        Common       #
    #######################

    def update_docs(self, collection_name, data, keep_none=False, merge=True, shard_key=None, flatten=True):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        try:
            col = self.db[collection_name]
            # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
            bulk_operations = []
            if not flatten:
                if not shard_key:
                    bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
                else:
                    bulk_operations = [UpdateOne({"_id": item["_id"], shard_key: item[shard_key]}, {"$set": item}, upsert=True) for item in data]
                col.bulk_write(bulk_operations)
                return

            for document in data:
                unset, set_, add_to_set = self.create_update_doc(document, keep_none, merge, shard_key)
                if not shard_key:
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$unset": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$set": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$addToSet": {key: value for key, value in item.items() if key != "_id"}},
                                  upsert=True)
                        for item in add_to_set]
                if shard_key:
                    keys = ["_id", shard_key]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$unset": {key: value for key, value in item.items() if key not in keys}},
                                  upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$set": {key: value for key, value in item.items() if key not in keys}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$addToSet": {key: value for key, value in item.items() if key not in keys}},
                                  upsert=True)
                        for item in add_to_set]
            col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    @staticmethod
    def create_update_doc(document, keep_none=False, merge=True, shard_key=None):
        unset, set_, add_to_set = [], [], []
        if not keep_none:
            doc = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    tmp = {
                        "_id": document["_id"],
                        key: ""
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    unset.append(tmp)
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    tmp = {
                        "_id": document["_id"],
                        key: {"$each": [i for i in value if i]}
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    add_to_set.append(tmp)
                else:
                    tmp = {
                        "_id": document["_id"],
                        key: value
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    set_.append(tmp)

        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))

        return unset, set_, add_to_set
