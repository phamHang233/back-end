import sys

import pymongo
from pymongo import MongoClient

from app.constants.mongo_constants import NFTMongoDBCollections
from app.utils.logger_utils import get_logger
from config import NFTMongoDBConfig

logger = get_logger('MongoDB')


class NFTMongoDB:
    def __init__(self, connection_url=None, database=NFTMongoDBConfig.DATABASE):
        if not connection_url:
            connection_url = NFTMongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.mongo_db = MongoClient(connection_url)
            self.db = self.mongo_db[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self.nfts = self.mongo_db[NFTMongoDBCollections.nfts]
        self.wallets = self.mongo_db[NFTMongoDBCollections.wallets]

    ##############
    ##  WALLET  ##
    ##############
    def get_wallet_by_addresses(self, addresses, chain_id='0x1'):
        keys = [f"{chain_id}_{address}" for address in addresses]
        cursor = self.wallets.find({"_id": {"$in": keys}})
        return cursor

    def get_top_wallets(self):
        cursor = self.nfts.find({}).sort('apr', pymongo.DESCENDING).limit(20)
        return cursor


    ##############
    ##     NFT  ##
    ##############
    def get_nfts_by_keys(self, keys):
        cursor = self.nfts.find({"tokenId": {"$in": keys}})
        return cursor

    def get_top_nfts(self,_limit):
        cursor = self.nfts.find({}).sort('aprInMonth', pymongo.DESCENDING).limit(_limit)
        return cursor
