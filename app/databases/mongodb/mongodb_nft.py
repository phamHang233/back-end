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

        self.nfts = self.db[NFTMongoDBCollections.nfts]
        self.wallets = self.db[NFTMongoDBCollections.wallets]
        self.pairs = self.db[NFTMongoDBCollections.pairs]

    ##############
    ##  WALLET  ##
    ##############
    def get_wallet_by_addresses(self, addresses, chain_id='0x1'):
        keys = [f"{chain_id}_{address}" for address in addresses]
        cursor = self.wallets.find({"_id": {"$in": keys}})
        return cursor

    def get_top_wallets(self):
        cursor = self.wallets.find({"chainId": "0x1"}).sort('apr', pymongo.DESCENDING).limit(10)
        return cursor


    ##############
    ##     NFT  ##
    ##############
    def get_nfts_by_keys(self, keys):
        cursor = self.nfts.find({"tokenId": {"$in": keys}})
        return cursor

    def get_nfts_by_key(self, key):
        cursor = self.nfts.find_one({"tokenId": key})
        return cursor

    def get_top_nfts(self, address=None):
        if address:

            cursor = self.nfts.find({"chainId": "0x1", "poolAddress": address.lower()}).sort('aprInMonth',
                                                                                     pymongo.DESCENDING).limit(20)
        else:
            cursor = self.nfts.find({"chainId": "0x1"}).sort('aprInMonth', pymongo.DESCENDING).limit(20)
        return cursor

    def get_nft_by_ids(self, keys):
        return list( self.nfts.find({"_id": {"$in": keys}}))

    def get_nft_by_wallet(self, wallet):
        return self.nfts.find({"wallet": wallet.lower()})


    ##############
    ##  WALLET  ##
    ##############
    def get_pair(self, key):
        try:
            cursor = self.pairs.find_one({"_id": key})
            return cursor
        except Exception as e:
            logger.exception(e)
            return None

    def get_all_pair(self):
        return self.pairs.find({"bestAPR": {"$exists": True}})
