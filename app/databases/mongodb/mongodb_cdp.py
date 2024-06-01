from pymongo import MongoClient

from app.constants.mongo_constants import MongoDBCDPCollections, Operator
from app.decorators.time_exe import sync_log_time_exe, TimeExeTag
from app.utils.logger_utils import get_logger
from config import MongoDBCDPConfig

logger = get_logger('MongoDB CDP')


class MongoDBCDP:
    def __init__(self, connection_url=None, database=MongoDBCDPConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBCDPConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._actions_v2_col = self.db[MongoDBCDPCollections.actions_v2]
        self._web2_events_v2_col = self.db[MongoDBCDPCollections.web2_events_v2]
        self._web2_actions_v2_col = self.db[MongoDBCDPCollections.web2_actions_v2]
        self._users_v2_col = self.db[MongoDBCDPCollections.users_v2]
        self._wallets_v2_col = self.db[MongoDBCDPCollections.wallets_v2]
        self._events_v2_col = self.db[MongoDBCDPCollections.events_v2]
        self._configs_col = self.db[MongoDBCDPCollections.configs]
        self._twitter_users_col = self.db[MongoDBCDPCollections.twitter_users]
        self._twitter_follows_col = self.db[MongoDBCDPCollections.twitter_follows]
        self._twitter_tweets_col = self.db[MongoDBCDPCollections.twitter_tweets]
        self._telegram_users_col = self.db[MongoDBCDPCollections.telegram_users]
        self._telegram_messages_col = self.db[MongoDBCDPCollections.telegram_messages]
        self._discord_servers_col = self.db[MongoDBCDPCollections.discord_servers]
        self._discord_members_col = self.db[MongoDBCDPCollections.discord_members]
        self._discord_messages_col = self.db[MongoDBCDPCollections.discord_messages]
        self._discord_channels_col = self.db[MongoDBCDPCollections.discord_channels]
        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Create index
        ...

    #######################
    #       Actions       #
    #######################
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_actions(self, project, projection=None, last_updated_at=None):
        filter_ = {'project': project}
        if last_updated_at is not None:
            filter_['lastUpdatedAt'] = {'$gt': last_updated_at}

        try:
            cursor = self._actions_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_web3_actions_by_addresses(self, project, addresses, projection=None):
        filter_ = {'_id': {'$in': [f'{project}_{address}' for address in addresses]}}

        try:
            cursor = self._actions_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_web3_actions_by_address(self, project, address, projection=None):
        filter_ = {'_id': f'{project}_{address}'}

        try:
            cursor = self._actions_v2_col.find_one(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    #######################
    #     Web2 actions    #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_web2_actions(self, project, users, projection=None, last_updated_at=None):
        keys = [f'{project}_{user_id}' for user_id in users]
        filter_ = {
            '_id': {'$in': keys}
        }
        if last_updated_at is not None:
            filter_['lastUpdatedAt'] = {'$gt': last_updated_at}

        try:
            cursor = self._web2_actions_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_web2_actions_by_project(self, project, projection=None, last_updated_at=None):
        filter_ = {'project': project}
        if last_updated_at is not None:
            filter_['lastUpdatedAt'] = {'$gt': last_updated_at}

        try:
            cursor = self._web2_actions_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #      Web2 events    #
    #######################
    def get_web2_events_by_timestamp(self, project_id, timestamp):
        key = project_id + "_" + str(timestamp)
        filter_ = {"_id": key}
        cursor = self._web2_events_v2_col.find_one(filter=filter_)
        if not cursor:
            return {}
        return cursor

    #######################
    #        Users        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_users_by_addresses(self, project, addresses, projection=None, last_updated_at=None):
        filter_ = {
            'project': project,
            'address': {'$in': addresses}
        }
        if last_updated_at is not None:
            filter_['lastActiveTime'] = {'$gte': last_updated_at}

        try:
            cursor = self._users_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_addresses_by_users(self, project, user_ids, projection=None):
        keys = [f'{project}_{user_id}' for user_id in user_ids]
        filter_ = {
            '_id': {'$in': keys}
        }

        try:
            cursor = self._users_v2_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Wallets       #
    #######################

    def get_new_wallets(self, create_at, project_id, end_timestamp, projection=None):
        filter_ = {'createAt': {'$gte': create_at, '$lte': end_timestamp}, f'lastInteractionAt.{project_id}': {'$exists': True}}

        try:
            cursor = self._wallets_v2_col.find(filter=filter_, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def count_new_wallets(self, create_at, end_timestamp, project_id):
        filter_ = {'createAt': {'$gte': create_at, '$lte': end_timestamp}, f'lastInteractionAt.{project_id}': {'$exists': True}}

        try:
            count = self._wallets_v2_col.count_documents(filter=filter_)
            return count
        except Exception as e:
            logger.exception(e)

        return 0

    def get_return_wallet(self, project_id, create_at, last_interaction_at, end_timestamp, projection=None):
        _filter = {
            '$and': [
                {
                    f'lastInteractionAt.{project_id}': {
                        '$gte': last_interaction_at,
                        '$lte': end_timestamp
                    }
                }, {
                    'createAt': {
                        '$lt': create_at
                    }
                }
            ]
        }

        try:
            cursor = self._wallets_v2_col.find(filter=_filter, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def count_return_wallet(self, project_id, create_at, last_interaction_at, end_timestamp):
        _filter = {
            '$and': [
                {
                    f'lastInteractionAt.{project_id}': {
                        '$gte': last_interaction_at,
                        '$lte': end_timestamp
                    }
                }, {
                    'createAt': {
                        '$lt': create_at
                    }
                }
            ]
        }

        try:
            count = self._wallets_v2_col.count_documents(filter=_filter)
            return count
        except Exception as e:
            logger.exception(e)

        return 0

    def get_all_interacted_wallets(self, project_id, yesterday, today, projection=None):
        _filter = {
            '$and': [
                {
                    f'lastInteractionAt.{project_id}': {
                        '$gte': yesterday
                    }
                }, {
                    f'lastInteractionAt.{project_id}': {
                        '$lte': today
                    }
                }
            ]
        }

        try:
            cursor = self._wallets_v2_col.find(filter=_filter, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def get_all_wallets(self, project_id=None, projection=None, batch_size=10000):
        filter_statement = {
            '$or': [
                {f'userPseudoId.{project_id}': {Operator.exists: True}},
                {f'lastInteractionAt.{project_id}': {Operator.exists: True}}
            ],
        }

        try:
            cursor = self._wallets_v2_col.find(filter=filter_statement, projection=projection).batch_size(
                batch_size=batch_size)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

    def get_batch_wallets(self, project_id, page=1, page_size=None, sort_by=None, chain_id=None, country=None,
                          keyword=None, projection=None):
        filter_statement = {
            '$or': [
                {f'userPseudoId.{project_id}': {Operator.exists: True}},
                {f'lastInteractionAt.{project_id}': {Operator.exists: True}}
            ]
        }

        if keyword is not None:
            if project_id in keyword:
                keyword = keyword.replace(project_id, '')
            keys = keyword.lower().split(' ')
            regx = f'.*{".*".join(keys)}.*'
            tags_regx = f'{project_id}.*{".*".join(keys)}.*'
            filter_statement = {
                '$and': [
                    filter_statement,
                    {
                        '$or': [
                            {'name': {Operator.regex: regx, Operator.options: 'i'}},
                            {'address': {Operator.regex: regx}},
                            {'tags': {Operator.regex: tags_regx}}
                        ]
                    }
                ]
            }

        if chain_id is not None:
            filter_statement.update({f'balance.{chain_id}': {Operator.exists: True}})

        if country is not None:
            filter_statement.update({'country': country})

        skip = (page - 1) * page_size if page_size is not None else 0
        try:
            cursor = self._wallets_v2_col.find(filter_statement, projection=projection)
            if sort_by is not None:
                cursor = cursor.sort(sort_by)
            cursor = cursor.skip(skip)
            if page_size is not None:
                cursor = cursor.limit(page_size)
            return list(cursor)
        except Exception as e:
            logger.exception(e)
        return []

    def get_wallet_by_address(self, address):
        try:
            doc = self._wallets_v2_col.find_one({'_id': address})
            return doc
        except Exception as e:
            logger.exception(e)
        return None

    #######################
    #        Users        #
    #######################

    def get_new_users(self, first_touch, project_id, end_timestamp, projection=None):
        filter_ = {'firstTouch': {'$gte': first_touch, '$lte': end_timestamp}, 'project': project_id}

        try:
            cursor = self._users_v2_col.find(filter=filter_, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def get_return_user(self, first_touch, last_active, end_timestamp, project_id, projection=None):
        _filter = {
            '$and': [
                {
                    'lastActiveTime': {
                        '$gte': last_active,
                        '$lte': end_timestamp
                    }
                }, {
                    'firstTouch': {
                        '$lt': first_touch
                    }
                }, {
                    'project': project_id
                }
            ]
        }

        try:
            cursor = self._users_v2_col.find(filter=_filter, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def get_daily_user(self, yesterday, today, project_id, projection=None):
        _filter = {
            '$and': [
                {
                    'lastActiveTime': {
                        '$gte': yesterday
                    }
                }, {
                    'lastActiveTime': {
                        '$lte': today
                    }
                }, {
                    'project': project_id
                }
            ]
        }

        try:
            cursor = self._users_v2_col.find(filter=_filter, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    #######################
    #       Events       #
    #######################
    def get_events(self):
        try:
            cursor = self._events_v2_col.find().sort('timestamp', 1)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

    def get_events_in_range(self, time_start, time_end, project_id):
        try:
            cursor = self._events_v2_col.find({
                '$and': [
                    {
                        'timestamp': {
                            '$gte': time_start
                        }
                    }, {
                        'timestamp': {
                            '$lt': time_end
                        }
                    }, {
                        'project': project_id
                    }
                ]
            }).sort('timestamp', 1)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

    def get_events_by_timestamp(self, project_id, timestamp):
        key = project_id + "_" + str(timestamp)
        filter_ = {"_id": key}
        cursor = self._events_v2_col.find_one(filter=filter_)
        if not cursor:
            return {}
        return cursor

    #######################
    #    Twitter Users    #
    #######################
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_twitter_users(self, accounts, projection=None):
        filter_ = {'userName': {'$in': accounts}}

        try:
            cursor = self._twitter_users_col.find(filter=filter_, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def update_twitter_project(self, new_values, project_id):
        filter_ = {'projectId': project_id}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    ##########################
    #    Twitter Followers   #
    ##########################
    def get_twitter_users_by_id(self, ids):
        filter_ = {'_id': {'$in': ids}}
        try:
            cursor = self._twitter_users_col.find(filter=filter_)
            return cursor
        except Exception as e:
            logger.exception(e)

    def get_following(self, accounts):
        filter_ = {'to': {"$in": accounts}}
        projection = {'from': 1}
        try:
            cursor = self._twitter_follows_col.find(filter=filter_, projection=projection)
            return [i['from'] for i in cursor]
        except Exception as e:
            logger.exception(e)

    def get_all_twitter_followers(self):
        filter_ = {}
        projection = {"countLogs": 1, "blue": 1, "country": 1}
        try:
            cursor = self._twitter_users_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

    def get_twitter_follower_quality(self, project_id):
        result = self._configs_col.find_one({"projectId": project_id})
        return result

    def update_follower(self, new_values, _id):
        filter_ = {"_id": _id}
        set_new_values = {
            "$set": new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    def get_twitter_quality_count(self, project_id):
        result = self._configs_col.find_one({'_id': f'{project_id}_twitter_follower_quality'})
        return result

    def get_twitter_country_follower(self, project_id):
        result = self._configs_col.find_one({'_id': f'{project_id}_twitter_follower_country'})
        return result

    #######################
    #    Twitter Tweets   #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_twitter_tweets_by_accounts(self, accounts, start_timestamp, end_timestamp, projection=None):
        filter_ = {
            'authorName': {'$in': accounts},
            'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
        }

        try:
            cursor = self._twitter_tweets_col.find(filter=filter_, projection=projection).batch_size(10000)
            return cursor.sort('timestamp', 1)
        except Exception as e:
            logger.exception(e)

        return []

    def get_all_twitter_tweets(self):
        filter_ = {}
        projection = {"_id": 0, "views": 1, "likes": 1, "replyCounts": 1, "retweetCounts": 1, "url": 1, "text": 1}
        try:
            cursor = self._twitter_tweets_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

    def get_all_twitter_tweets_hashtag(self):
        filter_ = {}
        projection = {"_id": 0, "views": 1, "likes": 1, "replyCounts": 1, "retweetCounts": 1, "hashTags": 1}
        try:
            cursor = self._twitter_tweets_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

    #######################
    #   Telegram Users    #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_telegram_user_by_id(self, project, user_id, projection=None):
        filter_ = {'_id': f'{project}_{user_id}'}

        try:
            user = self._telegram_users_col.find_one(filter=filter_, projection=projection)
            return user
        except Exception as e:
            logger.exception(e)

        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_telegram_users(self, project, user_ids, projection=None):
        keys = [f'{project}_{user_id}' for user_id in user_ids]
        filter_ = {'_id': {'$in': keys}}

        try:
            cursor = self._telegram_users_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_telegram_bots(self, project, projection=None):
        filter_ = {
            'project': project,
            'bot': True
        }

        try:
            cursor = self._telegram_users_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    #######################
    #  Telegram Messages  #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_messages_by_project(self, project, start_timestamp, end_timestamp, projection=None):
        filter_ = {
            'project': project,
            'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
        }

        try:
            cursor = self._telegram_messages_col.find(filter=filter_, projection=projection)
            return cursor.sort('timestamp', 1)
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_messages_by_ids(self, post_ids, projection=None):
        filter_ = {'_id': {'$in': post_ids}}

        try:
            cursor = self._telegram_messages_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Configs       #
    #######################

    def get_config(self, key, projection=None):
        filter_ = {'_id': key}

        try:
            config = self._configs_col.find_one(filter=filter_, projection=projection)
            return config
        except Exception as e:
            logger.exception(e)

        return None

    def get_configs(self, keys, projection=None):
        filter_ = {'_id': {'$in': keys}}

        try:
            cursor = self._configs_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    def get_project_settings(self, project_id, projection=None):
        try:
            settings = self._configs_col.find_one({'_id': f'{project_id}_settings'}, projection=projection)
            return settings
        except Exception as e:
            logger.exception(e)

        return None
    
    #######################
    #    Discord Members  #
    #######################

    # @sync_log_time_exe(tag=TimeExeTag.database)
    # def get_discord_member_by_id(self, project, user_id, projection=None):
    #     filter_ = {'_id': f'{project}_{user_id}'}

    #     try:
    #         user = self._discord_users_col.find_one(filter=filter_, projection=projection)
    #         return user
    #     except Exception as e:
    #         logger.exception(e)

    #     return None
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_member(self, project, user_ids, projection=None):
        keys = [f'{project}_{user_id}' for user_id in user_ids]
        filter_ = {'_id': {'$in': keys}}

        try:
            cursor = self._discord_members_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_discord_members(self):
        filter_ = {}
        projection = {'_id': 0, 'user': 1, 'joined_at': 1}
        try:
            cursor = self._discord_members_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_server_status(self):
        filter_ = {}
        projection = {'_id': 0, 'guild_id': 1, 'approximate_member_count': 1, 'approximate_presence_count': 1}
        try:
            cursor = self._discord_servers_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_discord_messages(self):
        filter_ = {}
        projection = {'_id': 0, 'message_id': 1, 'channel_id': 1, 'reactions': 1, 'author': 1, 'timestamp': 1}
        try:
            cursor = self._discord_messages_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_member_activity(self, project_id):
        result = self._configs_col.find_one({'projectId': project_id})
        return result

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_server_category(self):
        # Get server category by field 'type' in collection 'discord_channels', if 'type' = 4, it's category
        filter_ = {'type': 4}
        projection = {'_id': 0, 'channel_id': 1, 'channel_name': 1, 'type': 1}

        try:
            cursor = self._discord_channels_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)
        
        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_channels_by_category(self, category_id):
        filter_ = {'parent_id': category_id}
        projection = {'_id': 0, 'channel_id': 1, 'channel_name': 1, 'type': 1, 'parent_id': 1}

        try:
            cursor = self._discord_channels_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)
        
        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_messages_and_reactions_by_channel(self,channel_id, projection=None):
        filter_ = {
            'channel_id': channel_id,
            'reactions': {'$exists': True}
        }

        try:
            cursor = self._discord_messages_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_guilds_by_project(self, project_id, projection=None):
        filter_ = {"_id": {"$regex": project_id}}

        try:
            cursor = self._discord_servers_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_discord_users_by_project(self, project_id, projection=None):
        filter_ = {"_id": {"$regex": project_id}}

        try:
            cursor = self._discord_members_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_discord_server(self, new_values, project_id):
        filter_ = {'_id': f'{project_id}_discord_update_join_leave_members'}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_discord_member(self, new_values, project_id):
        filter_ = {'_id': f'{project_id}_discord_update_messages_of_member_data'}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_discord_messages(self, new_values, project_id):
        filter_ = {'_id': f'{project_id}_discord_update_message_logs'}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_discord_server_category(self, new_values, project_id):
        filter_ = {'_id': f'{project_id}_discord_update_category_data'}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_discord_server_status(self, new_values, project_id):
        filter_ = {'_id': f'{project_id}_discord_update_server_status'}
        set_new_values = {
            '$set': new_values
        }
        self._configs_col.update_one(filter_, set_new_values, upsert=True)
