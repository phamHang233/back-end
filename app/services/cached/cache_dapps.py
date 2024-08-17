import time

from redis import Redis

from app.constants.time_constants import TimeConstants
from app.services.cached.constants import CachedKeys
from app.services.cached.redis_cached import RedisCached
from app.utils.logger_utils import get_logger
from job.crawlers.uni_pool_data import get_pool_hour_data, pool_by_id, get_pool_day_datas

logger = get_logger('Cache DApps')


class CacheDApps(RedisCached):
    @classmethod
    def get_pool_info(cls, r: Redis, pool_address):
        key = f"{CachedKeys.dapp_protocols}_{pool_address}"
        # cls.delete_cache(r, key)

        pool_info = cls.get_cache(r, key)
        if not pool_info:
            pool_info = cls.cache_pool_info(r, pool_address)
        return pool_info

    @classmethod
    def cache_pool_info(cls, r: Redis, pool_address):
        end_timestamp = int(time.time())
        start_timestamp = end_timestamp - 30 * 24 * 3600
        pool_data = pool_by_id(pool_address, "ethereum")
        hourly_price_data = get_pool_hour_data(pool_address, start_timestamp, end_timestamp, "ethereum")

        pool_day_info = get_pool_day_datas(pool_address, "ethereum", from_date=start_timestamp, to_date=end_timestamp)
        data = {
            'overview': pool_data,
            'hourly': hourly_price_data,
            'daily': pool_day_info
        }
        key = f"{CachedKeys.dapp_protocols}_{pool_address}"
        cls.set_cache(r, key=key, value=data, ttl=TimeConstants.A_HOUR)

        return data

