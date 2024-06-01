import time

from redis import Redis

from app.constants.time_constants import TimeConstants
from app.services.cached.constants import CachedKeys
from app.services.cached.redis_cached import RedisCached
from app.utils.logger_utils import get_logger
from app.utils.time_utils import round_timestamp

logger = get_logger('Cache Calls')


class CacheCalls(RedisCached):
    """Limit 200 calls per 5 minutes. """

    @classmethod
    def get_on_minute_calls(cls, r: Redis, ip):
        ts = int(time.time() / TimeConstants.MINUTES_5) * TimeConstants.MINUTES_5
        calls = r.get(f'{ip}.{ts}') or 0
        return int(calls)

    @classmethod
    def call(cls, r: Redis, ip):
        ts = int(time.time() / TimeConstants.MINUTES_5) * TimeConstants.MINUTES_5
        calls = r.incr(f'{ip}.{ts}')
        if calls == 1:
            r.expire(f'{ip}.{ts}', TimeConstants.MINUTES_5)

    @classmethod
    def daily_requests_count(cls, r: Redis, api_key, status_code):
        daily_ts = round_timestamp(int(time.time()))
        key = f'{CachedKeys.requests_count}:{api_key}.{daily_ts}.{status_code}'

        calls = r.incr(key)
        if calls == 1:
            r.expire(key, 2 * TimeConstants.A_DAY)

        return key, calls

    @classmethod
    def get_daily_requests_count(cls, r: Redis):
        keys = r.keys(f'{CachedKeys.requests_count}:*')

        data = {}
        for key in keys:
            count = int(r.get(key))
            data[key.decode()] = count

        return data
