import os
import time
import warnings

import redis
from redis import Redis
from sanic import json, Request, Unauthorized
from sanic_ext import openapi

from app import create_app
from app.apis import api_v3

from app.misc.log import log
from app.services.cached.cache_calls import CacheCalls
from app.utils.logger_utils import get_logger
from config import Config, LocalDBConfig, RedisConfig

warnings.filterwarnings('ignore')

logger = get_logger('Main')

app = create_app(Config, LocalDBConfig)
# app.ext.openapi.add_security_scheme('x-apikey', 'apiKey', location='header', name='x-apikey')
app.ext.openapi.raw(Config.raw)
# app.blueprint(admin_bp)
app.blueprint(api_v3)


# @app.before_server_start
# async def setup_db(_):
#     app.ctx.db = MongoDB()
    # log(f'Connected to KLG Database {app.ctx.db.connection_url}')
    #
    # app.ctx.mongo = BlockchainETL(BlockchainETLConfig.TEST_CONNECTION_URL)
    # app.ctx.etl = BlockchainETL(BlockchainETLConfig.CONNECTION_URL)
    # app.ctx.dex_db = MongoDBDex()
    #
    # app.ctx.transfer_db = TokenTransferDB()


@app.before_server_start
async def setup_cache(_):
    app.ctx.redis = redis.from_url(RedisConfig.CONNECTION_URL)
    log('Setup Redis cached')


@app.route("/ping", methods={'GET'})
@openapi.exclude()
@openapi.tag("Ping")
@openapi.summary("Ping server !")
async def hello_world(request: Request):
    # For another version
    # cookie = request.cookies.get('activities_marked')
    # print(f'Cookie: {cookie}')
    #
    # if not cookie:
    #     cookie = 0
    # else:
    #     cookie = int(cookie)
    #
    # cookie += 1
    # response = json({'message': f"Hello, World {cookie} !!!"})
    # response.add_cookie(
    #     key='activities_marked',
    #     value=str(cookie),
    #     path='/',
    #     domain=None,
    #     max_age=300,
    #     secure=False
    # )

    response = json({
        "description": "Success",
        "status": 200,
        "message": f"App {request.app.name}: Hello, World !!!"
    })
    return response


# For using Google Cloud Apigee
# @app.middleware('request')
# async def proxy_required(request: Request):
#     if not GCloudConfig.PROXY_REQUIRED:
#         return
#
#     if request.path.startswith('/docs'):
#         return
#
#     proxy_api_secret = request.headers.get('x-api-secret')
#     if (not proxy_api_secret) or (proxy_api_secret != GCloudConfig.PROXY_API_SECRET):
#         status_code = 407
#         return json({
#             "description": "Proxy Authentication Required",
#             "status": status_code,
#             "message": "Authentication is needed to be done by proxy"
#         }, status=status_code)

#
# @app.middleware('request')
# async def auth(request: Request):
#     # Ignore check authenticate
#     if (request.method == 'OPTIONS') or (request.path == '/docs') or request.path.startswith('/docs/') or (request.path == '/ping'):
#         return
#
#     # Check x-apikey
#     api_key = request.headers.get('x-apikey')
#     if not api_key:
#         raise Unauthorized('x-apikey is required')
#
#     r: redis.Redis = request.app.ctx.redis
    # key_info = CacheAuth.get_key_info(r=r, api_key=api_key)
    # if not key_info:
    #     raise Unauthorized('x-apikey is invalid or expired')
    #
    # # Add address
    # if key_info.get('address'):
    #     request.headers['address'] = key_info.get('address')


@app.middleware('response')
async def count_request(request: Request, response):
    try:
        r: Redis = request.app.ctx.redis
        api_key = request.headers.get('x-apikey')
        if api_key:
            CacheCalls.daily_requests_count(r=r, api_key=api_key, status_code=response.status)
    except Exception as ex:
        logger.exception(ex)


@app.middleware('request')
async def add_start_time(request: Request):
    request.headers['start_time'] = time.time()


@app.middleware('response')
async def add_spent_time(request: Request, response):
    try:
        if 'start_time' in request.headers:
            timestamp = request.headers['start_time']
            spend_time = round((time.time() - timestamp), 3)
            response.headers['latency'] = spend_time

            msg = "{status} {method} {path} {query} {latency}s".format(
                status=response.status,
                method=request.method,
                path=request.path,
                query=request.query_string,
                latency=spend_time
            )
            if response.status >= 400:
                logger.error(msg)
            elif response.status >= 300:
                logger.warning(msg)
            else:
                logger.info(msg)
    except Exception as ex:
        logger.exception(ex)


if __name__ == '__main__':
    if 'SECRET_KEY' not in os.environ:
        log(message='SECRET KEY is not set in the environment variable.',
            keyword='WARN')

    try:
        app.run(**app.config['RUN_SETTING'])
    except (KeyError, OSError):
        log('End Server...')
