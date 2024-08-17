import datetime
import time

import requests
from redis import Redis
from sanic import Blueprint, Request, json, BadRequest

from app.constants.network_constants import ProviderURI
from app.databases.mongodb.mongodb_dex import MongoDBDex
from app.databases.mongodb.mongodb_klg import MongoDB
from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import PoolQuery, TokensPoolQuery, FeeQuery
from app.services.cached.cache_dapps import CacheDApps
from app.services.jobs.full_dex_liquidity_job import LiquidityPoolEnricher
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate

from job.algorthisms.average_strategy import AverageStrategy
from job.crawlers.uni_pool_data import query_pool_with_tokens
from job.services.fee_from_strategy import uniswap_strategy_algorithm
from job.utils.sqrt_price_math import convert_price_to_tick, convert_tick_to_price

bp = Blueprint('initialize_blueprint', url_prefix='/')

logger = get_logger('Initialize Blueprint')


@bp.get('/all-tokens')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
async def get_list_tokens(request: Request):
    nft_db: NFTMongoDB = request.app.ctx.nft_db
    db: MongoDB = request.app.ctx.db
    cursor = nft_db.get_all_pair()
    unique_tokens = set()
    for doc in cursor:
        unique_tokens.add(doc['token0'])
        unique_tokens.add(doc['token1'])
    symbol = []
    cursor = db.get_contracts_by_keys(keys=[f"0x1_{token}" for token in unique_tokens])
    for doc in cursor:
        address = doc['address']
        if not doc.get('symbol'):
            continue
        symbol.append(({
            'address': address,
            'symbol': doc.get('symbol').upper(),
            'price': doc.get('price')
        }))
    symbol = sorted(symbol, key=lambda x: x['symbol'])
    return json({'tokens': symbol})


@bp.get('/tokens')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="token0", description="token0 address", location="query")
@openapi.parameter(name="token1", description="token1 address", location="query")
@validate(query=TokensPoolQuery)
async def get_pools_with_tokens(request: Request, query: TokensPoolQuery):
    token0_address = query.token0
    token1_address = query.token1
    data = query_pool_with_tokens(token0_address, token1_address)
    max_rating = [{'pool_id': 0, 'tmp_max': float(data[0]['liquidity'])},
                  {"pool_id": 0, 'tmp_max': float(data[0]['txCount'])},
                  {"pool_id": 0, 'tmp_max': float(data[0]['volumeUSD']) / float(data[0]['txCount'])}]
    for idx, d in enumerate(data):
        if float(d['liquidity']) > max_rating[0]['tmp_max']:
            max_rating[0]['pool_id'] = idx
            max_rating[0]['tmp_max'] = float(d['liquidity'])
        if float(d['txCount']) > max_rating[1]['tmp_max']:
            max_rating[1]['pool_id'] = idx
            max_rating[1]['tmp_max'] = float(d['txCount'])
        if float(d['volumeUSD']) / float(d['txCount']) > max_rating[2]['tmp_max']:
            max_rating[2]['pool_id'] = idx
            max_rating[2]['tmp_max'] = float(d['volumeUSD']) / float(d['txCount'])
    best_pool = {}
    for entry in max_rating:
        pool_id = entry['pool_id']
        if pool_id in best_pool:
            best_pool[pool_id] += 1  # Increment count for existing pool ID
        else:
            best_pool[pool_id] = 1
    most_frequent_pool_id = max(best_pool, key=best_pool.get)
    result = {'pools': data, 'bestPoolID': most_frequent_pool_id}

    return json(result)


@bp.get('/pool-info')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="address", description="pool address", location="query")
@validate(query=PoolQuery)
async def get_pool_info(request: Request, query: PoolQuery):
    chain_id = '0x1'
    address = query.address
    dex_db: MongoDBDex = request.app.ctx.dex_db
    nft_db = request.app.ctx.nft_db
    pair = nft_db.get_pair(f"{chain_id}_{address}".lower())
    r: Redis = request.app.ctx.redis

    if pair:
        pool_info_from_subgraph = CacheDApps.get_pool_info(r, address)
        pool_day_info = pool_info_from_subgraph['daily']
        pool_overview = pool_info_from_subgraph['overview']
        prices = []
        for d in pool_day_info:
            date = datetime.datetime.fromtimestamp(d['date'], tz=datetime.timezone.utc)
            prices.append({'timestamp': f"{date.month}/{date.day}/{date.year}", 'price': d['close']} )
        # prices = [for d in pool_day_info]
        provider_uri = ProviderURI.archive_providers['0x1']
        job = LiquidityPoolEnricher(
            pool=address,
            provider_uri=provider_uri,
        )
        job.run()
        liquidity_dict = job.liquidity
        sorted_keys = sorted(liquidity_dict, reverse=True)
        pair_info = dex_db.get_pair_assets(f"{chain_id}_{address}".lower())
        tokens = pair_info.get('assets')
        decimals0 = tokens[0]['decimals']
        decimals1 = tokens[1]['decimals']
        liquidity_list = [{
            'liquidity': liquidity_dict[key],
            'price': convert_tick_to_price(key, decimals0, decimals1)}
            for key in sorted_keys]
        data = {
            'tickSpace': job.tick_space,
            'overview': pool_overview,
            'price30Day': prices,
            'liquidityDensity': {
                'currentTick': job.current_tick,
                'currentPrice': 1.0001 ** job.current_tick * (10 ** (decimals0 - decimals1)),
                'liquidity': liquidity_list,
                "tickNumber": len(job.liquidity)
            }
        }
        return json(data)
    else:
        raise BadRequest('Pool is not supported')


@bp.get('/optimize')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="address", description="pool address", location="query")
@validate(query=PoolQuery)
async def get_best_range_of_pool(request: Request, query: PoolQuery):
    chain_id = "0x1"
    nft_db = request.app.ctx.nft_db
    address = query.address

    pair = nft_db.get_pair(f"{chain_id}_{address}".lower())
    if pair:
        return json({
            'table': pair['bestAPR'],
            'min_max_price': pair["range"],

        })
    else:
        raise BadRequest('Pool is not supported')


@bp.get('/pivot-fee')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="pool_address", description="pool address", location="query")
@openapi.parameter(name="lower_price", description="price lower", location="query")
@openapi.parameter(name="upper_price", description="price upper", location="query")
@validate(query=FeeQuery)
def get_fee_earn_of_range(request: Request, query: FeeQuery):
    chain_id = "0x1"
    dex_db: MongoDBDex = request.app.ctx.dex_db
    address = query.pool_address
    lower_price = query.lower_price
    upper_price = query.upper_price
    r: Redis = request.app.ctx.redis
    pool_info_from_subgraph = CacheDApps.get_pool_info(r, address)

    pool_data = pool_info_from_subgraph['overview']
    hourly_price_data = pool_info_from_subgraph['hourly']

    backtest_data = hourly_price_data[::-1]
    pair_info = dex_db.get_pair_assets(f"{chain_id}_{address}".lower())
    tokens = pair_info.get('assets')
    decimals0 = tokens[0]['decimals']
    decimals1 = tokens[1]['decimals']
    max_tick = convert_price_to_tick(lower_price, decimals0, decimals1)
    min_tick = convert_price_to_tick(upper_price, decimals0, decimals1)

    backtest = uniswap_strategy_algorithm(backtest_data, pool_data, 1000, min_tick, max_tick)
    pivot = backtest['pivotData']
    return json({
        "table": backtest,
        # "pivotData": pivot,
    })


@bp.get('/strategy-2')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="address", description="pool address", location="query")
@validate(query=PoolQuery)
def strategy_average_price(request: Request, query: PoolQuery):
    address = query.address

    r: Redis = request.app.ctx.redis
    pool_info_from_subgraph = CacheDApps.get_pool_info(r, address)

    pool_data = pool_info_from_subgraph['overview']
    hourly_price_data = pool_info_from_subgraph['hourly']
    pool_day_data = pool_info_from_subgraph['daily']
    average_strategy = AverageStrategy(address, pool_data, hourly_price_data, pool_day_data)
    data, best_range = average_strategy.process()
    return json({
        'min_max_price': best_range,
        "table": data,
        # 'pivotData': data['pivotData']

    })
