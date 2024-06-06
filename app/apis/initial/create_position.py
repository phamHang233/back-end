import time

import requests
from sanic import Blueprint, Request, json

from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import PoolQuery, TokensPoolQuery
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate

from job.algorthisms.average_strategy import AverageStrategy
from job.algorthisms.genetic_algorithms import GeneticAlgorithms
from job.constants.network_constant import URL_PROTOCOL

bp = Blueprint('initialize_blueprint', url_prefix='/')

logger = get_logger('Initialize Blueprint')


@bp.get('/tokens')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="token0", description="token0 address", location="query")
@openapi.parameter(name="token1", description="token1 address", location="query")
@validate(query=TokensPoolQuery)
async def get_pool_with_tokens(request: Request, query: TokensPoolQuery):
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


@bp.get('/optimize')
@openapi.tag("Position")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="address", description="pool address", location="query")
@validate(query=PoolQuery)
async def get_best_range_of_pool(request: Request, query: PoolQuery):
    address = query.address
    end_timestamp = int(time.time())
    start_timestamp = end_timestamp - 30 * 24 * 3600
    ga = GeneticAlgorithms(pool=address, start_timestamp=start_timestamp,
                           end_timestamp=end_timestamp)
    best_apr_1, best_range_1 = ga.process()

    average_s = AverageStrategy(pool=address, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    best_apr_2, best_range_2 = average_s.process()

    return json({
        'strategy1': {'apr': best_apr_1, 'range': best_range_1},
        'strategy2': {'apr': best_apr_2, 'range': best_range_2},
    })

def query_pool_with_tokens(token0, token1, protocol='ethereum'):
    url = URL_PROTOCOL.mapping.get(protocol)
    query = """
    query Pools($token0: String!, $token1: String!){
        pools(
            where: {
                token0: $token0,
                token1: $token1
            }
        ) {
            liquidity
            volumeUSD
            feesUSD
            txCount
            feeTier
        }
    }
    """
    try:
        response = requests.post(url, json={'query': query,
                                            'variables': {"token0": token0, "token1": token1}})
        data = response.json()
        if data and data.get('data') and data.get('data')['pools']:
            return data['data']['pools']
        else:
            response = requests.post(url, json={'query': query,
                                                'variables': {"token0": token1, "token1": token0}})
            data = response.json()
            if data and data.get('data') and data.get('data')['pools']:
                return data['data']['pools']
            else:
                print("nothing returned from getPoolHourData")
                return None
    except Exception as error:
        return error

