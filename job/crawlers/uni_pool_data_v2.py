import time

from job.constants.network_constant import URL_PROTOCOL
import requests


def pool_by_id(id, protocol):
    url = URL_PROTOCOL.mapping.get(protocol)
    pool_query_fields = """{
        id
        name
        symbol
        totalValueLockedUSD
        cumulativeVolumeUSD
        rewardTokenEmissionsAmount
        rewardTokenEmissionsUSD
        inputTokens {
            id
            name
            symbol
            decimals
            lastPriceUSD
            lastPriceBlockNumber
        }
        fees {
            id
            feePercentage
            feeType
        }
    }"""
    query = "query LiquidityPools($id: ID!) { id: liquidityPools(where: { id: $id } ) " + pool_query_fields + "}"
    try:
        response = requests.post(url, json={'query': query, 'variables': {'id': id}})
        data = response.json()

        if data and 'data' in data:
            pools = data['data']

            if 'id' in pools and len(pools['id']) and len(pools['id']) == 1:
                return pools['id'][0]
        else:
            return None

    except Exception as error:
        return {'error': str(error)}


def get_pool_hour_data(pool, from_date, to_date, protocol):
    url = URL_PROTOCOL.mapping.get(protocol)
    query = """query
    PoolHourDatas($pool: ID!, $fromdate: Int!, $todate: Int!) {
        poolHourDatas(where: {pool:$pool, periodStartUnix_gt:$fromdate periodStartUnix_lt:$todate close_gt: 0}, orderBy: periodStartUnix, orderDirection: desc, first: 1000) {
    periodStartUnix
    liquidity
    high
    low
    volumeUSD
    pool {
      id
      totalValueLockedUSD
      totalValueLockedToken1
      totalValueLockedToken0
      token0
        {decimals}
      token1
        {decimals}
    }
    close
    feeGrowthGlobal0X128
    feeGrowthGlobal1X128
    }
    }
    """
    try:
        response = requests.post(url, json={'query': query,
                                            'variables': {"pool": pool, "fromdate": from_date, "todate": to_date}})
        data = response.json()
        if data and data.get('data') and data.get('data')['poolHourDatas']:
            return data['data']['poolHourDatas']
        else:
            print("nothing returned from getPoolHourData")
            return None
    except Exception as error:
        return error


def get_pool_day_datas(pool, protocol, from_date, to_date):
    url = URL_PROTOCOL.mapping.get(protocol)
    query = """
    query PoolDayDatas($id: ID!, $fromdate: Int!, $todate: Int!) {

        poolDayDatas(where: {pool: $id, date_gte: $fromdate, date_lte: $todate},orderBy: date )
        {
        high
        low
        close
        date
        volumeUSD
          pool {
      token0
        {decimals}
      token1
        {decimals}
    }
        }
      }"""

    try:
        response = requests.post(url, json={'query': query,
                                            'variables': {"id": pool, "fromdate": from_date, "todate": to_date}})

        data = response.json()

        if data and data.get('data') and data.get('data')['poolDayDatas']:
            return data['data']['poolDayDatas']

        else:
            print("nothing returned from PoolDayDatas")
            return None

    except Exception as error:
        return {'error': str(error)}



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
            id
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

