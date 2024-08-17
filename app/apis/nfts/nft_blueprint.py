from sanic import Blueprint, Request, json

from app.databases.mongodb.mongodb_dex import MongoDBDex
from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import NFTQuery, PoolQuery
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate

bp = Blueprint('nfts_blueprint', url_prefix='/')

logger = get_logger('NFTS Blueprint')


@bp.get('/nft-info')
@openapi.tag("NFT")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="token_id", description="DEX NFT IDS", location="query")
@validate(query=NFTQuery)
async def get_nft_info(request: Request, query: NFTQuery):
    nft_db: NFTMongoDB = request.app.ctx.nft_db
    dex_db: MongoDBDex = request.app.ctx.dex_db

    token_id = query.token_id
    doc = nft_db.get_nfts_by_key(key=token_id)
    pool_address = doc['poolAddress']
    pool_info = dex_db.get_pair_assets(f"0x1_{pool_address}")
    tokens = pool_info['assets']
    return json( {
            'tokenId': doc['tokenId'],
            'tickLower': doc['tickLower'],
            'tickUpper': doc['tickUpper'],
            'tickCurrent': pool_info.get('tick'),
            'poolAsset': doc['assetsInUSD'],
            'token0': tokens[0],
            'token1': tokens[1],
            'pnl': doc['PnL'],
            'totalAPR': doc['aprInMonth'] * 100,
            'feeAPR': doc.get('feeEarn', 0) / doc.get("assetsInUSD") * 100 if doc.get("assetsInUSD") else 0,
            'uncollectedFee': doc['uncollectedFee'],
            'totalFeeUSD': doc['feeEarn'],
            'collectedFee': doc.get("collectedFee", []),
        })


@bp.get('/top-nfts')
@openapi.tag("NFT")
@openapi.summary("Get TOP NFT info of DEX V3")
@openapi.parameter(name="address", description="pool address", location="query")
@validate(query=PoolQuery)
async def get_top_nft(request: Request, query: PoolQuery):
    nft_db: NFTMongoDB = request.app.ctx.nft_db
    dex_db: MongoDBDex = request.app.ctx.dex_db
    # address = query.address
    # if address:
    #     cursor = nft_db.get_top_nfts(address)
    #     pool_infos = {address: dex_db.get_pair_assets(f"0x1_{address}")}
    # else:
    cursor = list(nft_db.get_top_nfts())
    pool_addresses = [f"0x1_{doc['poolAddress']}" for doc in cursor]
    pool_infos = dex_db.get_pairs_assets(pool_addresses)

    nfts = []
    for doc in cursor:
        pool_address = doc['poolAddress']
        pool_info = pool_infos.get(pool_address)
        tokens = pool_info['assets']
        nfts.append({
            'nftId': doc['tokenId'],
            'apr': doc.get('aprInMonth') * 100,
            'assetsInUSD': doc.get("assetsInUSD"),
            'poolAddress': pool_address,
            'poolToken0': tokens[0]['symbol'],
            'poolToken1': tokens[1]['symbol'],
            'feeAPR': doc.get('feeEarn', 0) / doc.get("assetsInUSD") * 100 if doc.get("assetsInUSD") else 0,
            # 'investedAssetInUSD': doc.get("investedAssetInUSD"),
            'pnl': doc.get("PnL"),
            'owner': doc.get("wallet")

        })
    return json({"nfts": nfts})
