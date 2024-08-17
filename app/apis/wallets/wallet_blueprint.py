from sanic import Blueprint, Request, json

from app.databases.mongodb.mongodb_dex import MongoDBDex
from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import NFTQuery, WalletQuery, NFTSQuery
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate


bp = Blueprint('wallets_blueprint', url_prefix='/')

logger = get_logger('Wallets Blueprint')


@bp.get('/wallet')
@openapi.tag("Wallet")
@openapi.summary("Get wallet info of DEX V3")
@openapi.parameter(name="address", description="Wallet Address", location="query")
@validate(query=WalletQuery)
async def get_wallet_info(request: Request, query: WalletQuery):
    db: NFTMongoDB = request.app.ctx.db
    address = query.address
    cursor = db.get_wallet_by_addresses(addresses=[address])
    wallets = []
    for doc in cursor:
        wallets.append(doc)
    return json({'wallets': wallets})


@bp.get('/top-wallet')
@openapi.tag("Wallet")
@openapi.summary("Get wallet info of DEX V3")
# @openapi.parameter(name="address", description="Wallet Address", location="query")
# @validate(query=WalletQuery)
async def get_top_wallets(request: Request):
    nft_db: NFTMongoDB = request.app.ctx.nft_db

    cursor = nft_db.get_top_wallets()
    wallets = []
    for doc in cursor:
        wallets.append({
            'address': doc['address'],
            'totalAssets': doc['totalAsset'],
            'PnL': doc.get("PnL"),
            'apr': doc.get('apr'),
            'numberNfts': len(doc['nfts']),
            "nfts": doc['nfts']
        })

    return json({"wallets": wallets})


@bp.get('/nfts-wallet')
@openapi.tag("Wallet")
@openapi.summary("Get nfts info of DEX V3")
@openapi.parameter(name="nfts", description="Token ids, separated by commas.", location="query")

@validate(query=NFTSQuery)
async def get_nfts_by_wallets(request: Request, query: NFTSQuery):
    nft_db: NFTMongoDB = request.app.ctx.nft_db
    # wallet_add = query.address
    nfts = [t.strip() for t in query.nfts.replace('"', "").split(',')]

    cursor = nft_db.get_nft_by_ids(nfts)
    dex_db: MongoDBDex = request.app.ctx.dex_db
    addresses = [f"0x1_{doc['poolAddress']}" for doc in cursor]
    pool_infos = dex_db.get_pairs_assets(addresses)

    nfts_info = []
    for doc in cursor:
        pool_address = doc['poolAddress']
        pool_info = pool_infos.get(pool_address)
        if pool_info:
            tokens = pool_info['assets']
            nfts_info.append({
                'nftId': doc['tokenId'],
                'apr': doc.get('aprInMonth') * 100,
                'assetsInUSD': doc.get("assetsInUSD"),
                'poolToken0': tokens[0]['symbol'],
                'poolToken1': tokens[1]['symbol'],
                'feeAPR': doc.get('feeEarn', 0) / doc.get("assetsInUSD") * 100 if doc.get("assetsInUSD") else 0,
                # 'investedAssetInUSD': doc.get("investedAssetInUSD"),
                'pnl': doc.get("PnL"),
                'owner': doc.get("wallet")

            })
    return json({'nfts': nfts_info})




