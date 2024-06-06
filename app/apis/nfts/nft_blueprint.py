from sanic import Blueprint, Request, json

from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import NFTQuery
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate


bp = Blueprint('nfts_blueprint', url_prefix='/')

logger = get_logger('NFTS Blueprint')


@bp.get('/nft')
@openapi.tag("NFT")
@openapi.summary("Get NFT info of DEX V3")
@openapi.parameter(name="token_ids", description="DEX NFT IDS", location="query")
@validate(query=NFTQuery)
async def get_nft_info(request: Request, query: NFTQuery):
    db: NFTMongoDB = request.app.ctx.db
    token_id = query.token_id
    cursor = db.get_nfts_by_keys(keys=token_id)
    tokens = []
    for doc in cursor:
        tokens.append(doc)
    return json({'nfts': tokens})


@bp.get('/top-nfts')
@openapi.tag("NFT")
@openapi.summary("Get TOP NFT info of DEX V3")
# @openapi.parameter(name="token_ids", description="DEX NFT IDS", location="query")
async def get_top_nft(request: Request):
    db: NFTMongoDB = request.app.ctx.db
    cursor = db.get_top_nfts(_limit=20)
    tokens = []
    for doc in cursor:
        tokens.append({
            'tokenId': doc['tokensId'],
            'apr': doc.get('apr'),
            'currentAssetsInUSD': doc.get("assetsInUSD"),
            'investedAssetInUSD': doc.get("investedAssetInUSD"),
            'pnl': doc.get("PnL"),
            'wallet': doc.get("wallet")

        })
    return json({"nfts": tokens})

