from sanic import Blueprint, Request, json

from app.databases.mongodb.mongodb_nft import NFTMongoDB
from app.models.project import NFTQuery, WalletQuery
from app.utils.logger_utils import get_logger
from sanic_ext import openapi, validate


bp = Blueprint('wallets_blueprint', url_prefix='/')

logger = get_logger('Wallets Blueprint')


@bp.get('/wallet')
@openapi.tag("WALLET")
@openapi.summary("Get wallet info of DEX V3")
@openapi.parameter(name="address", description="Wallet Address", location="query")
@validate(query=WalletQuery)
async def get_nft_info(request: Request, query: WalletQuery):
    db: NFTMongoDB = request.app.ctx.db
    address = query.address
    cursor = db.get_wallet_by_addresses(addresses=[address])
    wallets = []
    for doc in cursor:
        wallets.append(doc)
    return json({'wallets': wallets})


@bp.get('/top-wallet')
@openapi.tag("WALLET")
@openapi.summary("Get wallet info of DEX V3")
@openapi.parameter(name="address", description="Wallet Address", location="query")
@validate(query=WalletQuery)
async def get_top_wallets(request: Request):
    db: NFTMongoDB = request.app.ctx.db
    cursor = db.get_top_wallets()
    wallets = []
    for doc in cursor:
        wallets.append({
            'address': doc['address'],
            'PnL': doc.get("PnL"),
            'apr': doc.get('apr')

        })
    return json({"wallets": wallets})

