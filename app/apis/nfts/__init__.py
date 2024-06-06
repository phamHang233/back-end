from sanic import Blueprint

from app.apis.nfts.nft_blueprint import bp as nft_bp

nft_api = Blueprint.group(
    nft_bp,
    url_prefix='/nft'
)
