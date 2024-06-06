from sanic import Blueprint

from app.apis.wallets.wallet_blueprint import bp as wallet_bp

wallet_api = Blueprint.group(
    wallet_bp,
    url_prefix='/wallet'
)
