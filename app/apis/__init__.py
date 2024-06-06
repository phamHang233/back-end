from sanic import Blueprint

from app.apis.initial import initital_api
from app.apis.nfts import nft_api
from app.apis.overviews import overview_api
from app.apis.v3.common import common_api
from app.apis.v3.data import data_api
from app.apis.wallets import wallet_api

api_v3 = Blueprint.group(
    initital_api,
    nft_api,
    overview_api,
    wallet_api

)
