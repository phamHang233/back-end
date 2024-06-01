from sanic import Blueprint

from app.apis.v3.common import common_api
from app.apis.v3.data import data_api

api_v3 = Blueprint.group(
    # portfolio_api,
    # score_api,
    common_api,
    # ranking_api,
    # entities_api,
    # search_api,
    # recommend_api,
    data_api,
    # auth_bp,
    version='3'
)
