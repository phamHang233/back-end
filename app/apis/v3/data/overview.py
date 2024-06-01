from sanic import Blueprint, Request
from sanic import json
from sanic_ext import openapi

from app.utils.logger_utils import get_logger

bp = Blueprint('data_overview_blueprint', url_prefix='/')

logger = get_logger('Data Overview Blueprint')


@bp.get('/overview')
@openapi.exclude()
@openapi.tag("Data")
@openapi.summary("Get data statistics information")
@openapi.secured('x-apikey')
async def get_data_overview(request: Request):
    return json({
        "dailyDataProcessed": 317042933760,
        "dailyDataCreated": 10568097792,
        "analyzedTransactions": 12585669000,
        "analyzedTokens": 17032,
        "analyzedAddresses": 365625742,
        "labeledAddresses": 100403032,
        "labeledProjects": 32158
    })
