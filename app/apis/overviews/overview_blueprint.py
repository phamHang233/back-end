from sanic import Blueprint, Request


from app.utils.logger_utils import get_logger
from sanic_ext import openapi

bp = Blueprint('overview_blueprint', url_prefix='/')

logger = get_logger('Common Blueprint')


@bp.get('/chains')
@openapi.tag("Overview")
@openapi.summary("Get all chain")
async def overview(request: Request):
    ...