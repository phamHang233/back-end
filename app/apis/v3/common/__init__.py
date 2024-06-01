from sanic import Blueprint

from app.apis.v3.common.data_blueprint import bp as data_bp

common_api = Blueprint.group(
    data_bp,
    url_prefix='/common'
)
