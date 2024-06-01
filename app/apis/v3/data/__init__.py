from sanic import Blueprint

from app.apis.v3.data.overview import bp as data_bp

data_api = Blueprint.group(
    data_bp,
    url_prefix='/data'
)
