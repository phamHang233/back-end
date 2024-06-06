from sanic import Blueprint

data_api  = Blueprint.group(
    # data_bp,
    url_prefix='/data'
)