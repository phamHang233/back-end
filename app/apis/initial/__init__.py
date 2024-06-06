from sanic import Blueprint

from app.apis.initial.create_position import bp as position_bp

initital_api = Blueprint.group(
    position_bp,
    url_prefix='/create-position'
)
