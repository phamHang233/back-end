from sanic import Blueprint
from app.apis.overviews.overview_blueprint import bp as overview_bp
overview_api = Blueprint.group(
    overview_bp,
    url_prefix='/overview'
)