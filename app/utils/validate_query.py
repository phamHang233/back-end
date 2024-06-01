from sanic.exceptions import BadRequest

from app.constants.search_constants import ProjectTypes
from app.services.artifacts.protocols import ProjectCollectorTypes


def wallet_transactions_with_pagination(query):
    if (not query.chain) and (query.page > 1):
        raise BadRequest('Chain ID must be set when page index is not the first.')


def get_project_type(sources):
    mapping = {
        ProjectCollectorTypes.defi: ProjectTypes.defi,
        ProjectCollectorTypes.nft: ProjectTypes.nft,
        ProjectCollectorTypes.exchange: ProjectTypes.exchange
    }
    if not sources:
        return ProjectTypes.defi

    for type_, project_type in mapping.items():
        if type_ in sources:
            return project_type

    return ProjectTypes.defi
