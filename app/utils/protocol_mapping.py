from app.utils.logger_utils import get_logger

logger = get_logger('Protocol mapping')


def get_protocol_address_mapping(protocols: dict):
    """
    Return mapping:
    - Aave V2, V3 Fork: lending_pool_address -> protocol_id
    - Compound Fork: c_token_address -> protocol_id
    - Compound V3 Fork: comet_address -> protocol_id
    - Morpho Fork: comptroller_address -> protocol_id
    - Silo Fork: pool_address -> protocol_id
    """
    mapping = {}
    for protocol_id, info in protocols.items():
        if info['type'] == 'lending':
            mapping.update(get_protocol_mapping(pool_info=info, value=protocol_id))
        elif info['type'] == 'vault':
            mapping.update(get_vault_mapping(protocol_id=protocol_id, vault_info=info, value=protocol_id))
        elif info['type'] == 'dex':
            mapping.update(get_dex_mapping(dex_info=info, value=protocol_id))

    return mapping


def get_protocol_mapping(pool_info, value, chain_id=None):
    mapping = {}

    forked_from = pool_info.get('forked')
    if not forked_from:
        return {}

    elif forked_from == 'compound':
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['cToken'].lower()
            mapping[address] = value

    elif forked_from.startswith('aave'):
        address = pool_info['address'].lower()
        mapping[address] = value

        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            t_address = market_info['tToken'].lower()
            mapping[t_address] = value
            d_address = market_info['dToken'].lower()
            mapping[d_address] = value

    elif forked_from == 'compound-v3':
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['comet'].lower()
            mapping[address] = value

    elif forked_from.startswith('morpho'):
        address = pool_info['comptrollerAddress'].lower()
        mapping[address] = value

    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['pool'].lower()
            mapping[address] = value

    elif pool_info.get('address'):
        logger.warning(f'Missing handle fork {forked_from}')
        address = pool_info['address'].lower()
        mapping[address] = value

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def get_vault_mapping(protocol_id, vault_info, value, chain_id=None):
    mapping = {}

    if protocol_id == 'trava-vault':
        reserves_list = vault_info.get('reservesList', {})
        for vault_address, market_info in reserves_list.items():
            mapping[vault_address] = value

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def get_dex_mapping(dex_info, value, chain_id=None):
    mapping = {}

    reserves_list = dex_info.get('reservesList', {})
    for pair_address, pair_info in reserves_list.items():
        mapping[pair_address] = value

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def get_markets_mapping(project_id, pool_info, chain_id=None):
    forked_from = pool_info.get('forked')

    mapping = {}
    if not forked_from:
        return {}

    elif forked_from == 'compound':
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['cToken'].lower()
            mapping[address] = {
                'market_key': project_id,
                'market_type': 'project'
            }

    elif forked_from.startswith('aave'):
        address = pool_info['address'].lower()
        mapping[address] = {
            'market_key': project_id,
            'market_type': 'project'
        }

    elif forked_from == 'compound-v3':
        reserves_list = pool_info.get('reservesList', {})
        for token_address, market_info in reserves_list.items():
            address = market_info['comet'].lower()
            mapping[address] = {
                'market_key': token_address,
                'market_type': 'token',
            }

    elif forked_from.startswith('morpho'):
        address = pool_info['comptrollerAddress'].lower()
        mapping[address] = {
            'market_key': project_id,
            'market_type': 'project'
        }

    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for token_address, market_info in reserves_list.items():
            address = market_info['pool'].lower()
            mapping[address] = {
                'market_key': token_address,
                'market_type': 'token',
            }

    elif pool_info.get('address'):
        logger.warning(f'Missing handle fork {forked_from}')
        address = pool_info['address'].lower()
        mapping[address] = {
            'market_key': project_id,
            'market_type': 'project'
        }

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def convert_reserves_list_to_markets(pool_info):
    forked_from = pool_info.get('forked')
    if not forked_from:
        return {}

    reserves_list = pool_info.get('reservesList', {})

    markets = {}
    if forked_from == 'compound':
        for asset_address, market_info in reserves_list.items():
            address = market_info['cToken'].lower()
            markets[address] = {asset_address: market_info}

    elif forked_from.startswith('aave'):
        address = pool_info['address'].lower()
        markets[address] = reserves_list

    elif forked_from == 'compound-v3':
        for asset_address, market_info in reserves_list.items():
            address = market_info['comet'].lower()
            assets = {asset_address: {}}
            assets.update(market_info.get('assets', {}))
            markets[address] = assets

    elif forked_from.startswith('morpho'):
        address = pool_info['comptrollerAddress'].lower()
        markets[address] = reserves_list

    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['pool'].lower()
            markets[address] = {asset_address: {} for asset_address in market_info.get('assets', [])}

    elif pool_info.get('address'):
        logger.warning(f'Missing handle fork {forked_from}')
        address = pool_info['address'].lower()
        markets[address] = reserves_list

    return markets


def mapping_asset_by_lending_token(lending_token, pool_info):
    mapping = {}

    forked_from = pool_info.get('forked')
    if not forked_from:
        return {}

    elif forked_from == 'compound' or forked_from.startswith('morpho-compound'):
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['cToken'].lower()
            mapping[address] = asset_address

    elif forked_from.startswith('aave') or forked_from.startswith('morpho-aave'):
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            t_address = market_info['tToken'].lower()
            mapping[t_address] = asset_address
            d_address = market_info['dToken'].lower()
            mapping[d_address] = asset_address

    elif forked_from == 'compound-v3':
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['comet'].lower()
            mapping[address] = asset_address

    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['pool'].lower()
            mapping[address] = asset_address

    elif pool_info.get('address'):
        logger.warning(f'Missing handle fork {forked_from}')

    return mapping.get(lending_token)
