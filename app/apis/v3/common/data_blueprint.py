from typing import Union

from sanic import Blueprint, Request
from sanic import json
from sanic_ext import openapi, validate

from app.constants.network_constants import Chain, COMMON_NOT_SUPPORTED, NativeTokens
from app.utils.logger_utils import get_logger

bp = Blueprint('data_blueprint', url_prefix='/')

logger = get_logger('Common Blueprint')


@bp.get('/chains')
@openapi.tag("Common")
@openapi.secured('x-apikey')
@openapi.summary("Get all chain")
async def get_all_chains(request: Request):
    return json({
        'chains': {
            Chain.ETH: {
                'id': Chain.ETH,
                'name': 'Ethereum',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FETH.png?alt=media&token=55db834b-029b-4237-9b30-f5fd28d7b2f4'
            },
            Chain.BSC: {
                'id': Chain.BSC,
                'name': 'BNB Chain',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FBNB.png?alt=media&token=b0a77aea-6f98-4916-9dbf-ffdc9b44c2c3'
            },
            Chain.POLYGON: {
                'id': Chain.POLYGON,
                'name': 'Polygon',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FMATIC.png?alt=media&token=f3dd80ba-b045-40ba-9c8c-ee0d9617d798'
            },
            Chain.FTM: {
                'id': Chain.FTM,
                'name': 'Fantom',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FFTM.png?alt=media&token=0fc3758c-9aa3-491b-904b-46fabb097447'
            },
            Chain.ARBITRUM: {
                'id': Chain.ARBITRUM,
                'name': 'Arbitrum',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Farbitrum.jpeg?alt=media&token=cd5a7393-1488-4d3a-8eeb-f9b7d65d952b'
            },
            Chain.OPTIMISM: {
                'id': Chain.OPTIMISM,
                'name': 'Optimism',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Foptimism.png?alt=media&token=5bdb5bd7-6aa7-4c31-bc49-121e869f6b49'
            },
            Chain.AVALANCHE: {
                'id': Chain.AVALANCHE,
                'name': 'Avalanche C-Chain',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX.png?alt=media&token=1e01b02f-0fb2-4887-b84d-837a4e2880dd'
            },
            Chain.AVALANCHE_X: {
                'id': Chain.AVALANCHE_X,
                'name': 'Avalanche X-Chain',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX-X.png?alt=media&token=ebaa0fc0-3420-42aa-ad0b-39bdeea3c074'
            },
            Chain.AVALANCHE_P: {
                'id': Chain.AVALANCHE_P,
                'name': 'Avalanche P-Chain',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX-P.png?alt=media&token=3d8caa29-28e8-46a0-8248-393637b8589d'
            },
            Chain.TRON: {
                'id': Chain.TRON,
                'name': 'Tron',
                'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FTRX.png?alt=media&token=85e1e5a3-26bc-433b-81dd-f2733c7ffe80'
            },
            Chain.CRONOS: {
                'id': Chain.CRONOS,
                'name': 'Cronos',
                'imgUrl': NativeTokens.cronos['imgUrl']
            },
            Chain.SOLANA: {
                'id': Chain.SOLANA,
                'name': 'Solana',
                'imgUrl': NativeTokens.solana['imgUrl']
            },
            Chain.POLKADOT: {
                'id': Chain.POLKADOT,
                'name': 'Polkadot',
                'imgUrl': NativeTokens.polkadot['imgUrl']
            }
        }
    })
