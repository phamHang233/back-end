from app.constants.network_constants import Chain


class RelationTypes:
    deployed_by = 'Deployed by'
    deploy_contract = 'Deploy contract'
    same_protocol_with = 'In the same protocol with'
    is_deposit_wallet_of_user = 'Is deposit wallet of user'
    has_deposit_wallet = 'Has deposit wallet'


class Scans:
    mapping = {
        'bscscan': Chain.BSC,
        'etherscan': Chain.ETH,
        'ftmscan': Chain.FTM,
        'polygonscan': Chain.POLYGON,
        'arbiscan': Chain.ARBITRUM,
        'optimistic': Chain.OPTIMISM,
        'snowtrace': Chain.AVALANCHE
    }

    api_keys = {
        Chain.BSC: 'GKRVTEQHSGWX335P21994DKNSTJVDYYXJ1',
        Chain.ETH: 'YQVXQAJAYNS7AC48ZXS1NT45RKYRPY7P7E',
        Chain.FTM: 'Y85XY5DDDN4Z7C7Z4ZX8N62M5WET7D8NYC',
        Chain.POLYGON: 'EYADI5Q4VK4VF99KFEJURMVE9CNE54YEYW',
        Chain.ARBITRUM: 'BU5VN8872FE119RDX898MS5EZHE11FPZSC',
        Chain.OPTIMISM: '1JW9PDV2HZZI3WRFQRVGWUDD2T1RV8GWX3',
        Chain.AVALANCHE: 'JWNM6Y5UA1499PWVA2T55MEH3TZDGAKJWJ'
    }

    api_bases = {
        Chain.BSC: 'https://api.bscscan.com/api',
        Chain.ETH: 'https://api.etherscan.io/api',
        Chain.FTM: 'https://api.ftmscan.com/api',
        Chain.POLYGON: 'https://api.polygonscan.com/api',
        Chain.ARBITRUM: 'https://api.arbiscan.io/api',
        Chain.OPTIMISM: 'https://api-optimistic.etherscan.io/api',
        Chain.AVALANCHE: 'https://api.snowtrace.io/api'
    }

    scan_base_urls = {
        Chain.ETH: 'https://etherscan.io/',
        Chain.BSC: 'https://bscscan.com/',
        Chain.FTM: 'https://ftmscan.com/',
        Chain.POLYGON: 'https://polygonscan.com/',
        Chain.ARBITRUM: 'https://arbiscan.io/',
        Chain.OPTIMISM: 'https://optimistic.etherscan.io/',
        Chain.AVALANCHE: 'https://snowtrace.io/'
    }

    all_base_urls = {
        Chain.BSC: [
            'https://bscscan.com/token',
            'https://bscscan-com.translate.goog/token'
        ],
        Chain.POLYGON: [
            'https://polygonscan.com/token',
            'https://polygonscan-com.translate.goog/token'
        ],
        Chain.ETH: [
            'https://etherscan-io.translate.goog/token',
        ],
        Chain.FTM: [
            'https://ftmscan.com/token',
            'https://ftmscan-com.translate.goog/token'
        ],
        Chain.ARBITRUM: [
            'https://arbiscan-io.translate.goog/token'
        ],
        Chain.OPTIMISM: [
            'https://optimistic-etherscan-io.translate.goog/token'
        ],
        Chain.AVALANCHE: [
            'https://snowtrace.io/token',
            'https://snowtrace-io.translate.goog/token'
        ]
    }

    gg_translate_suffix = '?_x_tr_sl=vi&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=wapp'
