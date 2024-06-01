from app.constants.network_constants import Chain


class dYdXTokens:
    mapping = {
        Chain.ETH: "0x92d6c1e31e14520e676a687f0a93788b716beff5",
    }


class HolderTags:
    mapping = {
        "dydx": ["dydx_holders"],
        "aave": ["aave_holders", "stkaave_holders", "gho_holders"],
        "trava": ["trava_holders", "rtrava_holders"],
        "lido": ['ldo_holders']
    }


class RewardTokens:
    mapping = {
        "0x1": {
            "aave": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
            "dydx": "0x92d6c1e31e14520e676a687f0a93788b716beff5",
            "trava": "0x477a9d5df9beda06f6b021136a2efe7be242fcc9"
        },
        "0x38": {
            "trava": "0x0391be54e72f7e001f6bbc331777710b4f2999ef"
        },
        "0xfa": {
            "trava": "0x186d0ba3dfc3386c464eecd96a61fbb1e2da00bf"
        }

    }
