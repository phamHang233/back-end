import os
from textwrap import dedent

from dotenv import load_dotenv

from app.constants.network_constants import Chain

load_dotenv()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class Config:
    RUN_SETTING = {
        'host': os.environ.get('SERVER_HOST', 'localhost'),
        'port': int(os.environ.get('SERVER_PORT', 8080)),
        'debug': os.getenv('DEBUG', False),
        "access_log": False,
        "auto_reload": True,
        'workers': int(os.getenv('SERVER_WORKERS', 4))
    }
    # uWSGI를 통해 배포되어야 하므로, production level에선 run setting을 건드리지 않음

    SECRET = os.environ.get('SECRET_KEY', 'example project')
    JWT_PASSWORD = os.getenv('JWT_PASSWORD', 'dev123')
    EXPIRATION_JWT = 2592000  # 1 month
    RESPONSE_TIMEOUT = 900  # seconds

    SERVER_NAME = os.getenv('SERVER_NAME')

    # To reorder swagger tags
    raw = {
        'tags': [

            {'name': 'Search'}, {'name': 'Wallet'},  {'name': 'NFT'},
            {'name': 'Explore Users'}, {'name': "Position"}
        ]
    }
    if SERVER_NAME:
        raw['servers'] = [{'url': SERVER_NAME}]

    FALLBACK_ERROR_FORMAT = 'json'

    OAS_UI_DEFAULT = 'swagger'
    SWAGGER_UI_CONFIGURATION = {
        'apisSorter': "alpha",
        'docExpansion': "list",
        'operationsSorter': "alpha"
    }

    API_HOST = os.getenv('API_HOST', '0.0.0.0:8096')
    API_BASEPATH = os.getenv('API_BASEPATH', '')
    API_SCHEMES = os.getenv('API_SCHEMES', 'http')
    API_VERSION = os.getenv('API_VERSION', '0.1.0')
    API_TITLE = os.getenv('API_TITLE', 'Centic Data')
    API_CONTACT_EMAIL = os.getenv('API_CONTACT_EMAIL', 'example@gmail.com')

    API_DESCRIPTION = os.getenv('API_DESCRIPTION', dedent(
        """
        ## Explore the Entity-Oriented API
        
        Power your AI application with Centic Data API focus on entity: Wallet, digital assets, DApps, and Foundation.
        
        Come with us to accelerate your Growth.
        - Our Centic API has a rate limit of 200 calls/5 minutes.
        - Need something more powerful? [Contact us](https://centic.io).
        """
    ))


class LocalDBConfig:
    pass


class RemoteDBConfig:
    pass


class ArangoDBGraphConfig:
    USERNAME = os.environ.get("ARANGO_GRAPH_USERNAME") or "just_for_dev"
    PASSWORD = os.environ.get("ARANGO_GRAPH_PASSWORD") or "password_for_dev"
    HOST = os.environ.get("ARANGO_GRAPH_HOST") or "localhost"
    PORT = os.environ.get("ARANGO_GRAPH_PORT") or "8529"

    CONNECTION_URL = os.getenv("ARANGO_GRAPH_URL") or f"arangodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"

    DATABASE = os.getenv('ARANGO_GRAPH_DB', 'klg_database')


class MongoDBConfig:
    CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DATABASE', 'knowledge_graph')

class NFTMongoDBConfig:
    CONNECTION_URL = os.getenv("NFT_MONGODB_CONNECTION_URL")
    DATABASE = os.getenv('NFT_MONGODB_DATABASE', 'dex_nft_manager')


class ScoreArangoConfig:
    CONNECTION_URL = os.getenv("SCORE_ARANGO_URL")
    DATABASE = os.getenv('SCORE_ARANGO_DB', 'klg_database')


class RedisConfig:
    CONNECTION_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


class BlockchainETLConfig:
    CONNECTION_URL = os.environ.get('BLOCKCHAIN_ETL_CONNECTION_URL')
    TEST_CONNECTION_URL = os.environ.get('TEST_BLOCKCHAIN_ETL_CONNECTION_URL')

    BNB_DATABASE = os.environ.get("BNB_DATABASE") or "blockchain_etl"
    ETHEREUM_DATABASE = os.environ.get("ETHEREUM_DATABASE") or "ethereum_blockchain_etl"
    FANTOM_DATABASE = os.environ.get("FANTOM_DATABASE") or "ftm_blockchain_etl"
    POLYGON_DATABASE = os.environ.get("POLYGON_DATABASE") or "polygon_blockchain_etl"
    ARBITRUM_DATABASE = os.environ.get("ARBITRUM_DATABASE") or "arbitrum_blockchain_etl"
    OPTIMISM_DATABASE = os.environ.get("OPTIMISM_DATABASE") or "optimism_blockchain_etl"
    AVALANCHE_DATABASE = os.environ.get("AVALANCHE_DATABASE") or "avalanche_blockchain_etl"
    TRON_DATABASE = os.environ.get("TRON_DATABASE") or "tron_blockchain_etl"


class ArangoDBLendingConfig:
    USERNAME = os.environ.get("ARANGO_LENDING_USERNAME") or "just_for_dev"
    PASSWORD = os.environ.get("ARANGO_LENDING_PASSWORD") or "password_for_dev"
    CONNECTION_URL = os.environ.get("ARANGO_LENDING_URL") or None


class TokenMongoDBConfig:
    MONGODB_HOST = os.environ.get("TOKEN_MONGO_HOST", '0.0.0.0')
    MONGODB_PORT = os.environ.get("TOKEN_MONGO_PORT", '27017')
    USERNAME = os.environ.get("TOKEN_MONGO_USERNAME", "admin")
    PASSWORD = os.environ.get("TOKEN_MONGO_PASSWORD", "admin123")
    CONNECTION_URL = os.environ.get(
        'TOKEN_MONGO_CONNECTION_URL') or f'mongodb://{USERNAME}:{PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}'

    TOKEN_DATABASE = 'TokenDatabase'


class PostgresDBConfig:
    TRANSFER_EVENT_TABLE = os.environ.get("POSTGRES_TRANSFER_EVENT_TABLE", "transfer_event")
    CONNECTION_URL_1 = os.environ.get("POSTGRES_CONNECTION_URL_1", "postgresql://user:password@localhost:5432/database")

    CONNECTION_URLS = {
        CONNECTION_URL_1: [
            Chain.BSC, Chain.FTM, Chain.ETH, Chain.POLYGON,
            Chain.ARBITRUM, Chain.OPTIMISM, Chain.AVALANCHE, Chain.TRON
        ]
    }


class CreditScoreAPIConfig:
    URL = os.getenv('CREDIT_SCORE_API')


class MongoDBDexConfig:
    CONNECTION_URL = os.getenv("MONGODB_DEX_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DEX_DATABASE', 'dex')


class MongoDBContractLabelConfig:
    CONNECTION_URL = os.getenv("MONGODB_CONTRACT_LABEL_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_CONTRACT_LABEL_DATABASE', 'SmartContractLabel')


class MongoDBLabelConfig:
    CONNECTION_URL = os.getenv("MONGODB_LABEL_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_LABEL_DATABASE', 'LabelDatabase')


class MongoDBCommunityConfig:
    CONNECTION_URL = os.getenv("MONGODB_COMMUNITY_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_COMMUNITY_DATABASE', 'CommunityDatabase')


class MongoDBdYdXConfig:
    CONNECTION_URL = os.getenv("MONGODB_DYDX_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DYDX_DATABASE', 'dYdXDatabase')


class MongoDBAuthConfig:
    CONNECTION_URL = os.getenv("MONGODB_AUTH_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_AUTH_DATABASE', 'AuthDatabase')


class MongoDBPageRankConfig:
    CONNECTION_URL = os.getenv("MONGODB_PAGE_RANK_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_PAGE_RANK_DATABASE', 'TravaPageRank')


class MongoDBCDPConfig:
    CONNECTION_URL = os.getenv("MONGODB_CDP_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_CDP_DATABASE', 'cdp_database')


class CenticDBConfig:
    CONNECTION_URL = os.getenv("CENTIC_DB_CONNECTION_URL")
    DATABASE = os.getenv('CENTIC_DB_DATABASE', 'centic')


class ElasticsearchConfig:
    ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_CONNECTION_URL")
    ELASTICSEARCH_PASS = os.getenv("ELASTICSEARCH_PASSWORD")


class MonitoringConfig:
    MONITOR_ROOT_PATH = os.getenv("MONITOR_ROOT_PATH", "/home/monitor/.log/")


class GCloudConfig:
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    ENVIRONMENT = os.getenv("GOOGLE_ENVIRONMENT")
    CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    DEFAULT_EMAIL = os.getenv("GOOGLE_DEFAULT_EMAIL")
    DEFAULT_APP = os.getenv("GOOGLE_DEFAULT_APP")
    PROXY_API_SECRET = os.getenv("GOOGLE_PROXY_API_SECRET")
