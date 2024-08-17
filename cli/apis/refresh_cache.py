# import time
#
# import click
# import redis
# from cli_scheduler.scheduler_job import SchedulerJob, scheduler_format
#
# from app.constants.network_constants import Chain
# from app.constants.time_constants import TimeConstants
# from app.databases.mongodb.mongodb_contract_label import MongoDBContractLabel
# from app.databases.mongodb.mongodb_dex import MongoDBDex
# from app.databases.mongodb.mongodb_klg import MongoDB
# from app.databases.mongodb.mongodb_nft import NFTMongoDB
# from app.services.cached.cache_dapps import CacheDApps
# from app.utils.file_utils import write_last_time_running_logs
# from app.utils.logger_utils import get_logger
# from config import RedisConfig
#
# logger = get_logger('Refresh Cache')
#
#
# @click.command(context_settings=dict(help_option_names=['-h', '--help']))
# @click.option('--tag', default=None, show_default=True, type=str, help='Tag: staging or latest')
# @click.option('--monitor', default=False, show_default=True, type=bool, help='Monitor or not')
# @click.option('--scheduler', default='^true@hourly/10', show_default=True, type=str, help=f'Scheduler with format "{scheduler_format}"')
# def refresh_cache(tag, monitor, scheduler):
#     """Refresh cache for API."""
#
#     job = RefreshCacheJob(
#         monitor=monitor,
#         tag=tag,
#         scheduler=scheduler
#     )
#     job.run()
#
#
# class RefreshCacheJob(SchedulerJob):
#     def __init__(self, monitor, tag, scheduler):
#         self.monitor = monitor
#         self.tag = tag
#
#         super().__init__(scheduler=scheduler)
#
#     def _pre_start(self):
#         self._db = MongoDB()
#         self.dex_nft_db = NFTMongoDB()
#         self._dex_db = MongoDBDex()
#         self._sc_label_db = MongoDBContractLabel()
#         logger.info(f'Connect to entity data: {self._db.connection_url}')
#
#     def _execute(self, *args, **kwargs):
#         r = redis.from_url(RedisConfig.CONNECTION_URL)
#         self.cache_dapps(r)
